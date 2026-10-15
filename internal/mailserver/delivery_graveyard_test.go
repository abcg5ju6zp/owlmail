package mailserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// waitAdmitted blocks until the given number of delivery waiters are
// registered.
func waitAdmitted(t *testing.T, s *MailServer, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.storeMutex.RLock()
		got := s.deliveryWaiters
		s.storeMutex.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("delivery waiters never reached %d", want)
}

// TestGraveyardWakesBlockedWaiterOnDelete is TR-2.1: a waiter blocked before
// the deleted message's sequence is woken by the single delete and receives
// the complete matching message without reaching its timeout.
func TestGraveyardWakesBlockedWaiterOnDelete(t *testing.T) {
	s := deliveryTestServer(t)
	cursor := s.DeliveryCursor()

	parked := make(chan struct{})
	var parkOnce sync.Once
	s.beforeDeliveryWaitSelect = func() { parkOnce.Do(func() { close(parked) }) }

	type waitOutcome struct {
		result DeliveryResult
		err    error
	}
	done := make(chan waitOutcome, 1)
	go func() {
		r, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{RunID: "run-a"}, 1, 5*time.Second)
		done <- waitOutcome{r, err}
	}()
	waitAdmitted(t, s, 1)
	<-parked

	// The email transitions live -> deleted while the waiter is parked. The
	// graveyard record is what its next merged scan must observe.
	commitTestDelivery(t, s, "wanted", "run-a", "a@example.test")
	if err := s.DeleteEmail("wanted"); err != nil {
		t.Fatal(err)
	}

	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.result.TimedOut {
			t.Fatalf("waiter did not receive the deleted match: %+v %v", outcome.result, outcome.err)
		}
		if len(outcome.result.Emails) != 1 || outcome.result.Emails[0].ID != "wanted" {
			t.Fatalf("waiter emails = %#v", outcome.result.Emails)
		}
		if outcome.result.WaitedMilliseconds > 1000 {
			t.Fatalf("waiter was not woken by the delete, waited %dms", outcome.result.WaitedMilliseconds)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked waiter was never woken by the delete")
	}
}

// TestGraveyardScanMergeOrder directly pins the live+graveyard merge used by
// every wait round, including interleaved sequences and count cut-off.
func TestGraveyardScanMergeOrder(t *testing.T) {
	s := deliveryTestServer(t)
	cursor := s.DeliveryCursor()
	commitTestDelivery(t, s, "live-1", "run-a", "a@example.test") // seq1
	commitTestDelivery(t, s, "dead-2", "run-a", "a@example.test") // seq2
	commitTestDelivery(t, s, "live-3", "run-a", "a@example.test") // seq3
	commitTestDelivery(t, s, "live-4", "run-a", "a@example.test") // seq4
	if err := s.DeleteEmail("dead-2"); err != nil {
		t.Fatal(err)
	}

	after, err := s.parseCursorLockedForTest(cursor)
	if err != nil {
		t.Fatal(err)
	}
	s.storeMutex.RLock()
	matches, scanned, satisfied := s.scanDeliveriesLocked(after, &DeliveryFilter{RunID: "run-a"}, 3)
	s.storeMutex.RUnlock()
	if !satisfied || len(matches) != 3 {
		t.Fatalf("matches = %d satisfied=%v", len(matches), satisfied)
	}
	if got := matches[0].ID + "," + matches[1].ID + "," + matches[2].ID; got != "live-1,dead-2,live-3" {
		t.Fatalf("merge order = %q", got)
	}
	if scanned != 3 {
		t.Fatalf("scanned = %d, want cut-off seq 3", scanned)
	}
}

func (ms *MailServer) parseCursorLockedForTest(cursor string) (uint64, error) {
	ms.storeMutex.Lock()
	defer ms.storeMutex.Unlock()
	return ms.parseCursorLocked(cursor)
}

// TestGraveyardReclaimExpiresOldCursor is TR-2.2: aged entries reclaimed
// without a waiter expire the old cursor; a global clear does the same; fresh
// cursors keep working; malformed cursors stay ErrInvalidCursor.
func TestGraveyardReclaimExpiresOldCursor(t *testing.T) {
	s := deliveryTestServer(t)
	current := time.Now().UTC()
	s.nowFunc = func() time.Time { return current }

	cursor := s.DeliveryCursor() // epoch:0
	commitTestDelivery(t, s, "m1", "run-a", "a@example.test")
	first, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{RunID: "run-a"}, 1, time.Second)
	if err != nil || len(first.Emails) != 1 || first.Emails[0].ID != "m1" {
		t.Fatalf("live match before delete: %+v %v", first, err)
	}
	if err := s.DeleteEmail("m1"); err != nil {
		t.Fatal(err)
	}
	// The deleted match still replays from the graveyard.
	replayed, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{RunID: "run-a"}, 1, time.Second)
	if err != nil || len(replayed.Emails) != 1 || replayed.Emails[0].ID != "m1" {
		t.Fatalf("graveyard replay: %+v %v", replayed, err)
	}

	// Age past the retention window with no waiter registered: the next
	// delivery event reaps the entry and advances the floor.
	current = current.Add(deliveryGraveyardRetention + time.Second)
	commitTestDelivery(t, s, "m2", "run-other", "b@example.test")
	if _, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{RunID: "run-a"}, 1, 50*time.Millisecond); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("old cursor after reclaim: %v", err)
	}
	// A fresh cursor is immediately usable and no longer replays the reaped mail.
	fresh := s.DeliveryCursor()
	after, err := s.WaitDeliveries(context.Background(), fresh, DeliveryFilter{RunID: "run-a"}, 1, 50*time.Millisecond)
	if err != nil || !after.TimedOut || len(after.Emails) != 0 {
		t.Fatalf("fresh cursor after reclaim: %+v %v", after, err)
	}
	// Malformed and cross-process cursors remain hard invalid.
	for _, bad := range []string{"garbage", deliveryTestServer(t).DeliveryCursor()} {
		if _, err := s.WaitDeliveries(context.Background(), bad, DeliveryFilter{}, 1, time.Second); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("cursor %q: %v", bad, err)
		}
	}

	// Global clear resets the graveyard and expires every older cursor.
	cleared := deliveryTestServer(t)
	old := cleared.DeliveryCursor()
	commitTestDelivery(t, cleared, "g1", "run-a", "a@example.test")
	if err := cleared.DeleteAllEmail(); err != nil {
		t.Fatal(err)
	}
	if _, err := cleared.WaitDeliveries(context.Background(), old, DeliveryFilter{}, 1, time.Second); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("old cursor after global clear: %v", err)
	}
	next := cleared.DeliveryCursor()
	commitTestDelivery(t, cleared, "g2", "run-a", "a@example.test")
	r, err := cleared.WaitDeliveries(context.Background(), next, DeliveryFilter{RunID: "run-a"}, 1, time.Second)
	if err != nil || len(r.Emails) != 1 || r.Emails[0].ID != "g2" {
		t.Fatalf("fresh cursor after global clear: %+v %v", r, err)
	}
}

// TestGraveyardProtectsEntriesRegisteredWaitersNeed is TR-2.3.
func TestGraveyardProtectsEntriesRegisteredWaitersNeed(t *testing.T) {
	s := deliveryTestServer(t)
	current := time.Now().UTC()
	s.nowFunc = func() time.Time { return current }

	cursor := s.DeliveryCursor()
	commitTestDelivery(t, s, "keep", "run-keep", "a@example.test")
	if err := s.DeleteEmail("keep"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcome := make(chan DeliveryResult, 1)
	go func() {
		r, _ := s.WaitDeliveries(ctx, cursor, DeliveryFilter{RunID: "run-keep"}, 2, 5*time.Second)
		outcome <- r
	}()
	waitAdmitted(t, s, 1)

	// Long after the retention window, the registered waiter still needs the
	// entry (its start sequence is below it), so it cannot be reclaimed.
	current = current.Add(deliveryGraveyardRetention + time.Minute)
	commitTestDelivery(t, s, "other", "run-other", "b@example.test")
	s.storeMutex.RLock()
	kept := len(s.deliveryGraveyard)
	floor := s.deliveryGraveyardFloor
	s.storeMutex.RUnlock()
	if kept != 1 || floor != 0 {
		t.Fatalf("entry reclaimed while a waiter needed it: kept=%d floor=%d", kept, floor)
	}
	select {
	case r := <-outcome:
		t.Fatalf("blocked waiter returned early: %+v", r)
	default:
	}

	// Once the waiter deregisters, the aged entry is reclaimable and its
	// sequence moves the floor.
	cancel()
	waitAdmitted(t, s, 0)
	s.storeMutex.Lock()
	s.reapDeliveryGraveyardLocked()
	kept = len(s.deliveryGraveyard)
	floor = s.deliveryGraveyardFloor
	s.storeMutex.Unlock()
	if kept != 0 || floor != 1 {
		t.Fatalf("entry retained after waiter left: kept=%d floor=%d", kept, floor)
	}
	if _, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{}, 1, time.Second); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("old cursor after reclaim: %v", err)
	}
}

// TestDeliveryWaitFailsFastOnClose is TR-2.4: blocked waiters fail within a
// bounded time of Close, and new requests fail immediately.
func TestDeliveryWaitFailsFastOnClose(t *testing.T) {
	s := deliveryTestServer(t)
	cursor := s.DeliveryCursor()
	parked := make(chan struct{})
	var parkOnce sync.Once
	s.beforeDeliveryWaitSelect = func() { parkOnce.Do(func() { close(parked) }) }

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{RunID: fmt.Sprintf("run-%d", i)}, 1, 10*time.Second)
			errs <- err
		}()
	}
	waitAdmitted(t, s, 3)
	<-parked
	started := time.Now()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrMailboxClosed) {
			t.Fatalf("blocked waiter error = %v, want ErrMailboxClosed", err)
		}
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked waiters took %s to fail after Close", elapsed)
	}
	if _, err := s.WaitDeliveries(context.Background(), cursor, DeliveryFilter{}, 1, time.Second); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("new waiter after Close: %v", err)
	}
}

// TestReadOnlyRefreshExpiresStaleCursor is TR-2.5.
func TestReadOnlyRefreshExpiresStaleCursor(t *testing.T) {
	directory := t.TempDir()
	raw := "From: sender@example.test\r\nTo: a@example.test\r\nSubject: observed\r\n\r\nbody"
	if err := os.WriteFile(filepath.Join(directory, "observed.eml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	observer, err := NewMailServerWithOptions(1025, "localhost", directory, ServerOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close() }()
	cursor := observer.DeliveryCursor()
	if err := observer.RefreshReadOnlyMailbox(); err != nil {
		t.Fatal(err)
	}
	r, err := observer.WaitDeliveries(context.Background(), cursor, DeliveryFilter{}, 1, time.Second)
	if err != nil || len(r.Emails) != 1 || r.Emails[0].ID != "observed" {
		t.Fatalf("read-only delivery: %+v %v", r, err)
	}

	if err := os.Remove(filepath.Join(directory, "observed.eml")); err != nil {
		t.Fatal(err)
	}
	if err := observer.RefreshReadOnlyMailbox(); err != nil {
		t.Fatal(err)
	}
	if _, err := observer.WaitDeliveries(context.Background(), cursor, DeliveryFilter{}, 1, time.Second); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("cursor after read-only removal: %v", err)
	}
}
