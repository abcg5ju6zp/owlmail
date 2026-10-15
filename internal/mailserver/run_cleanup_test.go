package mailserver

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func cleanupTestServer(t *testing.T) *MailServer {
	t.Helper()
	s, err := NewMailServer(1025, "localhost", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func commitRunEmail(t *testing.T, s *MailServer, id, run string) {
	t.Helper()
	if err := s.SaveEmailToStore(id, false, &Envelope{To: []string{"a@example.test"}}, &Email{
		Subject: "run mail " + id,
		Text:    "body",
		RunID:   run,
	}); err != nil {
		t.Fatalf("SaveEmailToStore(%s): %v", id, err)
	}
}

func runEmailIDs(s *MailServer, run string) []string {
	ids := make([]string, 0)
	for _, email := range s.GetAllEmail() {
		if email.RunID == run {
			ids = append(ids, email.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// TestRunCleanupPinsSetInCriticalSection is TR-3.1: a delivery that commits
// while the pin critical section is open cannot join the pinned set.
func TestRunCleanupPinsSetInCriticalSection(t *testing.T) {
	s := cleanupTestServer(t)
	for _, id := range []string{"a1", "a2", "a3"} {
		commitRunEmail(t, s, id, "run-a")
	}
	commitRunEmail(t, s, "b1", "run-b")
	commitRunEmail(t, s, "b2", "run-b")

	pinned := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan struct{})
	started := make(chan struct{})
	var pinOnce sync.Once
	var startedOnce sync.Once
	var captured []string
	s.duringRunCleanupPin = func(run string, ids []string) {
		if run != "run-a" {
			return
		}
		pinOnce.Do(func() {
			captured = append([]string(nil), ids...)
			close(pinned)
		})
		<-release
	}
	s.beforeStoreCommit = func(email *Email) error {
		if email.ID == "late" {
			startedOnce.Do(func() { close(started) })
		}
		return nil
	}

	type cleanupOutcome struct {
		result RunCleanupResult
		err    error
	}
	done := make(chan cleanupOutcome, 1)
	go func() {
		r, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-a"}, nil)
		done <- cleanupOutcome{r, err}
	}()
	select {
	case <-pinned:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup never reached the pin critical section")
	}

	// The delivery reaches the store commit while the pin still holds the
	// store lock; metadata persistence happens under that lock, so no late
	// artifact or runtime entry can exist yet.
	go func() {
		commitRunEmail(t, s, "late", "run-a")
		close(committed)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("late delivery never reached the blocked store commit")
	}
	if _, err := os.Stat(s.metadataPath("late")); !os.IsNotExist(err) {
		close(release)
		t.Fatalf("late delivery persisted metadata while the pin was open: %v", err)
	}

	close(release)
	outcome := <-done
	if outcome.err != nil || len(outcome.result.Deleted) != 3 {
		t.Fatalf("cleanup result = %+v %v", outcome.result, outcome.err)
	}
	sort.Strings(captured)
	if strings.Join(captured, ",") != "a1,a2,a3" {
		t.Fatalf("pinned set = %v", captured)
	}
	deleted := append([]string(nil), outcome.result.Deleted...)
	sort.Strings(deleted)
	if strings.Join(deleted, ",") != "a1,a2,a3" {
		t.Fatalf("deleted set = %v", deleted)
	}
	select {
	case <-committed:
	case <-time.After(2 * time.Second):
		t.Fatal("late delivery never committed after the pin closed")
	}

	if got := runEmailIDs(s, "run-a"); len(got) != 1 || got[0] != "late" {
		t.Fatalf("run-a survivors = %v, want only late", got)
	}
	if got := runEmailIDs(s, "run-b"); len(got) != 2 {
		t.Fatalf("other run affected: %v", got)
	}
	// The late mail belongs to the next round and is deleted there.
	next, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-a"}, nil)
	if err != nil || len(next.Deleted) != 1 || next.Deleted[0] != "late" {
		t.Fatalf("next round = %+v %v", next, err)
	}
	if len(runEmailIDs(s, "run-a")) != 0 || len(runEmailIDs(s, "run-b")) != 2 {
		t.Fatal("next round or other run invariant broken")
	}
}

// TestRunCleanupPaginationAcrossPages is TR-3.2.
func TestRunCleanupPaginationAcrossPages(t *testing.T) {
	s := cleanupTestServer(t)
	const total = 2500
	for i := 0; i < total; i++ {
		commitRunEmail(t, s, fmt.Sprintf("m%04d", i), "run-page")
	}
	var cursor string
	pages := 0
	deleted := make(map[string]struct{}, total)
	for {
		result, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-page", Cursor: cursor, Limit: 1000}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range result.Deleted {
			if _, dup := deleted[id]; dup {
				t.Fatalf("duplicate deletion across pages: %s", id)
			}
			deleted[id] = struct{}{}
		}
		pages++
		if pages == 2 {
			// Emails arriving between pages stay out of this snapshot.
			for i := 0; i < 5; i++ {
				commitRunEmail(t, s, fmt.Sprintf("late%02d", i), "run-page")
			}
		}
		if !result.MorePossible {
			if result.NextCursor != "" {
				t.Fatal("final page carried a next cursor")
			}
			break
		}
		sid, index, err := decodeRunCleanupCursor(result.NextCursor)
		if err != nil || !validSnapshotID(sid) || index != pages*1000 {
			t.Fatalf("page %d cursor = %q (%d): %v", pages, result.NextCursor, index, err)
		}
		cursor = result.NextCursor
	}
	if pages != 3 || len(deleted) != total {
		t.Fatalf("pages=%d deleted=%d", pages, len(deleted))
	}
	if survivors := runEmailIDs(s, "run-page"); len(survivors) != 5 || !strings.HasPrefix(survivors[0], "late") {
		t.Fatalf("between-page arrivals = %v", survivors)
	}
	next, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-page"}, nil)
	if err != nil || len(next.Deleted) != 5 {
		t.Fatalf("next round = %+v %v", next, err)
	}
}

// TestRunCleanupCursorValidation is TR-3.3.
func TestRunCleanupCursorValidation(t *testing.T) {
	s := cleanupTestServer(t)
	for _, id := range []string{"c1", "c2", "c3"} {
		commitRunEmail(t, s, id, "run-c")
	}
	commitRunEmail(t, s, "d1", "run-d")

	alwaysPending := func(string) error { return ErrRunEmailRelayPending }

	// First page leaves the snapshot unfinished and returns a continuation.
	first, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-c", Limit: 2}, alwaysPending)
	if err != nil || !first.MorePossible || len(first.Failures) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %+v %v", first, err)
	}
	cursor := first.NextCursor

	// Cursor belongs to another run.
	if _, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-d", Cursor: cursor, Limit: 2}, alwaysPending); !errors.Is(err, ErrRunCleanupInvalidCursor) {
		t.Fatalf("other-run cursor: %v", err)
	}
	// Malformed token.
	if _, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-c", Cursor: "garbage", Limit: 2}, alwaysPending); !errors.Is(err, ErrRunCleanupInvalidCursor) {
		t.Fatalf("garbage cursor: %v", err)
	}
	// Stale boundary (replay the same page boundary once the snapshot advances).
	snapshotID, _, err := decodeRunCleanupCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	stale := encodeRunCleanupCursor(snapshotID, 1)
	if _, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-c", Cursor: stale, Limit: 2}, alwaysPending); !errors.Is(err, ErrRunCleanupInvalidCursor) {
		t.Fatalf("stale boundary cursor: %v", err)
	}

	// Finish the snapshot (all failures, but boundary completes).
	second, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-c", Cursor: cursor, Limit: 2}, alwaysPending)
	if err != nil || second.MorePossible || len(second.Failures) != 1 {
		t.Fatalf("second page = %+v %v", second, err)
	}
	if _, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-c", Cursor: cursor, Limit: 2}, alwaysPending); !errors.Is(err, ErrRunCleanupInvalidCursor) {
		t.Fatalf("completed snapshot cursor: %v", err)
	}
	// Zero deletions happened through the whole validation sequence.
	if got := len(runEmailIDs(s, "run-c")); got != 3 {
		t.Fatalf("run-c emails deleted during validation: %d", got)
	}
	if got := len(runEmailIDs(s, "run-d")); got != 1 {
		t.Fatalf("run-d affected: %d", got)
	}
}

// TestRunCleanupSnapshotPersistence is TR-3.4.
func TestRunCleanupSnapshotPersistence(t *testing.T) {
	s := cleanupTestServer(t)

	// No match: nothing is ever written.
	result, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-empty"}, nil)
	if err != nil || len(result.Deleted) != 0 || result.MorePossible {
		t.Fatalf("empty result = %+v %v", result, err)
	}
	if entries, _ := os.ReadDir(s.runCleanupDir()); len(entries) != 0 {
		t.Fatalf("empty pin wrote snapshots: %v", entries)
	}

	for _, id := range []string{"p1", "p2", "p3"} {
		commitRunEmail(t, s, id, "run-p")
	}
	first, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-p", Limit: 2}, func(string) error { return ErrRunEmailRelayPending })
	if err != nil || !first.MorePossible {
		t.Fatalf("first page = %+v %v", first, err)
	}
	statuses := s.RunCleanupSnapshots()
	if len(statuses) != 1 || statuses[0].NextIndex != 2 || statuses[0].Remaining != 1 {
		t.Fatalf("snapshot status = %+v", statuses)
	}
	path := runCleanupFilePath(s.GetMailDir(), statuses[0].SnapshotID)
	file, err := s.loadRunCleanupFile(statuses[0].SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if file.NextIndex != 2 || len(file.IDs) != 3 || file.RunID != "run-p" {
		t.Fatalf("persisted file = %+v", file)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file missing between pages: %v", err)
	}

	second, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-p", Cursor: first.NextCursor, Limit: 2}, nil)
	if err != nil || second.MorePossible || len(second.Deleted) != 1 {
		t.Fatalf("second page = %+v %v", second, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("completed snapshot file retained: %v", err)
	}
	if len(s.RunCleanupSnapshots()) != 0 {
		t.Fatal("completed snapshot stayed registered")
	}
}

// TestRunCleanupFailureClassification is TR-3.5.
func TestRunCleanupFailureClassification(t *testing.T) {
	s := cleanupTestServer(t)
	ids := []string{"ok1", "relay", "source", "storage", "ok2"}
	for _, id := range ids {
		commitRunEmail(t, s, id, "run-f")
	}
	storageErr := errors.New("disk unavailable")
	deleteOne := func(id string) error {
		switch id {
		case "relay":
			return ErrRunEmailRelayPending
		case "source":
			return ErrEmailSourceInUse
		case "storage":
			return storageErr
		default:
			return s.DeleteEmail(id)
		}
	}
	result, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-f"}, deleteOne)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != 2 || result.Failed != 3 || len(result.Failures) != 3 {
		t.Fatalf("result = %+v", result)
	}
	reasons := map[string]string{}
	for _, failure := range result.Failures {
		reasons[failure.ID] = failure.Reason
	}
	if reasons["relay"] != RunCleanupReasonRelayPending || reasons["source"] != RunCleanupReasonSourceInUse || reasons["storage"] != RunCleanupReasonStorageUnavailable {
		t.Fatalf("reasons = %v", reasons)
	}
	// Failed members keep every source artifact and stay visible.
	for _, id := range []string{"relay", "source", "storage"} {
		if _, err := s.GetEmail(id); err != nil {
			t.Fatalf("failed email %s vanished: %v", id, err)
		}
		if _, err := os.Stat(s.metadataPath(id)); err != nil {
			t.Fatalf("failed email %s lost metadata: %v", id, err)
		}
	}
	for _, id := range []string{"ok1", "ok2"} {
		if _, err := s.GetEmail(id); err == nil {
			t.Fatalf("deleted email %s still visible", id)
		}
	}
}

// TestRunCleanupSerializesSameRun is TR-3.6.
func TestRunCleanupSerializesSameRun(t *testing.T) {
	s := cleanupTestServer(t)
	const total = 100
	for i := 0; i < total; i++ {
		commitRunEmail(t, s, fmt.Sprintf("s%03d", i), "run-s")
	}
	var mu sync.Mutex
	calls := make(map[string]int)
	deleteOne := func(id string) error {
		mu.Lock()
		calls[id]++
		mu.Unlock()
		return s.DeleteEmail(id)
	}

	var wg sync.WaitGroup
	var resultMu sync.Mutex
	var deletedCount int
	var firstErr error
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				result, err := s.CleanupTestRun(RunCleanupRequest{RunID: "run-s", Limit: 7}, deleteOne)
				if err != nil {
					resultMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					resultMu.Unlock()
					return
				}
				resultMu.Lock()
				deletedCount += len(result.Deleted)
				resultMu.Unlock()
				if !result.MorePossible {
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if deletedCount != total {
		t.Fatalf("deleted across workers = %d, want %d", deletedCount, total)
	}
	if len(runEmailIDs(s, "run-s")) != 0 {
		t.Fatal("emails survived serialized cleanup")
	}
	for id, count := range calls {
		if count != 1 {
			t.Fatalf("email %s deleted %d times", id, count)
		}
	}
}

// TestRunCleanupParkedSnapshots is TR-3.7.
func TestRunCleanupParkedSnapshots(t *testing.T) {
	dir := t.TempDir()
	first, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x1", "x2"} {
		if err := first.storeIncomingEmail(id, bytes.NewReader([]byte("From: a@example.test\r\nTo: a@example.test\r\nX-OwlMail-Run-ID: run-x\r\nSubject: "+id+"\r\n\r\nbody")), nil); err != nil {
			t.Fatal(err)
		}
	}
	page, err := first.CleanupTestRun(RunCleanupRequest{RunID: "run-x", Limit: 1}, func(string) error { return ErrRunEmailRelayPending })
	if err != nil || !page.MorePossible {
		t.Fatalf("page = %+v %v", page, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	statuses := restarted.RunCleanupSnapshots()
	if len(statuses) != 1 || statuses[0].RunID != "run-x" || statuses[0].Remaining != 1 || statuses[0].NextIndex != 1 {
		t.Fatalf("parked statuses = %+v", statuses)
	}
	// Loading is registration only: nothing is executed and sources survive.
	if got := len(runEmailIDs(restarted, "run-x")); got != 2 {
		t.Fatalf("parked cleanup executed deletions: %d remain", got)
	}

	// A corrupt snapshot is reported and skipped without touching mail.
	if err := os.WriteFile(filepath.Join(restarted.runCleanupDir(), "0123456789abcdef0123456789abcdef.json"), []byte("{bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := restarted.LoadMailsFromDirectory(); err == nil {
		t.Fatal("corrupt snapshot did not produce a load error")
	}
	if got := len(runEmailIDs(restarted, "run-x")); got != 2 {
		t.Fatalf("corrupt snapshot deleted mail: %d remain", got)
	}
	if len(restarted.RunCleanupSnapshots()) != 1 {
		t.Fatal("valid parked snapshot lost while loading corrupt one")
	}
}

// writeResumeFixtureSnapshot writes an unfinished snapshot file directly.
func writeResumeFixtureSnapshot(t *testing.T, s *MailServer, run string, ids []string, nextIndex int, deleted []string) string {
	t.Helper()
	snapshotID, err := newSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	snapshot := &runCleanupSnapshot{
		id:   snapshotID,
		run:  run,
		path: runCleanupFilePath(s.GetMailDir(), snapshotID),
		file: runCleanupFile{
			Version:    runCleanupFileVersion,
			SnapshotID: snapshotID,
			RunID:      run,
			CreatedAt:  now,
			UpdatedAt:  now,
			IDs:        ids,
			NextIndex:  nextIndex,
			Deleted:    append([]string{}, deleted...),
			Failures:   []RunCleanupFailure{},
		},
	}
	if err := s.writeRunCleanupFile(snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshotID
}

func rawRunMessage(id string) []byte {
	return []byte(fmt.Sprintf("From: sender@example.test\r\nTo: a@example.test\r\nSubject: %s\r\nX-OwlMail-Run-ID: run-resume\r\n\r\nbody", id))
}

// TestResumeRunCleanupsReconcilesAcrossRestart is TR-4.1.
func TestResumeRunCleanupsReconcilesAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	first, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"r-a", "r-b", "r-c", "r-d", "r-e"}
	for _, id := range ids {
		if err := first.storeIncomingEmail(id, bytes.NewReader(rawRunMessage(id)), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.storeIncomingEmail("r-late", bytes.NewReader(rawRunMessage("r-late")), nil); err != nil {
		t.Fatal(err)
	}
	// Two members were already confirmed deleted before the crash.
	if err := first.DeleteEmail("r-a"); err != nil {
		t.Fatal(err)
	}
	if err := first.DeleteEmail("r-b"); err != nil {
		t.Fatal(err)
	}
	// One member crashed between local-attachments and EML layers.
	if err := first.ensureDeletionFence("r-c"); err != nil {
		t.Fatal(err)
	}
	if err := first.writeDeletionFenceCheckpoint("r-c", []string{deletionLayerRemoteAttachments, deletionLayerLocalAttachments}); err != nil {
		t.Fatal(err)
	}
	writeResumeFixtureSnapshot(t, first, "run-resume", ids, 2, []string{"r-a", "r-b"})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	// Startup recovery converges the in-flight fence via the layered checkpoint
	// before explicit takeover runs, so r-c is already gone silently.
	for _, path := range []string{
		filepath.Join(dir, "r-c.eml"),
		restarted.metadataPath("r-c"),
		deletionFencePath(dir, "r-c"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("startup recovery did not converge r-c, %s remains: %v", path, err)
		}
	}
	if len(restarted.RunCleanupSnapshots()) != 1 {
		t.Fatal("fixture snapshot not registered as parked")
	}
	deleteEvents := 0
	var eventMu sync.Mutex
	if err := restarted.OnWithConcurrency("delete", 1, func(*Email) {
		eventMu.Lock()
		deleteEvents++
		eventMu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	results, err := restarted.ResumeRunCleanups(nil)
	if err != nil {
		t.Fatalf("ResumeRunCleanups: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %+v", results)
	}
	gotDeleted := append([]string(nil), results[0].Deleted...)
	sort.Strings(gotDeleted)
	if strings.Join(gotDeleted, ",") != "r-d,r-e" {
		t.Fatalf("resume deleted = %v", gotDeleted)
	}
	if results[0].Failed != 0 || results[0].MorePossible {
		t.Fatalf("resume result = %+v", results[0])
	}
	for _, id := range ids {
		if _, err := os.Stat(filepath.Join(dir, id+".eml")); !os.IsNotExist(err) {
			t.Fatalf("artifact for %s retained: %v", id, err)
		}
		if _, err := os.Stat(deletionFencePath(dir, id)); !os.IsNotExist(err) {
			t.Fatalf("fence for %s retained: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "r-late.eml")); err != nil {
		t.Fatalf("post-pin mail lost during resume: %v", err)
	}
	if len(restarted.RunCleanupSnapshots()) != 0 {
		t.Fatal("snapshot retained after resume completed")
	}
	if entries, _ := os.ReadDir(restarted.runCleanupDir()); len(entries) != 0 {
		t.Fatalf("snapshot files retained: %v", entries)
	}
	eventMu.Lock()
	defer eventMu.Unlock()
	if deleteEvents != 0 {
		t.Fatalf("resume leaked %d delete events", deleteEvents)
	}
}

// TestResumeRunCleanupsPreservesRelayPending is TR-4.2.
func TestResumeRunCleanupsPreservesRelayPending(t *testing.T) {
	dir := t.TempDir()
	first, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"q1", "q2"} {
		if err := first.storeIncomingEmail(id, bytes.NewReader([]byte("From: a@example.test\r\nTo: a@example.test\r\nX-OwlMail-Run-ID: run-q\r\nSubject: "+id+"\r\n\r\nbody")), nil); err != nil {
			t.Fatal(err)
		}
	}
	writeResumeFixtureSnapshot(t, first, "run-q", []string{"q1", "q2"}, 0, nil)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()

	var mu sync.Mutex
	q1Calls := 0
	pendingDeleter := func(id string) error {
		mu.Lock()
		defer mu.Unlock()
		if id == "q1" {
			q1Calls++
			if q1Calls == 1 {
				return ErrRunEmailRelayPending
			}
		}
		return restarted.DeleteEmailQuiet(id)
	}
	results, err := restarted.ResumeRunCleanups(pendingDeleter)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(results[0].Deleted) != 1 || results[0].Deleted[0] != "q2" || results[0].Failed != 1 {
		t.Fatalf("first resume = %+v", results)
	}
	if results[0].Failures[0].ID != "q1" || results[0].Failures[0].Reason != RunCleanupReasonRelayPending {
		t.Fatalf("failure = %+v", results[0].Failures)
	}
	if _, err := restarted.GetEmail("q1"); err != nil {
		t.Fatal("relay-pending source removed")
	}
	if len(restarted.RunCleanupSnapshots()) != 1 {
		t.Fatal("snapshot should remain after partial resume")
	}

	results, err = restarted.ResumeRunCleanups(pendingDeleter)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(results[0].Deleted) != 1 || results[0].Deleted[0] != "q1" || results[0].Failed != 0 {
		t.Fatalf("second resume = %+v", results)
	}
	if len(restarted.RunCleanupSnapshots()) != 0 {
		t.Fatal("snapshot retained after final resume")
	}
}

// TestResumeRunCleanupsEmitsNoEvents is TR-4.3.
func TestResumeRunCleanupsEmitsNoEvents(t *testing.T) {
	dir := t.TempDir()
	first, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e1", "e2", "e3"} {
		if err := first.storeIncomingEmail(id, bytes.NewReader([]byte("From: a@example.test\r\nTo: a@example.test\r\nX-OwlMail-Run-ID: run-e\r\nSubject: "+id+"\r\n\r\nbody")), nil); err != nil {
			t.Fatal(err)
		}
	}
	writeResumeFixtureSnapshot(t, first, "run-e", []string{"e1", "e2", "e3"}, 0, nil)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	events := make(chan string, 8)
	if err := restarted.OnWithConcurrency("delete", 1, func(email *Email) { events <- email.ID }); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ResumeRunCleanups(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-events:
		t.Fatalf("quiet resume emitted a delete event for %s", id)
	case <-time.After(100 * time.Millisecond):
	}
	if len(runEmailIDs(restarted, "run-e")) != 0 {
		t.Fatal("resume did not delete the snapshot members")
	}
}
