package mailserver

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// failingUpsertMailboxIndex is an in-memory MailboxIndex whose Upsert can be
// forced to fail, to prove publish rejection instead of partial visibility.
type failingUpsertMailboxIndex struct {
	mu                      sync.Mutex
	records                 map[string]IndexedEmail
	upsertErr               error
	upsertFailuresRemaining int
}

func newFailingUpsertMailboxIndex() *failingUpsertMailboxIndex {
	return &failingUpsertMailboxIndex{records: make(map[string]IndexedEmail)}
}

func (index *failingUpsertMailboxIndex) failNextUpsert(err error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.upsertErr = err
	index.upsertFailuresRemaining = 1
}

func (index *failingUpsertMailboxIndex) Rebuild(records []IndexedEmail) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.records = make(map[string]IndexedEmail, len(records))
	for _, record := range records {
		index.records[record.ID] = record
	}
	return nil
}

func (index *failingUpsertMailboxIndex) Upsert(record IndexedEmail) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.upsertFailuresRemaining > 0 {
		index.upsertFailuresRemaining--
		return index.upsertErr
	}
	index.records[record.ID] = record
	return nil
}

func (index *failingUpsertMailboxIndex) Delete(id string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	delete(index.records, id)
	return nil
}

func (index *failingUpsertMailboxIndex) Clear() error {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.records = make(map[string]IndexedEmail)
	return nil
}

func (index *failingUpsertMailboxIndex) Query(query EmailQuery) ([]IndexedEmailResult, int, error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	results := make([]IndexedEmailResult, 0, len(index.records))
	for _, record := range index.records {
		results = append(results, IndexedEmailResult{ID: record.ID, Read: record.Read})
	}
	return results, len(results), nil
}

func (index *failingUpsertMailboxIndex) OwnsPath(path string) bool { return false }
func (index *failingUpsertMailboxIndex) Backend() string           { return "failing-memory" }
func (index *failingUpsertMailboxIndex) Close() error              { return nil }

func (index *failingUpsertMailboxIndex) has(id string) bool {
	index.mu.Lock()
	defer index.mu.Unlock()
	_, ok := index.records[id]
	return ok
}

// TestIndexPublishFailureRejectsWholeMessage proves that a mailbox index
// failure during the publish window leaves no API-visible message, no
// metadata, no source EML, and no index row: every storage layer either
// commits together or not at all.
func TestIndexPublishFailureRejectsWholeMessage(t *testing.T) {
	directory := t.TempDir()
	index := newFailingUpsertMailboxIndex()
	server, err := NewMailServerWithOptions(1025, "localhost", directory, ServerOptions{MailboxIndex: index})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	index.failNextUpsert(errors.New("injected index publish failure"))

	id := "index-fail"
	err = server.storeIncomingEmail(id, bytes.NewReader(validMessage("index failure")), nil)
	if err == nil || !strings.Contains(err.Error(), "injected index publish failure") {
		t.Fatalf("expected index publish failure, got %v", err)
	}
	if got := server.GetAllEmail(); len(got) != 0 {
		t.Fatalf("mailbox exposed %d partially published email(s)", len(got))
	}
	if index.has(id) {
		t.Fatal("index retained a row for the rejected message")
	}
	if _, err := os.Stat(filepath.Join(directory, id+".eml")); !os.IsNotExist(err) {
		t.Fatalf("rejected EML survived: %v", err)
	}
	if _, err := os.Stat(server.metadataPath(id)); !os.IsNotExist(err) {
		t.Fatalf("rejected metadata survived: %v", err)
	}
	if state, stateErr := readRollbackFenceState(rollbackFencePath(directory, id)); stateErr != nil || state != rollbackFenceState {
		t.Fatalf("rejected identity fence = %q, %v", state, stateErr)
	}
	assertNoCommittedOrTemporaryArtifacts(t, directory)

	// The rejected identity is retired by its durable fence (an SMTP retry
	// receives a fresh receiving identity), while a subsequent transaction
	// commits cleanly exactly once.
	retryID := "index-retry"
	if err := server.storeIncomingEmail(retryID, bytes.NewReader(validMessage("index retry")), nil); err != nil {
		t.Fatalf("retry after index failure: %v", err)
	}
	if got := server.GetAllEmail(); len(got) != 1 {
		t.Fatalf("retry published %d emails, want 1", len(got))
	}
	if !index.has(retryID) {
		t.Fatal("committed retry missing its index row")
	}
	if metrics := server.GetMailboxMetrics(); metrics.ReceivedMessages != 1 {
		t.Fatalf("received counter after retry = %d, want 1", metrics.ReceivedMessages)
	}
}

// TestRecoveryConfirmsIdentityWithoutRenotifying proves that restart
// recovery resumes a committed receive under the same identity without
// emitting new-mail notifications, auto-relay work, or receive counters a
// second time, while still making the message available to delivery waits.
func TestRecoveryConfirmsIdentityWithoutRenotifying(t *testing.T) {
	directory := t.TempDir()
	id := "recover-once"
	if err := os.WriteFile(filepath.Join(directory, id+".eml"), validMessage("recover once"), 0600); err != nil {
		t.Fatal(err)
	}

	server, err := NewMailServer(1025, "localhost", directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	notifications := make(chan struct{}, 8)
	server.On("new", func(*Email) { notifications <- struct{}{} })

	// Runtime reload discovers the committed identity and must confirm it
	// silently instead of replaying the original new-mail event.
	if err := server.LoadMailsFromDirectory(); err != nil {
		t.Fatal(err)
	}
	if _, err := server.GetEmail(id); err != nil {
		t.Fatalf("committed email was not resumed: %v", err)
	}
	select {
	case <-notifications:
		t.Fatal("recovery re-notified a previously committed email")
	case <-time.After(30 * time.Millisecond):
	}
	if metrics := server.GetMailboxMetrics(); metrics.ReceivedMessages != 0 {
		t.Fatalf("recovery counted restored email, received = %d", metrics.ReceivedMessages)
	}

	// A genuinely new receive after recovery notifies and counts exactly once.
	if err := server.storeIncomingEmail("fresh-mail", bytes.NewReader(validMessage("fresh")), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("new receive did not notify after recovery")
	}
	if metrics := server.GetMailboxMetrics(); metrics.ReceivedMessages != 1 {
		t.Fatalf("received counter = %d, want 1", metrics.ReceivedMessages)
	}
}

// TestSaveEmailToStoreIsIdempotentByIdentity proves re-confirming the same
// receiving identity in process does not double count or extend the
// delivery cursor.
func TestSaveEmailToStoreIsIdempotentByIdentity(t *testing.T) {
	server, err := NewMailServer(1025, "localhost", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	envelope := &Envelope{From: "sender@example.test", To: []string{"receiver@example.test"}}
	email := &Email{Subject: "same identity", Text: "body"}

	if err := server.SaveEmailToStore("same-id", false, envelope, email); err != nil {
		t.Fatal(err)
	}
	cursorAfterFirst := server.DeliveryCursor()
	if err := server.SaveEmailToStore("same-id", false, cloneEnvelope(envelope), cloneEmail(email)); err != nil {
		t.Fatal(err)
	}
	if cursorAfterSecond := server.DeliveryCursor(); cursorAfterSecond != cursorAfterFirst {
		t.Fatalf("delivery cursor moved on identity re-confirm: %s -> %s", cursorAfterFirst, cursorAfterSecond)
	}
	if metrics := server.GetMailboxMetrics(); metrics.ReceivedMessages != 1 {
		t.Fatalf("received counter = %d, want 1", metrics.ReceivedMessages)
	}
	if got := server.GetAllEmail(); len(got) != 1 {
		t.Fatalf("mailbox contains %d emails for one identity", len(got))
	}
}

// TestQuarantineExplainsIsolatedArtifacts verifies that every isolated
// artifact carries an operator-readable explanation, and that metadata
// sidecars without a committed identity are isolated instead of lingering.
func TestQuarantineExplainsIsolatedArtifacts(t *testing.T) {
	directory := t.TempDir()

	// Corrupt message: isolated with an explanation.
	if err := os.WriteFile(filepath.Join(directory, "broken.eml"), []byte("not a message"), 0644); err != nil {
		t.Fatal(err)
	}
	// Orphan metadata: no EML, no fence.
	metadataDir := filepath.Join(directory, metadataDirectoryName)
	if err := os.MkdirAll(metadataDir, 0750); err != nil {
		t.Fatal(err)
	}
	orphanMetadata := fmt.Sprintf(`{"version":%d,"id":"orphan-meta","read":false}`, currentMetadataVersion)
	if err := os.WriteFile(filepath.Join(metadataDir, "orphan-meta.json"), []byte(orphanMetadata), 0600); err != nil {
		t.Fatal(err)
	}
	// Interrupted metadata atomic write.
	if err := os.WriteFile(filepath.Join(metadataDir, ".state-abandoned.tmp"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}

	server, err := NewMailServer(1025, "localhost", directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	if _, err := os.Stat(filepath.Join(metadataDir, "orphan-meta.json")); !os.IsNotExist(err) {
		t.Fatalf("orphan metadata was not isolated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(metadataDir, ".state-abandoned.tmp")); !os.IsNotExist(err) {
		t.Fatalf("abandoned metadata temp file was not isolated: %v", err)
	}

	quarantineRoot := filepath.Join(directory, quarantineDirName)
	entries, err := os.ReadDir(quarantineRoot)
	if err != nil {
		t.Fatal(err)
	}
	var explained int
	for _, entry := range entries {
		explanation, err := os.ReadFile(filepath.Join(quarantineRoot, entry.Name(), quarantineExplanationFileName))
		if err != nil {
			t.Fatalf("quarantine entry %s has no explanation: %v", entry.Name(), err)
		}
		text := string(explanation)
		if !strings.Contains(text, "Receive identity:") || !strings.Contains(text, "Reason:") {
			t.Fatalf("explanation for %s is missing identity/reason: %q", entry.Name(), text)
		}
		explained++
	}
	if explained < 3 {
		t.Fatalf("expected explanations for corrupt email, orphan metadata and temp metadata, got %d", explained)
	}
}
