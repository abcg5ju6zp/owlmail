package mailserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlmail/internal/attachmentstore"
)

// countingAttachmentStore wraps memoryAttachmentStore to count per-email
// remote deletion calls and to fail the first attempt on demand.
type countingAttachmentStore struct {
	*memoryAttachmentStore
	mu            sync.Mutex
	deleteCalls   map[string]int
	failOnceForID string
}

func newCountingAttachmentStore() *countingAttachmentStore {
	return &countingAttachmentStore{
		memoryAttachmentStore: newMemoryAttachmentStore(),
		deleteCalls:           make(map[string]int),
	}
}

func (store *countingAttachmentStore) DeleteEmail(ctx context.Context, emailID string) error {
	store.mu.Lock()
	store.deleteCalls[emailID]++
	fail := store.failOnceForID == emailID && store.deleteCalls[emailID] == 1
	store.mu.Unlock()
	if fail {
		return errors.New("injected remote delete failure")
	}
	return store.memoryAttachmentStore.DeleteEmail(ctx, emailID)
}

func (store *countingAttachmentStore) deleteCount(emailID string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.deleteCalls[emailID]
}

var _ attachmentstore.Store = (*countingAttachmentStore)(nil)

func storeDeletionTestMessage(t *testing.T, server *MailServer, id string) {
	t.Helper()
	if err := server.storeIncomingEmail(id, bytes.NewReader(multipartMessage()), nil); err != nil {
		t.Fatalf("storeIncomingEmail(%s): %v", id, err)
	}
}

func readFenceCompleted(t *testing.T, server *MailServer, id string) []string {
	t.Helper()
	checkpoint, err := server.readDeletionFenceCheckpoint(id)
	if err != nil {
		t.Fatalf("readDeletionFenceCheckpoint(%s): %v", id, err)
	}
	return checkpoint.Completed
}

// TestDeletionFenceResumesAtUnfinishedLayer injects a failure at every layer
// in turn and proves retries resume at that layer without replaying completed
// work, while the delete event fires exactly once.
func TestDeletionFenceResumesAtUnfinishedLayer(t *testing.T) {
	for _, failingLayer := range deletionLayers {
		t.Run(failingLayer, func(t *testing.T) {
			dir := t.TempDir()
			remote := newCountingAttachmentStore()
			server, err := NewMailServerWithOptions(1025, "localhost", dir, ServerOptions{AttachmentStore: remote})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = server.Close() }()

			const id = "layered-delete"
			storeDeletionTestMessage(t, server, id)
			emails := server.GetAllEmail()
			if len(emails) != 1 || len(emails[0].Attachments) != 1 {
				t.Fatalf("unexpected mailbox state: %#v", emails)
			}
			filename := emails[0].Attachments[0].GeneratedFileName
			if _, ok := remote.objects[id+"/"+filename]; !ok {
				t.Fatalf("remote attachment missing before delete: %#v", remote.objects)
			}

			deleteEvents := make(chan string, 4)
			if err := server.OnWithConcurrency("delete", 1, func(email *Email) {
				select {
				case deleteEvents <- email.ID:
				default:
				}
			}); err != nil {
				t.Fatal(err)
			}

			// The remote layer is failed through the real store call; the
			// local/eml/metadata layers fail through the pre-layer hook.
			if failingLayer == deletionLayerRemoteAttachments {
				remote.failOnceForID = id
			}
			hookCalls := make(map[string]int)
			var hookMu sync.Mutex
			failed := false
			server.beforeDeletionLayer = func(_ string, layer string) error {
				hookMu.Lock()
				defer hookMu.Unlock()
				hookCalls[layer]++
				if layer == failingLayer && layer != deletionLayerRemoteAttachments && !failed {
					failed = true
					return fmt.Errorf("injected %s failure", layer)
				}
				return nil
			}

			if err := server.DeleteEmail(id); err == nil {
				t.Fatalf("first DeleteEmail(%s) succeeded, want injected failure", failingLayer)
			}

			// The fence survives and records exactly the preceding layers.
			fencePath := deletionFencePath(dir, id)
			if _, err := os.Stat(fencePath); err != nil {
				t.Fatalf("fence missing after %s failure: %v", failingLayer, err)
			}
			wantCompleted := make([]string, 0)
			for _, layer := range deletionLayers {
				if layer == failingLayer {
					break
				}
				wantCompleted = append(wantCompleted, layer)
			}
			if got := readFenceCompleted(t, server, id); strings.Join(got, ",") != strings.Join(wantCompleted, ",") {
				t.Fatalf("completed layers = %v, want %v", got, wantCompleted)
			}
			if _, err := server.GetEmail(id); err != nil {
				t.Fatalf("email vanished from runtime state after failed %s layer", failingLayer)
			}
			switch failingLayer {
			case deletionLayerRemoteAttachments:
				if _, err := os.Stat(filepath.Join(dir, id+".eml")); err != nil {
					t.Fatalf("eml removed before remote layer succeeded: %v", err)
				}
			case deletionLayerEML:
				if _, err := os.Stat(filepath.Join(dir, id+".eml")); err != nil {
					t.Fatalf("eml layer reported failure but file is gone: %v", err)
				}
			case deletionLayerMetadata:
				if _, err := os.Stat(server.metadataPath(id)); err != nil {
					t.Fatalf("metadata layer reported failure but sidecar is gone: %v", err)
				}
			}
			if got := remote.deleteCount(id); got != 1 {
				t.Fatalf("remote delete calls after first attempt = %d, want 1", got)
			}

			// Retry converges without replaying completed layers.
			if err := server.DeleteEmail(id); err != nil {
				t.Fatalf("retry DeleteEmail(%s): %v", failingLayer, err)
			}
			if _, err := server.GetEmail(id); err == nil {
				t.Fatal("email still visible after converged delete")
			}
			if _, err := os.Stat(fencePath); !os.IsNotExist(err) {
				t.Fatalf("fence retained after converged delete: %v", err)
			}
			for _, path := range []string{filepath.Join(dir, id+".eml"), server.metadataPath(id), filepath.Join(dir, id)} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("artifact retained after converged delete: %s", path)
				}
			}
			if len(remote.objects) != 0 {
				t.Fatalf("remote objects survived converged delete: %#v", remote.objects)
			}
			wantRemoteCalls := 1
			if failingLayer == deletionLayerRemoteAttachments {
				// The unfinished remote layer is idempotently replayed once.
				wantRemoteCalls = 2
			}
			if got := remote.deleteCount(id); got != wantRemoteCalls {
				t.Fatalf("remote delete calls after retry = %d, want %d", got, wantRemoteCalls)
			}
			hookMu.Lock()
			for _, layer := range deletionLayers {
				want := 1
				if layer == failingLayer {
					want = 2
				}
				if hookCalls[layer] != want {
					t.Fatalf("hook calls for %s = %d, want %d (map %v)", layer, hookCalls[layer], want, hookCalls)
				}
			}
			hookMu.Unlock()

			events := 0
			deadline := time.After(2 * time.Second)
		wait:
			for {
				select {
				case <-deleteEvents:
					events++
				case <-deadline:
					break wait
				}
				if events == 2 {
					break
				}
			}
			if events != 1 {
				t.Fatalf("delete events = %d, want exactly 1", events)
			}
		})
	}
}

// TestDeletionFenceLegacyPlainTextResumes proves a fence written by an older
// build ("delete\n") is treated as zero completed layers and still converges.
func TestDeletionFenceLegacyPlainTextResumes(t *testing.T) {
	dir := t.TempDir()
	server, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	const id = "legacy-fence"
	storeDeletionTestMessage(t, server, id)
	emlPath := filepath.Join(dir, id+".eml")
	metadataPath := server.metadataPath(id)
	fencePath := deletionFencePath(dir, id)
	if err := os.WriteFile(fencePath, []byte(deletionFenceState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	if got := readFenceCompleted(t, server, id); len(got) != 0 {
		t.Fatalf("legacy fence loaded completed layers %v, want none", got)
	}
	if err := server.cleanupDeletionFencedEmail(id); err != nil {
		t.Fatalf("cleanupDeletionFencedEmail with legacy fence: %v", err)
	}
	for _, path := range []string{emlPath, metadataPath, fencePath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("legacy-fence delete retained %s: %v", path, err)
		}
	}
}

// TestDeletionFenceLegacyPlainTextRecoversOnRestart proves the restart
// recovery path converges a legacy fence found on disk.
func TestDeletionFenceLegacyPlainTextRecoversOnRestart(t *testing.T) {
	dir := t.TempDir()
	first, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	const id = "legacy-restart"
	storeDeletionTestMessage(t, first, id)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deletionFencePath(dir, id), []byte(deletionFenceState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	if got := len(restarted.GetAllEmail()); got != 0 {
		t.Fatalf("legacy-fenced email republished after restart: %d", got)
	}
	for _, path := range []string{filepath.Join(dir, id+".eml"), restarted.metadataPath(id), deletionFencePath(dir, id)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("recovery retained %s: %v", path, err)
		}
	}
}

// TestDeletionFenceCheckpointRenameFailureKeepsOldState proves a crash right
// before the checkpoint rename leaves the previous fence payload parseable:
// progress neither advances nor rolls back, and no temporary file lingers.
func TestDeletionFenceCheckpointRenameFailureKeepsOldState(t *testing.T) {
	dir := t.TempDir()
	remote := newCountingAttachmentStore()
	server, err := NewMailServerWithOptions(1025, "localhost", dir, ServerOptions{AttachmentStore: remote})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	const id = "checkpoint-rename"
	storeDeletionTestMessage(t, server, id)

	server.beforeDeletionCheckpointRename = func(string) error {
		return errors.New("injected checkpoint crash")
	}
	if err := server.DeleteEmail(id); err == nil || !strings.Contains(err.Error(), "injected checkpoint crash") {
		t.Fatalf("DeleteEmail() error = %v, want injected checkpoint crash", err)
	}
	fencePath := deletionFencePath(dir, id)
	if _, err := os.Stat(fencePath); err != nil {
		t.Fatalf("fence lost after checkpoint crash: %v", err)
	}
	if got := readFenceCompleted(t, server, id); len(got) != 0 {
		t.Fatalf("checkpoint advanced despite failed rename: %v", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), storageTempPrefix+"fence-") {
			t.Fatalf("temporary checkpoint survived crash: %s", entry.Name())
		}
	}

	// Remote artifacts may already be gone (the layer op ran), but the retry
	// replays the remote layer idempotently and then completes every layer.
	server.beforeDeletionCheckpointRename = nil
	if err := server.DeleteEmail(id); err != nil {
		t.Fatalf("retry DeleteEmail(): %v", err)
	}
	if _, err := os.Stat(fencePath); !os.IsNotExist(err) {
		t.Fatalf("fence retained after converged delete: %v", err)
	}
	if _, err := server.GetEmail(id); err == nil {
		t.Fatal("email still visible after converged delete")
	}
	if got := remote.deleteCount(id); got != 2 {
		t.Fatalf("remote layer replay count = %d, want 2 (idempotent)", got)
	}
}

// TestDeletionFencePayloadValidation covers legacy/garbage reset and
// semantically invalid JSON rejection.
func TestDeletionFencePayloadValidation(t *testing.T) {
	dir := t.TempDir()
	server, err := NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	for _, tc := range []struct {
		name    string
		payload string
		wantErr bool
	}{
		{name: "garbage", payload: "not-json", wantErr: false},
		{name: "wrong id", payload: `{"version":1,"id":"other","completed":[]}`, wantErr: true},
		{name: "wrong version", payload: `{"version":9,"id":"x","completed":[]}`, wantErr: true},
		{name: "unknown layer", payload: `{"version":1,"id":"x","completed":["mystery"]}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(deletionFencePath(dir, "x"), []byte(tc.payload), 0600); err != nil {
				t.Fatal(err)
			}
			checkpoint, err := server.readDeletionFenceCheckpoint("x")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("payload %q accepted: %#v", tc.payload, checkpoint)
				}
				return
			}
			if err != nil {
				t.Fatalf("payload %q rejected: %v", tc.payload, err)
			}
			if len(checkpoint.Completed) != 0 {
				t.Fatalf("payload %q yielded completed layers %v", tc.payload, checkpoint.Completed)
			}
		})
	}
}
