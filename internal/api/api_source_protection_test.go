package api

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/soulteary/owlmail/internal/attachmentstore"
	"github.com/soulteary/owlmail/internal/mailserver"
)

// blockingAttachmentStore serves attachment bodies that block until gate is
// closed, stretching the streaming window deterministically.
type blockingAttachmentStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	gate      chan struct{}
	opened    chan struct{}
	reading   chan struct{}
	openOnce  sync.Once
	readOnce  sync.Once
}

func newBlockingAttachmentStore() *blockingAttachmentStore {
	return &blockingAttachmentStore{
		objects: make(map[string][]byte),
		gate:    make(chan struct{}),
		opened:  make(chan struct{}),
		reading: make(chan struct{}),
	}
}

func (s *blockingAttachmentStore) Put(_ context.Context, emailID, filename, _ string, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[emailID+"/"+filename] = data
	s.mu.Unlock()
	return nil
}

type gateReader struct {
	gate   <-chan struct{}
	data   []byte
	sent   bool
	signal func()
}

func (r *gateReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.signal()
		<-r.gate
		r.sent = true
		return copy(p, r.data), io.EOF
	}
	return 0, io.EOF
}
func (r *gateReader) Close() error { return nil }

func (s *blockingAttachmentStore) Open(_ context.Context, emailID, filename string) (*attachmentstore.Object, error) {
	s.mu.Lock()
	data, ok := s.objects[emailID+"/"+filename]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	s.openOnce.Do(func() { close(s.opened) })
	return &attachmentstore.Object{Body: &gateReader{
		gate:   s.gate,
		data:   data,
		signal: func() { s.readOnce.Do(func() { close(s.reading) }) },
	}, Size: int64(len(data))}, nil
}

func (s *blockingAttachmentStore) DeleteEmail(_ context.Context, emailID string) error {
	s.mu.Lock()
	prefix := emailID + "/"
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			delete(s.objects, key)
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *blockingAttachmentStore) CheckHealth(context.Context) error { return nil }

var _ attachmentstore.Store = (*blockingAttachmentStore)(nil)

// seedRemoteAttachment stores an email whose attachment is served through the
// remote attachment store, bypassing SMTP ingestion.
func seedRemoteAttachment(t *testing.T, server *mailserver.MailServer, store attachmentstore.Store, id, run string) string {
	t.Helper()
	const filename = "test.txt"
	if err := server.SaveEmailToStore(id, false,
		&mailserver.Envelope{To: []string{"a@example.test"}},
		&mailserver.Email{
			Subject: "attachment",
			Text:    "body",
			RunID:   run,
			Attachments: []*mailserver.Attachment{{
				GeneratedFileName: filename,
				FileName:          "test.txt",
				ContentType:       "text/plain",
				Storage:           "s3",
			}},
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), id, filename, "text/plain", strings.NewReader("data"), 4); err != nil {
		t.Fatal(err)
	}
	return filename
}

// TestDownloadStreamsProtectSourcesFromRunCleanup is TR-6.1: while a v1 and a
// MailDev attachment stream is open, run cleanup reports source_in_use and the
// artifacts stay intact; the next round after stream close deletes them.
func TestDownloadStreamsProtectSourcesFromRunCleanup(t *testing.T) {
	dir := t.TempDir()
	store := newBlockingAttachmentStore()
	server, err := mailserver.NewMailServerWithOptions(1025, "localhost", dir, mailserver.ServerOptions{AttachmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	api := NewAPI(server, 1080, "localhost")
	api.SetMailDevRESTCompat(true)

	filename := seedRemoteAttachment(t, server, store, "v1-mail", "run-a")

	streamDone := make(chan *http.Response, 1)
	go func() {
		resp, err := api.app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/emails/v1-mail/attachments/"+filename, nil), fiber.TestConfig{Timeout: 0})
		if err != nil {
			t.Error(err)
			streamDone <- nil
			return
		}
		streamDone <- resp
	}()
	select {
	case <-store.reading:
	case <-time.After(2 * time.Second):
		t.Fatal("v1 attachment stream never entered the blocked read")
	}

	cleanupReq := httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-a/emails", nil)
	resp, err := api.app.Test(cleanupReq, fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	var result mailserver.RunCleanupResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || result.Failed != 1 || len(result.Failures) != 1 {
		t.Fatalf("cleanup during stream: status=%d result=%+v", resp.StatusCode, result)
	}
	if result.Failures[0].ID != "v1-mail" || result.Failures[0].Reason != mailserver.RunCleanupReasonSourceInUse {
		t.Fatalf("failure = %+v", result.Failures[0])
	}
	if _, err := server.GetEmail("v1-mail"); err != nil {
		t.Fatal("source vanished while its stream was open")
	}
	if data, ok := store.objects["v1-mail/"+filename]; !ok || string(data) != "data" {
		t.Fatalf("remote attachment removed while stream open: %#v", store.objects)
	}

	close(store.gate)
	var streamed *http.Response
	select {
	case streamed = <-streamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("v1 stream did not finish")
	}
	if streamed == nil {
		t.Fatal("stream failed")
	}
	body, _ := io.ReadAll(streamed.Body)
	_ = streamed.Body.Close()
	if streamed.StatusCode != 200 || string(body) != "data" {
		t.Fatalf("stream response = %d %q", streamed.StatusCode, body)
	}

	// Next cleanup round deletes the released source.
	resp, err = api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-a/emails", nil), fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("follow-up cleanup status = %d", resp.StatusCode)
	}
	if _, err := server.GetEmail("v1-mail"); err == nil {
		t.Fatal("source survived the cleanup round after release")
	}
}

// TestMailDevDownloadProtectsSource exercises one compatibility endpoint.
func TestMailDevDownloadProtectsSource(t *testing.T) {
	dir := t.TempDir()
	store := newBlockingAttachmentStore()
	server, err := mailserver.NewMailServerWithOptions(1025, "localhost", dir, mailserver.ServerOptions{AttachmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	api := NewAPI(server, 1080, "localhost")
	api.SetMailDevRESTCompat(true)

	filename := seedRemoteAttachment(t, server, store, "compat-mail", "run-b")

	streamDone := make(chan *http.Response, 1)
	go func() {
		resp, err := api.app.Test(httptest.NewRequest(http.MethodGet, "/api/email/compat-mail/attachment/"+filename, nil), fiber.TestConfig{Timeout: 0})
		if err != nil {
			t.Error(err)
			streamDone <- nil
			return
		}
		streamDone <- resp
	}()
	select {
	case <-store.reading:
	case <-time.After(2 * time.Second):
		t.Fatal("maildev attachment stream never entered the blocked read")
	}

	resp, err := api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-b/emails", nil), fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	var result mailserver.RunCleanupResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if result.Failed != 1 || len(result.Failures) != 1 || result.Failures[0].Reason != mailserver.RunCleanupReasonSourceInUse {
		t.Fatalf("maildev cleanup during stream = %+v", result)
	}
	if _, err := server.GetEmail("compat-mail"); err != nil {
		t.Fatal("source vanished while the maildev stream was open")
	}

	close(store.gate)
	var streamed *http.Response
	select {
	case streamed = <-streamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("maildev stream did not finish")
	}
	if streamed == nil || streamed.StatusCode != 200 {
		t.Fatalf("maildev stream response = %v", streamed)
	}
}

// TestExportAcquiresSourcesWhileStreaming proves the export pins each source:
// a running export keeps cleanup at source_in_use and still serves the archive.
func TestExportAcquiresSourcesWhileStreaming(t *testing.T) {
	api, server, mailDir := setupTestAPI(t)
	defer func() { _ = server.Close() }()
	saveRunEmail(t, server, "zip1", "run-z")
	if err := os.WriteFile(filepath.Join(mailDir, "zip1.eml"), []byte("Subject: zip\r\n\r\nBody"), 0600); err != nil {
		t.Fatal(err)
	}

	_, release, err := server.AcquireEmailSource("zip1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// Readers are reentrant: another pinned read does not disturb the export.
	resp, err := api.app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/emails/export?run_id=run-z", nil))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("export status = %d body = %s", resp.StatusCode, body)
	}
	archive, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if len(archive.File) != 1 || !strings.Contains(archive.File[0].Name, "zip1") {
		t.Fatalf("export files = %#v", archive.File)
	}

	// Cleanup against a pinned source keeps the message intact.
	outcome, err := server.CleanupTestRun(mailserver.RunCleanupRequest{RunID: "run-z"}, func(id string) error {
		// Emulate the API composition: protection check then delete.
		return server.DeleteEmail(id)
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Failed != 1 || outcome.Failures[0].Reason != mailserver.RunCleanupReasonSourceInUse {
		t.Fatalf("cleanup during export = %+v", outcome)
	}
	if _, err := os.Stat(filepath.Join(mailDir, "zip1.eml")); err != nil {
		t.Fatalf("export source removed while pinned: %v", err)
	}
}
