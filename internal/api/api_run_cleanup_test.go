package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/soulteary/owlmail/internal/mailserver"
)

func saveRunEmail(t *testing.T, s *mailserver.MailServer, id, run string) {
	t.Helper()
	if err := s.SaveEmailToStore(id, false, &mailserver.Envelope{To: []string{"a@example.test"}}, &mailserver.Email{
		Subject: "run mail " + id,
		Text:    "body",
		RunID:   run,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRunCleanupEndpointPagesAndFailures(t *testing.T) {
	api, s, _ := setupTestAPI(t)
	defer func() { _ = s.Close() }()

	saveRunEmail(t, s, "ok", "run-a")
	saveRunEmail(t, s, "relay", "run-a")
	saveRunEmail(t, s, "source", "run-a")
	if _, err := api.relayJobs.create("relay", "relay@example.test"); err != nil {
		t.Fatal(err)
	}
	_, release, err := s.AcquireEmailSource("source")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	resp, err := api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-a/emails", nil))
	if err != nil {
		t.Fatal(err)
	}
	var result mailserver.RunCleanupResult
	decodeJSON(t, resp, &result)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(result.Deleted) != 1 || result.Deleted[0] != "ok" || result.Failed != 2 {
		t.Fatalf("result = %+v", result)
	}
	reasons := map[string]string{}
	for _, failure := range result.Failures {
		reasons[failure.ID] = failure.Reason
	}
	if reasons["relay"] != mailserver.RunCleanupReasonRelayPending || reasons["source"] != mailserver.RunCleanupReasonSourceInUse {
		t.Fatalf("failure reasons = %v", reasons)
	}
	// Failed members stay fully readable; the round does not block on them.
	for _, id := range []string{"relay", "source"} {
		if _, err := s.GetEmail(id); err != nil {
			t.Fatalf("failed source %s removed: %v", id, err)
		}
	}

	// Pagination survives cursor round-trips.
	saveRunEmail(t, s, "p1", "run-p")
	saveRunEmail(t, s, "p2", "run-p")
	resp, err = api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-p/emails?limit=1", nil))
	if err != nil {
		t.Fatal(err)
	}
	var page1 mailserver.RunCleanupResult
	decodeJSON(t, resp, &page1)
	_ = resp.Body.Close()
	if !page1.MorePossible || page1.NextCursor == "" || len(page1.Deleted) != 1 {
		t.Fatalf("page1 = %+v", page1)
	}
	resp, err = api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-p/emails?cursor="+page1.NextCursor+"&limit=1", nil))
	if err != nil {
		t.Fatal(err)
	}
	var page2 mailserver.RunCleanupResult
	decodeJSON(t, resp, &page2)
	_ = resp.Body.Close()
	if page2.MorePossible || len(page2.Deleted) != 1 {
		t.Fatalf("page2 = %+v", page2)
	}

	// Deterministic request errors and empty rounds.
	for _, target := range []string{
		"/api/v1/test-runs/run-p/emails?cursor=garbage",
		"/api/v1/test-runs/run-p/emails?limit=0",
		"/api/v1/test-runs/bad~run/emails",
	} {
		resp, err = api.app.Test(httptest.NewRequest(http.MethodDelete, target, nil))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("%s: status %d body %s", target, resp.StatusCode, body)
		}
	}
	resp, err = api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/test-runs/run-empty/emails", nil))
	if err != nil {
		t.Fatal(err)
	}
	var empty mailserver.RunCleanupResult
	decodeJSON(t, resp, &empty)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || empty.Failed != 0 || empty.MorePossible || len(empty.Deleted) != 0 {
		t.Fatalf("empty = %+v (%d)", empty, resp.StatusCode)
	}
}

func TestWaitDeliveriesExpiredCursorMapping(t *testing.T) {
	api, s, _ := setupTestAPI(t)
	defer func() { _ = s.Close() }()
	cursor := s.DeliveryCursor()
	saveRunEmail(t, s, "later", "run-a")
	if err := s.DeleteAllEmail(); err != nil {
		t.Fatal(err)
	}
	body := `{"after":"` + cursor + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/emails/wait", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := api.app.Test(req, fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var payload struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	decodeJSON(t, resp, &payload)
	if resp.StatusCode != 400 || payload.Code != ErrorCodeCursorExpired || payload.Error != "CURSOR_EXPIRED" {
		t.Fatalf("status=%d payload=%+v", resp.StatusCode, payload)
	}
}

func TestSingleDeletePendingRelayStillConflicts(t *testing.T) {
	api, s, _ := setupTestAPI(t)
	defer func() { _ = s.Close() }()
	saveRunEmail(t, s, "single", "run-z")
	if _, err := api.relayJobs.create("single", "relay@example.test"); err != nil {
		t.Fatal(err)
	}
	resp, err := api.app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/emails/single", nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 409 {
		t.Fatalf("v1 single delete status = %d, want 409", resp.StatusCode)
	}
	if _, err := s.GetEmail("single"); err != nil {
		t.Fatal("v1 pending-relay delete removed the source")
	}
}

func TestStartupRunCleanupTakeoverAssembly(t *testing.T) {
	dir := t.TempDir()

	first, err := mailserver.NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	firstAPI := NewAPIWithHTTPSDeferredRecovery(first, 1080, "localhost", "", "", false, "", "")
	raw := "From: sender@example.test\r\nTo: a@example.test\r\nX-OwlMail-Run-ID: run-r\r\nSubject: takeover\r\n\r\nbody"
	if err := os.WriteFile(filepath.Join(dir, "relay-mail.eml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := first.LoadMailsFromDirectory(); err != nil {
		t.Fatal(err)
	}
	job, err := firstAPI.relayJobs.create("relay-mail", "relay@example.test")
	if err != nil {
		t.Fatal(err)
	}
	// An interrupted mid-page snapshot parked on disk.
	snapshotID := "0123456789abcdef0123456789abcdef"
	snapshotJSON := `{"version":1,"snapshot_id":"` + snapshotID + `","run_id":"run-r","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","ids":["relay-mail"],"next_index":0,"deleted":[]}`
	snapshotPath := filepath.Join(dir, ".owlmail-meta", "run-cleanups", snapshotID+".json")
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath, []byte(snapshotJSON), 0600); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := mailserver.NewMailServer(1025, "localhost", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	api2 := NewAPIWithHTTPSDeferredRecovery(restarted, 1081, "localhost", "", "", false, "", "")
	if len(restarted.RunCleanupSnapshots()) != 1 {
		t.Fatal("parked snapshot not registered at construction")
	}

	// Synchronous takeover (the assembly path runs the same method in a goroutine).
	api2.resumeRunCleanups()
	if _, err := restarted.GetEmail("relay-mail"); err != nil {
		t.Fatalf("queued relay source removed during takeover: %v", err)
	}
	if len(restarted.RunCleanupSnapshots()) != 1 {
		t.Fatal("snapshot should stay parked while the relay is queued")
	}
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("parked snapshot file lost: %v", err)
	}

	// Once the relay finishes (job completion + source release), a later
	// takeover completes the round quietly.
	api2.finishRelayAttempt(job.ID, nil)
	api2.resumeRunCleanups()
	if _, err := restarted.GetEmail("relay-mail"); err == nil {
		t.Fatal("queued relay source not deleted after release")
	}
	if len(restarted.RunCleanupSnapshots()) != 0 {
		t.Fatal("snapshot retained after successful takeover")
	}
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Fatalf("snapshot file retained: %v", err)
	}
}

func decodeJSON(t *testing.T, resp *http.Response, target interface{}) {
	t.Helper()
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
