package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestServiceConcurrencyLimitAppliesToIndividualTargets(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{
		{Name: "first", URL: receiver.URL},
		{Name: "second", URL: receiver.URL},
	}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan []Result, 1)
	service, err := NewService(dispatcher, ServiceOptions{
		MaxConcurrency:  1,
		ShutdownTimeout: time.Second,
		OnResults: func(_ string, results []Result) {
			completed <- results
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close() }()
	if err := service.Enqueue(testEmail()); err != nil {
		t.Fatal(err)
	}
	select {
	case results := <-completed:
		if len(results) != 2 {
			t.Fatalf("result count = %d, want 2", len(results))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deliveries")
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent target requests = %d, want 1", got)
	}
}

func TestServiceCloseDrainsInFlightDelivery(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "drain", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(dispatcher, ServiceOptions{MaxConcurrency: 1, ShutdownTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Enqueue(testEmail()); err != nil {
		t.Fatal(err)
	}
	<-started
	closed := make(chan error, 1)
	go func() { closed <- service.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before delivery completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := service.Enqueue(testEmail()); err == nil {
		t.Fatal("enqueue after close should fail")
	}
}

type fakeDeliveryQueue struct {
	claims     chan *queueReceipt
	mu         sync.Mutex
	markers    map[string]string // handoff job ID -> stream ID or "done"
	stream     map[string]deliveryJob
	owners     map[string]string // stream ID -> current consumer
	tokens     map[string]string // stream ID -> current fencing token
	states     map[string]map[string]targetState
	dead       []deadLetterRecord
	renewed    []string
	enqueueErr error
	uncertain  bool
	nextID     int
}

func newFakeDeliveryQueue() *fakeDeliveryQueue {
	return &fakeDeliveryQueue{
		claims:  make(chan *queueReceipt, 4),
		markers: make(map[string]string),
		stream:  make(map[string]deliveryJob),
		owners:  make(map[string]string),
		tokens:  make(map[string]string),
		states:  make(map[string]map[string]targetState),
	}
}

// Enqueue mirrors the Redis handoff marker: a repeated call for the same job
// ID reconciles against the existing entry instead of duplicating it.
func (queue *fakeDeliveryQueue) Enqueue(_ context.Context, job deliveryJob) error {
	queue.mu.Lock()
	if queue.enqueueErr != nil && !queue.uncertain {
		err := queue.enqueueErr
		queue.mu.Unlock()
		return err
	}
	err := queue.enqueueErr
	uncertain := queue.uncertain
	if marker, exists := queue.markers[job.ID]; exists {
		if existing, ok := queue.stream[marker]; ok && marker != redisHandoffDone && existing.ID != job.ID {
			queue.mu.Unlock()
			return fmt.Errorf("handoff identity mismatch for %s", job.ID)
		}
		queue.mu.Unlock()
		return err
	}
	queue.nextID++
	id := fmt.Sprintf("fake-%d", queue.nextID)
	queue.stream[id] = job
	queue.markers[job.ID] = id
	queue.owners[id] = "test-consumer"
	queue.tokens[id] = "token-" + id
	receipt := &queueReceipt{id: id, token: "token-" + id, job: job}
	queue.mu.Unlock()
	if uncertain && err == nil {
		err = errors.New("simulated lost enqueue response")
	}
	if err == nil {
		queue.claims <- receipt
	}
	return err
}

func (queue *fakeDeliveryQueue) streamEntry(id string) (deliveryJob, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	job, ok := queue.stream[id]
	return job, ok
}

func (queue *fakeDeliveryQueue) stealLease(id string) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.owners[id] = "other-consumer"
	queue.tokens[id] = "stolen-token"
}

func (queue *fakeDeliveryQueue) snapshot() (map[string]string, map[string]map[string]targetState, []deadLetterRecord) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	markers := make(map[string]string, len(queue.markers))
	for key, value := range queue.markers {
		markers[key] = value
	}
	states := make(map[string]map[string]targetState, len(queue.states))
	for key, value := range queue.states {
		copied := make(map[string]targetState, len(value))
		for target, state := range value {
			copied[target] = state
		}
		states[key] = copied
	}
	return markers, states, append([]deadLetterRecord(nil), queue.dead...)
}

func TestOutboxRetainsJobUntilQueueAcceptsIt(t *testing.T) {
	outbox, err := newDeliveryOutbox(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "durable-job", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Commit(job.Email.ID); err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	queue.enqueueErr = errors.New("redis unavailable")
	service := &Service{queue: queue, outbox: outbox, ctx: context.Background()}

	if pending, err := service.flushOutbox(); err == nil || !pending {
		t.Fatalf("flushOutbox() = %v, %v; expected retained failure", pending, err)
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 1 {
		t.Fatalf("outbox after queue failure = %#v, %v", entries, err)
	}

	queue.mu.Lock()
	queue.enqueueErr = nil
	queue.mu.Unlock()
	if pending, err := service.flushOutbox(); err != nil || !pending {
		t.Fatalf("flushOutbox() retry = %v, %v", pending, err)
	}
	entries, err = outbox.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("outbox after successful handoff = %#v, %v", entries, err)
	}
}

func TestOutboxDecouplesEnqueueFromMemoryQueueCapacity(t *testing.T) {
	outbox, err := newDeliveryOutbox(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		outbox: outbox, outboxWake: make(chan struct{}, 1),
		memoryQueue: make(chan deliveryJob), ctx: context.Background(), accepting: true,
	}
	completed := make(chan error, 1)
	go func() { completed <- service.Enqueue(testEmail()) }()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked on the unavailable in-memory queue")
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("staged outbox entries became consumable before acceptance = %#v, %v", entries, err)
	}
	select {
	case <-service.outboxWake:
		t.Fatal("staged outbox entry woke the worker before acceptance")
	default:
	}
	if err := service.Commit(testEmail().ID); err != nil {
		t.Fatal(err)
	}
	entries, err = outbox.List()
	if err != nil || len(entries) != 1 {
		t.Fatalf("durable outbox entries = %#v, %v", entries, err)
	}
}

func TestServiceCloseWaitsForStagedHandoffDecision(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "staged", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(dispatcher, ServiceOptions{
		MaxConcurrency: 1, ShutdownTimeout: time.Second, SpoolDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	email := testEmail()
	if err := service.Enqueue(email); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- service.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before staged handoff was committed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := service.Commit(email.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after staged handoff was committed")
	}
}

// blockOutboxDirectory replaces the outbox directory with a regular file so
// that promotion fails.
//
// Writing the file straight after renaming the directory aside races the
// service: runOutbox calls flushOutbox on every pass, flushOutbox calls
// deliveryOutbox.List, and List calls ensureDirectory, which recreates the
// directory with MkdirAll. A pass landing between the rename and the write
// leaves the path a directory again, and the write fails with
// "is a directory" -- rarely on an idle machine, but reproducibly enough on a
// loaded CI runner to turn an unrelated pull request red.
//
// Retrying keeps the test deterministic without pausing the worker or relying
// on file modes, which a container running as root ignores. The recreated
// directory is empty while promotion is blocked, so removing it reopens the
// window for the next attempt.
func blockOutboxDirectory(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := os.WriteFile(dir, []byte("blocks outbox directory"), 0600); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("could not replace outbox directory %s with a blocking file: %v", dir, err)
		}
		_ = os.Remove(dir)
		time.Sleep(time.Millisecond)
	}
}

func TestServiceCloseRetainsFailedPromotionUntilRetrySucceeds(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "retry", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(dispatcher, ServiceOptions{
		MaxConcurrency: 1, ShutdownTimeout: time.Second, SpoolDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	email := testEmail()
	if err := service.Enqueue(email); err != nil {
		t.Fatal(err)
	}
	backupDir := service.outbox.dir + "-backup"
	if err := os.Rename(service.outbox.dir, backupDir); err != nil {
		t.Fatal(err)
	}
	blockOutboxDirectory(t, service.outbox.dir)
	if err := service.Commit(email.ID); err == nil {
		t.Fatal("blocked outbox promotion unexpectedly succeeded")
	}

	closed := make(chan error, 1)
	go func() { closed <- service.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while accepted promotion was still failing: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := os.Remove(service.outbox.dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backupDir, service.outbox.dir); err != nil {
		t.Fatal(err)
	}
	service.wakeOutbox()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the promotion retry succeeded")
	}
}

func TestServiceCloseTimeoutCancelsPersistentlyFailedPromotion(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "timeout", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(dispatcher, ServiceOptions{
		MaxConcurrency: 1, ShutdownTimeout: 50 * time.Millisecond, SpoolDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	email := testEmail()
	if err := service.Enqueue(email); err != nil {
		t.Fatal(err)
	}
	backupDir := service.outbox.dir + "-backup"
	if err := os.Rename(service.outbox.dir, backupDir); err != nil {
		t.Fatal(err)
	}
	blockOutboxDirectory(t, service.outbox.dir)
	if err := service.Commit(email.ID); err == nil {
		t.Fatal("blocked outbox promotion unexpectedly succeeded")
	}
	if err := service.Close(); err == nil || !strings.Contains(err.Error(), "webhook drain exceeded") {
		t.Fatalf("Close error = %v, want bounded drain timeout", err)
	}
	if err := os.Remove(service.outbox.dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backupDir, service.outbox.dir); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxRecreatesMissingDirectory(t *testing.T) {
	outbox, err := newDeliveryOutbox(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(outbox.dir); err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "recreated", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatalf("Store() after directory removal: %v", err)
	}
	if err := outbox.Commit(job.Email.ID); err != nil {
		t.Fatal(err)
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 1 {
		t.Fatalf("List() after directory recreation = %#v, %v", entries, err)
	}
}

func TestOutboxStageSyncFailureRemainsNonConsumable(t *testing.T) {
	outbox, err := newDeliveryOutbox(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox.syncDirectory = func(string) error {
		return errors.New("injected directory sync failure")
	}
	service := &Service{
		outbox: outbox, outboxWake: make(chan struct{}, 1),
		ctx: context.Background(), accepting: true,
	}

	if err := service.Enqueue(testEmail()); err == nil {
		t.Fatal("unsynced pending outbox handoff must fail")
	}
	select {
	case <-service.outboxWake:
		// The worker is woken to reclaim the rejected pending entry after the
		// mail rollback decision becomes visible.
	default:
		t.Fatal("failed staged handoff did not schedule pending cleanup")
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed staged handoff became consumable = %#v, %v", entries, err)
	}
	files, err := os.ReadDir(outbox.dir)
	if err != nil || len(files) != 1 || !strings.HasSuffix(files[0].Name(), ".pending") {
		t.Fatalf("non-consumable pending handoff = %#v, %v", files, err)
	}
}

func TestOutboxRetriesAcceptedPendingPromotion(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "retry-promotion", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	backupDir := outbox.dir + "-backup"
	if err := os.Rename(outbox.dir, backupDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outbox.dir, []byte("blocks outbox directory"), 0600); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		outbox: outbox, outboxWake: make(chan struct{}, 1),
		memoryQueue: make(chan deliveryJob, 1), ctx: context.Background(),
	}
	if err := service.Commit(job.Email.ID); err == nil {
		t.Fatal("blocked outbox promotion unexpectedly succeeded")
	}
	if err := os.Remove(outbox.dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backupDir, outbox.dir); err != nil {
		t.Fatal(err)
	}

	if pending, err := service.flushOutbox(); err != nil || !pending {
		t.Fatalf("promotion retry flush = %v, %v", pending, err)
	}
	select {
	case delivered := <-service.memoryQueue:
		if delivered.ID != job.ID {
			t.Fatalf("promoted job = %q, want %q", delivered.ID, job.ID)
		}
	default:
		t.Fatal("accepted pending job was not delivered after promotion retry")
	}
}

func TestOutboxRetryResyncsRenamedPromotionBeforeFenceCleanup(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "resync-promotion", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	fencePath := filepath.Join(spoolDir, mailRollbackFencePrefix+job.Email.ID+mailRollbackFenceSuffix)
	if err := os.WriteFile(fencePath, []byte(mailAcceptedState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	outbox.syncDirectory = func(string) error {
		return errors.New("injected committed outbox sync failure")
	}
	if err := outbox.Commit(job.Email.ID); err == nil {
		t.Fatal("promotion with failed directory sync unexpectedly succeeded")
	}
	if _, err := os.Stat(fencePath); err != nil {
		t.Fatalf("accepted fence removed before durable promotion: %v", err)
	}
	files, err := os.ReadDir(outbox.dir)
	if err != nil || len(files) != 1 || !strings.HasSuffix(files[0].Name(), ".json") {
		t.Fatalf("renamed promotion after sync failure = %#v, %v", files, err)
	}

	syncCalls := 0
	outbox.syncDirectory = func(string) error {
		syncCalls++
		return nil
	}
	if err := outbox.Commit(job.Email.ID); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("promotion retry directory sync calls = %d, want 1", syncCalls)
	}
	if _, err := os.Stat(fencePath); !os.IsNotExist(err) {
		t.Fatalf("accepted fence survived durable promotion retry: %v", err)
	}
}

func TestOutboxPrunesPendingJobForRollbackFencedMail(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "rejected-pending", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	fencePath := filepath.Join(spoolDir, mailRollbackFencePrefix+job.Email.ID+mailRollbackFenceSuffix)
	if err := os.WriteFile(fencePath, []byte(mailRollbackState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := outbox.PruneRejectedPending(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(outbox.dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("outbox after rejected pending cleanup = %#v, %v", files, err)
	}
}

func TestOutboxKeepsPendingJobForActiveMailTransaction(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "active-pending", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	fencePath := filepath.Join(spoolDir, mailRollbackFencePrefix+job.Email.ID+mailRollbackFenceSuffix)
	if err := os.WriteFile(fencePath, []byte(mailActiveState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := outbox.PruneRejectedPending(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(outbox.dir)
	if err != nil || len(files) != 1 || !strings.HasSuffix(files[0].Name(), ".pending") {
		t.Fatalf("active transaction pending job = %#v, %v", files, err)
	}
}

func TestOutboxKeepsPendingJobWithoutExplicitRollbackFence(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "accepted-then-deleted", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	if err := outbox.PruneRejectedPending(); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Commit(job.Email.ID); err != nil {
		t.Fatal(err)
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 1 || entries[0].job.ID != job.ID {
		t.Fatalf("accepted deleted-mail handoff = %#v, %v", entries, err)
	}
}

func TestRecoverAcceptedPendingAfterEmailDeletion(t *testing.T) {
	spoolDir := t.TempDir()
	outbox, err := newDeliveryOutbox(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ID: "recover-deleted-accepted", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	fencePath := filepath.Join(spoolDir, mailRollbackFencePrefix+job.Email.ID+mailRollbackFenceSuffix)
	if err := os.WriteFile(fencePath, []byte(mailAcceptedState+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	service := &Service{outbox: outbox, outboxWake: make(chan struct{}, 1)}
	if err := service.RecoverAcceptedPending(); err != nil {
		t.Fatal(err)
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 1 || entries[0].job.ID != job.ID {
		t.Fatalf("recovered accepted deleted-mail handoff = %#v, %v", entries, err)
	}
	if _, err := os.Stat(fencePath); !os.IsNotExist(err) {
		t.Fatalf("accepted recovery fence survived promotion: %v", err)
	}
}

func TestOutboxCloseSignalFlushesAcceptedEntries(t *testing.T) {
	outbox, err := newDeliveryOutbox(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		outbox: outbox, outboxWake: make(chan struct{}, 1), outboxClosing: make(chan struct{}),
		memoryQueue: make(chan deliveryJob, 1), ctx: ctx,
	}
	service.outboxWorkers.Add(1)
	go service.runOutbox()
	// Let the worker observe an empty outbox and wait for work.
	time.Sleep(10 * time.Millisecond)
	job := deliveryJob{ID: "accepted-before-close", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := outbox.Store(job); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Commit(job.Email.ID); err != nil {
		t.Fatal(err)
	}
	close(service.outboxClosing)
	service.outboxWorkers.Wait()
	select {
	case delivered := <-service.memoryQueue:
		if delivered.ID != job.ID {
			t.Fatalf("delivered job = %q, want %q", delivered.ID, job.ID)
		}
	default:
		t.Fatal("accepted outbox job was not flushed during close")
	}
	entries, err := outbox.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("outbox after close = %#v, %v", entries, err)
	}
}

func (queue *fakeDeliveryQueue) Claim(ctx context.Context) (*queueReceipt, error) {
	select {
	case receipt := <-queue.claims:
		queue.mu.Lock()
		if receipt.token != "" {
			queue.tokens[receipt.id] = receipt.token
			queue.owners[receipt.id] = "test-consumer"
		}
		if _, exists := queue.stream[receipt.id]; !exists {
			queue.stream[receipt.id] = receipt.job
		}
		queue.mu.Unlock()
		return receipt, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, nil
	}
}

func (queue *fakeDeliveryQueue) Renew(_ context.Context, receipt *queueReceipt) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	current, ok := queue.tokens[receipt.id]
	if !ok || current != receipt.token {
		return ErrLeaseNotOwned
	}
	queue.renewed = append(queue.renewed, receipt.id)
	return nil
}

func (queue *fakeDeliveryQueue) RecordResult(_ context.Context, receipt *queueReceipt, target string, state targetState) (leaseDisposition, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	current, tokenExists := queue.tokens[receipt.id]
	_, owned := queue.owners[receipt.id]
	if !owned {
		return leaseFinalized, nil
	}
	if !tokenExists || current != receipt.token {
		return leaseLost, nil
	}
	if queue.states[receipt.job.ID] == nil {
		queue.states[receipt.job.ID] = make(map[string]targetState)
	}
	queue.states[receipt.job.ID][target] = state
	return leaseHeld, nil
}

func (queue *fakeDeliveryQueue) Finalize(_ context.Context, receipt *queueReceipt, expected []string) (finalizeDisposition, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, owned := queue.owners[receipt.id]; !owned {
		return finalizeAlreadyDone, nil
	}
	if current, ok := queue.tokens[receipt.id]; !ok || current != receipt.token {
		return finalizeLeaseLost, nil
	}
	states := queue.states[receipt.job.ID]
	wanted := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		wanted[name] = struct{}{}
	}
	for name := range states {
		if _, keep := wanted[name]; !keep {
			delete(states, name)
		}
	}
	for _, name := range expected {
		state, ok := states[name]
		if !ok || !state.terminal() {
			// Retryable or unattempted targets retain the entry; release the
			// current lease for the next claim generation.
			delete(queue.tokens, receipt.id)
			return finalizeRetained, nil
		}
	}
	record := deadLetterRecord{Job: receipt.job, FailedAt: time.Now().UTC()}
	for _, name := range expected {
		state := states[name]
		if state.Stage == targetStageFailed {
			record.Failures = append(record.Failures, failure{
				Target: name, StatusCode: state.StatusCode,
				Attempts: state.Attempts, Error: state.Error,
			})
		}
	}
	if len(record.Failures) > 0 {
		queue.dead = append(queue.dead, record)
	}
	delete(queue.owners, receipt.id)
	delete(queue.tokens, receipt.id)
	delete(queue.stream, receipt.id)
	queue.markers[receipt.job.ID] = redisHandoffDone
	return finalizeCompleted, nil
}

func (queue *fakeDeliveryQueue) TargetStates(_ context.Context, jobID string) (map[string]targetState, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	states := make(map[string]targetState)
	for target, state := range queue.states[jobID] {
		states[target] = state
	}
	return states, nil
}

func (queue *fakeDeliveryQueue) ReopenRetry(_ context.Context, job deliveryJob, targets []string) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	delete(queue.markers, job.ID)
	if queue.states[job.ID] == nil {
		queue.states[job.ID] = make(map[string]targetState)
	}
	now := time.Now().UTC()
	for _, target := range targets {
		queue.states[job.ID][target] = pendingTargetState(job.ID, target, now)
	}
	return nil
}

func (queue *fakeDeliveryQueue) RecoverTerminal(_ context.Context) ([]terminalHandoff, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	var handoffs []terminalHandoff
	for jobID, marker := range queue.markers {
		if marker != redisHandoffDone {
			continue
		}
		states := make(map[string]targetState)
		for target, state := range queue.states[jobID] {
			states[target] = state
		}
		if len(states) == 0 {
			continue
		}
		handoffs = append(handoffs, terminalHandoff{jobID: jobID, states: states})
	}
	return handoffs, nil
}

func (queue *fakeDeliveryQueue) Close() error { return nil }

func newDurableTestService(dispatcher *Dispatcher, queue *fakeDeliveryQueue, leaseRefresh time.Duration, onResults func(string, []Result)) (*Service, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	history, err := newDeliveryHistory("")
	if err != nil {
		cancel()
		panic(err)
	}
	service := &Service{
		dispatcher: dispatcher, queue: queue, history: history, ctx: ctx, cancel: cancel,
		shutdownTimeout: 5 * time.Second, workerCount: 1, accepting: true,
		leaseRefresh: leaseRefresh, onResults: onResults,
	}
	service.start()
	return service, cancel
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestDurableServicePermanentFailureFinalizesWithDeadLetter(t *testing.T) {
	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hits.Add(1)
		if request.Header.Get("X-OwlMail-Delivery-ID") != "replayed-job" {
			t.Errorf("delivery ID = %q", request.Header.Get("X-OwlMail-Delivery-ID"))
		}
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "failing", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: deliveryJob{ID: "replayed-job", Email: testEmail()}}
	service, _ := newDurableTestService(dispatcher, queue, 0, nil)
	waitForCondition(t, time.Second, func() bool {
		markers, _, _ := queue.snapshot()
		return markers["replayed-job"] == redisHandoffDone
	}, "permanent failure did not finalize the job")
	if got := hits.Load(); got != 1 {
		t.Fatalf("permanent target HTTP hits = %d, want 1", got)
	}
	_, states, dead := queue.snapshot()
	if len(dead) != 1 || len(dead[0].Failures) != 1 || dead[0].Failures[0].Target != "failing" {
		t.Fatalf("dead letter = %#v", dead)
	}
	if state := states["replayed-job"]["failing"]; state.Stage != targetStageFailed || state.Category != "http_status" {
		t.Fatalf("permanent target state = %#v", state)
	}
	records, _ := service.DeliveryHistory()
	if len(records) != 1 || records[0].Status != "failed" || len(records[0].Outcomes) != 1 {
		t.Fatalf("terminal history = %#v", records)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableServiceRetryableTargetRetainedForNextClaim(t *testing.T) {
	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "retrying", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	job := deliveryJob{ID: "retained-job", Email: testEmail()}
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: job}
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-2", job: job}
	service, _ := newDurableTestService(dispatcher, queue, 0, nil)
	waitForCondition(t, 2*time.Second, func() bool {
		_, states, _ := queue.snapshot()
		return states["retained-job"]["retrying"].Attempts == 2
	}, "retryable target was not redelivered by a later claim")
	markers, states, dead := queue.snapshot()
	if _, retained := queue.streamEntry("redis-entry"); !retained {
		t.Fatalf("retryable job finalized early: %#v", markers)
	}
	if len(dead) != 0 {
		t.Fatalf("retryable target was dead-lettered: %#v", dead)
	}
	if state := states["retained-job"]["retrying"]; state.Stage != targetStageRetry {
		t.Fatalf("retryable target state = %#v", state)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("HTTP hits = %d, want 2", got)
	}
	// Shutdown releases the retained lease promptly instead of waiting for the
	// target to recover; the stream entry survives for restart takeover.
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, exists := queue.tokens["redis-entry"]; exists {
		t.Fatal("retained lease fence was not released on shutdown")
	}
	if _, exists := queue.stream["redis-entry"]; !exists {
		t.Fatal("retained entry was removed on shutdown; restart could not take over")
	}
}

func TestDurableServiceRecoveryContinuesOnlyRemainingTargets(t *testing.T) {
	var good, bad atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/good":
			good.Add(1)
			writer.WriteHeader(http.StatusNoContent)
		case "/bad":
			if bad.Add(1) == 1 {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		}
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{
		{Name: "good", URL: receiver.URL + "/good"},
		{Name: "bad", URL: receiver.URL + "/bad"},
	}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	job := deliveryJob{ID: "multi-job", Email: testEmail()}
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: job}
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-2", job: job}
	service, _ := newDurableTestService(dispatcher, queue, 0, nil)
	waitForCondition(t, 2*time.Second, func() bool {
		markers, _, _ := queue.snapshot()
		return markers["multi-job"] == redisHandoffDone
	}, "recovered claim did not finish the remaining target")
	if got := good.Load(); got != 1 {
		t.Fatalf("successful target was redelivered %d times", got)
	}
	if got := bad.Load(); got != 2 {
		t.Fatalf("retryable target HTTP hits = %d, want 2", got)
	}
	records, _ := service.DeliveryHistory()
	if len(records) != 1 || records[0].Status != "succeeded" || len(records[0].Outcomes) != 2 {
		t.Fatalf("terminal history = %#v", records)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableServicePermanentFailureDoesNotBlockSiblingTarget(t *testing.T) {
	var good atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/good" {
			good.Add(1)
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{
		{Name: "bad", URL: receiver.URL + "/bad"},
		{Name: "good", URL: receiver.URL + "/good"},
	}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: deliveryJob{ID: "mixed-job", Email: testEmail()}}
	service, _ := newDurableTestService(dispatcher, queue, 0, nil)
	waitForCondition(t, time.Second, func() bool {
		markers, _, _ := queue.snapshot()
		return markers["mixed-job"] == redisHandoffDone
	}, "permanent sibling failure blocked job finalization")
	if got := good.Load(); got != 1 {
		t.Fatalf("healthy sibling target hits = %d, want 1", got)
	}
	_, states, dead := queue.snapshot()
	if len(dead) != 1 || len(dead[0].Failures) != 1 || dead[0].Failures[0].Target != "bad" {
		t.Fatalf("dead letter = %#v", dead)
	}
	if states["mixed-job"]["good"].Stage != targetStageSucceeded {
		t.Fatalf("sibling state = %#v", states["mixed-job"])
	}
	records, _ := service.DeliveryHistory()
	if len(records) != 1 || records[0].Status != "failed" || len(records[0].Outcomes) != 2 {
		t.Fatalf("terminal history = %#v", records)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableServiceLeaseLossRejectsLateResults(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	reports := make(chan error, 4)
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "slow", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: deliveryJob{ID: "slow-job", Email: testEmail()}}
	service, _ := newDurableTestService(dispatcher, queue, 5*time.Millisecond, func(_ string, results []Result) {
		for _, result := range results {
			if errors.Is(result.Err, ErrLeaseNotOwned) {
				select {
				case reports <- result.Err:
				default:
				}
			}
		}
	})
	<-started
	queue.stealLease("redis-entry")
	select {
	case err := <-reports:
		if !errors.Is(err, ErrLeaseNotOwned) {
			t.Fatalf("report = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss was not reported")
	}
	waitForCondition(t, time.Second, func() bool {
		_, states, _ := queue.snapshot()
		return len(states["slow-job"]) == 0
	}, "late worker persisted results after losing the lease")
	close(release)
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableServiceRenewsLeaseDuringDelivery(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	dispatcher, err := NewDispatcher(Config{Targets: []Target{{Name: "slow", URL: receiver.URL}}}, receiver.Client())
	if err != nil {
		t.Fatal(err)
	}
	queue := newFakeDeliveryQueue()
	queue.claims <- &queueReceipt{id: "redis-entry", token: "gen-1", job: deliveryJob{ID: "slow-job", Email: testEmail()}}
	service, _ := newDurableTestService(dispatcher, queue, 5*time.Millisecond, nil)
	<-started
	waitForCondition(t, time.Second, func() bool {
		queue.mu.Lock()
		defer queue.mu.Unlock()
		return len(queue.renewed) > 0 && queue.renewed[0] == "redis-entry"
	}, "active delivery lease was not renewed")
	close(release)
	waitForCondition(t, time.Second, func() bool {
		markers, _, _ := queue.snapshot()
		return markers["slow-job"] == redisHandoffDone
	}, "successful delivery did not finalize")
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFakeQueueEnqueueReconcilesUncertainResponse(t *testing.T) {
	queue := newFakeDeliveryQueue()
	job := deliveryJob{ID: "uncertain-job", Email: testEmail()}
	queue.uncertain = true
	if err := queue.Enqueue(context.Background(), job); err == nil {
		t.Fatal("expected simulated lost enqueue response")
	}
	queue.uncertain = false
	if err := queue.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("reconciled enqueue: %v", err)
	}
	markers, _, _ := queue.snapshot()
	if len(markers) != 1 || markers[job.ID] == "" {
		t.Fatalf("handoff markers after reconcile = %#v", markers)
	}
	select {
	case <-queue.claims:
		t.Fatal("reconciled enqueue appended a duplicate stream entry")
	default:
	}
}

func TestStartupBackfillsTerminalDeliveriesFromQueue(t *testing.T) {
	queue := newFakeDeliveryQueue()
	now := time.Now().UTC()
	queue.markers["old-job"] = redisHandoffDone
	queue.states["old-job"] = map[string]targetState{
		"good": {Stage: targetStageSucceeded, DeliveryID: "d1", StatusCode: 204, Attempts: 1, UpdatedAt: now},
		"bad":  {Stage: targetStageFailed, DeliveryID: "d2", StatusCode: 400, Attempts: 1, Category: "http_status", Error: "target returned HTTP 400", UpdatedAt: now},
	}
	history, err := newDeliveryHistory("")
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{queue: queue, history: history, ctx: context.Background()}
	service.recoverTerminalDeliveries()
	records, healthy := service.DeliveryHistory()
	if !healthy || len(records) != 1 {
		t.Fatalf("backfilled history = %#v, healthy=%v", records, healthy)
	}
	if records[0].ID != "old-job" || records[0].Status != "failed" || len(records[0].Outcomes) != 2 {
		t.Fatalf("backfilled record = %#v", records[0])
	}
}

func TestRedisQueueIntegration(t *testing.T) {
	redisURL := testRedisURL()
	if redisURL == "" {
		t.Skip("set OWLMAIL_TEST_REDIS_URL to run Redis Streams integration coverage")
	}
	ctx := context.Background()
	prefix := "owlmail:test:" + uuid.NewString()
	queue, err := newRedisDeliveryQueue(ctx, redisURL, prefix, "test-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = queue.Close() }()
	job := deliveryJob{ID: "integration-job", EnqueuedAt: time.Now().UTC(), Email: testEmail()}
	if err := queue.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	// A lost enqueue response must reconcile instead of duplicating the entry.
	if err := queue.Enqueue(ctx, job); err != nil {
		t.Fatalf("idempotent enqueue: %v", err)
	}
	if length, err := queue.client.XLen(ctx, queue.stream).Result(); err != nil || length != 1 {
		t.Fatalf("stream length after reconcile = %d, %v", length, err)
	}
	receipt, err := queue.Claim(ctx)
	if err != nil || receipt == nil || receipt.job.ID != job.ID || receipt.token == "" {
		t.Fatalf("Claim() = %#v, %v", receipt, err)
	}
	if err := queue.Renew(ctx, receipt); err != nil {
		t.Fatalf("Renew() while owned: %v", err)
	}
	// Another generation claims the PEL entry and installs its own fence.
	other, err := newRedisDeliveryQueue(ctx, redisURL, prefix, "other-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	if _, err := other.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream: queue.stream, Group: redisConsumerGroup, Consumer: other.consumer,
		MinIdle: 0, Messages: []string{receipt.id},
	}).Result(); err != nil {
		t.Fatal(err)
	}
	newToken := uuid.NewString()
	leaseKey := queue.leasePrefix + receipt.id
	if err := other.client.Set(ctx, leaseKey, newToken, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.Renew(ctx, receipt); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale Renew() = %v, want lease lost", err)
	}
	state := targetState{Stage: targetStageSucceeded, DeliveryID: "d", Attempts: 1, StatusCode: 204, UpdatedAt: time.Now().UTC()}
	if disposition, err := queue.RecordResult(ctx, receipt, "target", state); err != nil || disposition != leaseLost {
		t.Fatalf("stale RecordResult() = %v, %v", disposition, err)
	}
	if disposition, err := queue.Finalize(ctx, receipt, []string{"target"}); err != nil || disposition != finalizeLeaseLost {
		t.Fatalf("stale Finalize() = %v, %v", disposition, err)
	}
	// The new generation completes the job and removes the live entry.
	takeover := &queueReceipt{id: receipt.id, token: newToken, job: job}
	if disposition, err := other.RecordResult(ctx, takeover, "target", state); err != nil || disposition != leaseHeld {
		t.Fatalf("takeover RecordResult() = %v, %v", disposition, err)
	}
	if disposition, err := other.Finalize(ctx, takeover, []string{"target"}); err != nil || disposition != finalizeCompleted {
		t.Fatalf("takeover Finalize() = %v, %v", disposition, err)
	}
	if length, err := queue.client.XLen(ctx, queue.stream).Result(); err != nil || length != 0 {
		t.Fatalf("stream length after finalize = %d, %v", length, err)
	}
	if marker, err := queue.client.HGet(ctx, queue.markerKey(job.ID), "s").Result(); err != nil || marker != redisHandoffDone {
		t.Fatalf("handoff marker after finalize = %q, %v", marker, err)
	}
}

func testRedisURL() string {
	return os.Getenv("OWLMAIL_TEST_REDIS_URL")
}
