package mailserver

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/soulteary/owlmail/internal/common"
)

const (
	// RunCleanupDefaultLimit is the page size used when a caller omits limit.
	RunCleanupDefaultLimit = 1000
	// RunCleanupMaxLimit bounds a single cleanup page.
	RunCleanupMaxLimit = 1000

	runCleanupFileVersion = 1
	runCleanupDirectory   = "run-cleanups"
	runCleanupCursorMagic = "rc1"
)

// Failure reasons reported for individual snapshot members.
const (
	// RunCleanupReasonRelayPending means an accepted relay job still owns the source.
	RunCleanupReasonRelayPending = "relay_pending"
	// RunCleanupReasonSourceInUse means a reader or stream still holds the source.
	RunCleanupReasonSourceInUse = "source_in_use"
	// RunCleanupReasonStorageUnavailable means a storage layer deletion failed.
	RunCleanupReasonStorageUnavailable = "storage_unavailable"
)

// ErrRunEmailRelayPending is returned by a cleanup deleter when the email has
// an accepted relay that has not finished with its source.
var ErrRunEmailRelayPending = errors.New("email has a pending relay job")

// ErrRunCleanupInvalidCursor means the cleanup cursor is malformed, belongs to
// another run, points at an unknown/completed snapshot, or is stale.
var ErrRunCleanupInvalidCursor = errors.New("invalid run cleanup cursor")

// RunCleanupDeleter deletes one snapshot member. It is injected by the API so
// the domain service can combine relay-source protection with DeleteEmail.
type RunCleanupDeleter func(id string) error

// RunCleanupRequest starts or continues a paginated test-run cleanup.
type RunCleanupRequest struct {
	RunID  string
	Cursor string
	Limit  int
}

// RunCleanupFailure records one member that could not be deleted and why.
type RunCleanupFailure struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// RunCleanupResult reports one page of a snapshot cleanup. Deleted and
// Failures contain only this page's outcomes; NextCursor is present exactly
// when MorePossible is true.
type RunCleanupResult struct {
	RunID        string              `json:"run_id"`
	Deleted      []string            `json:"deleted"`
	Failed       int                 `json:"failed"`
	Failures     []RunCleanupFailure `json:"failures"`
	MorePossible bool                `json:"more_possible"`
	NextCursor   string              `json:"next_cursor,omitempty"`
}

// RunCleanupStatus describes a registered snapshot for tests and the API.
type RunCleanupStatus struct {
	RunID      string `json:"run_id"`
	SnapshotID string `json:"snapshot_id"`
	Total      int    `json:"total"`
	NextIndex  int    `json:"next_index"`
	Remaining  int    `json:"remaining"`
}

// runCleanupFile is the persisted snapshot payload.
type runCleanupFile struct {
	Version    int                 `json:"version"`
	SnapshotID string              `json:"snapshot_id"`
	RunID      string              `json:"run_id"`
	Sequence   uint64              `json:"sequence,omitempty"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`
	IDs        []string            `json:"ids"`
	NextIndex  int                 `json:"next_index"`
	Deleted    []string            `json:"deleted"`
	Failures   []RunCleanupFailure `json:"failures,omitempty"`
}

type runCleanupSnapshot struct {
	mu   sync.Mutex
	id   string
	run  string
	path string
	file runCleanupFile
}

type runCleanupRegistry struct {
	mu    sync.Mutex
	byRun map[string]*runCleanupSnapshot
	byID  map[string]*runCleanupSnapshot
}

func newRunCleanupRegistry() *runCleanupRegistry {
	return &runCleanupRegistry{
		byRun: make(map[string]*runCleanupSnapshot),
		byID:  make(map[string]*runCleanupSnapshot),
	}
}

// reset drops every in-memory snapshot registration. Durable snapshot files
// survive and reconcile on the next startup instead of being lost.
func (r *runCleanupRegistry) reset() {
	r.mu.Lock()
	r.byRun = make(map[string]*runCleanupSnapshot)
	r.byID = make(map[string]*runCleanupSnapshot)
	r.mu.Unlock()
}

// lookupRun returns the run's registered snapshot without taking its mutex.
func (r *runCleanupRegistry) lookupRun(runID string) *runCleanupSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byRun[runID]
}

// lookupID returns the registered snapshot or loads and registers a parked
// snapshot file. It never acquires the snapshot mutex, so the registry and
// per-snapshot mutexes cannot nest.
func (ms *MailServer) lookupOrLoadSnapshot(snapshotID string) (*runCleanupSnapshot, error) {
	ms.runCleanups.mu.Lock()
	snapshot := ms.runCleanups.byID[snapshotID]
	ms.runCleanups.mu.Unlock()
	if snapshot != nil {
		return snapshot, nil
	}
	file, err := ms.loadRunCleanupFile(snapshotID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: snapshot not found or already completed", ErrRunCleanupInvalidCursor)
		}
		return nil, fmt.Errorf("%w: %v", ErrRunCleanupInvalidCursor, err)
	}
	snapshot = &runCleanupSnapshot{
		id:   file.SnapshotID,
		run:  file.RunID,
		path: runCleanupFilePath(ms.mailDir, file.SnapshotID),
		file: file,
	}
	ms.runCleanups.mu.Lock()
	if existing := ms.runCleanups.byID[snapshotID]; existing != nil {
		snapshot = existing
	} else {
		ms.runCleanups.byID[snapshotID] = snapshot
		if existingRun := ms.runCleanups.byRun[file.RunID]; existingRun == nil {
			ms.runCleanups.byRun[file.RunID] = snapshot
		}
	}
	ms.runCleanups.mu.Unlock()
	return snapshot, nil
}

// unregister removes a finished snapshot's mappings. The only legal nesting
// order is snapshot mutex -> registry mutex; registry methods never take a
// snapshot mutex, so callers may hold snapshot.mu here.
func (ms *MailServer) unregisterSnapshot(snapshot *runCleanupSnapshot) {
	ms.runCleanups.mu.Lock()
	delete(ms.runCleanups.byID, snapshot.id)
	if ms.runCleanups.byRun[snapshot.run] == snapshot {
		delete(ms.runCleanups.byRun, snapshot.run)
	}
	ms.runCleanups.mu.Unlock()
}

func (ms *MailServer) runCleanupDir() string {
	return filepath.Join(ms.mailDir, metadataDirectoryName, runCleanupDirectory)
}

func runCleanupFilePath(mailDir, snapshotID string) string {
	return filepath.Join(mailDir, metadataDirectoryName, runCleanupDirectory, snapshotID+".json")
}

func newSnapshotID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate cleanup snapshot id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func validSnapshotID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// writeRunCleanupFile atomically replaces the snapshot payload.
func (ms *MailServer) writeRunCleanupFile(snapshot *runCleanupSnapshot) error {
	dir := filepath.Dir(snapshot.path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("create run cleanup directory: %w", err)
	}
	snapshot.file.UpdatedAt = time.Now().UTC()
	encoded, err := json.MarshalIndent(&snapshot.file, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run cleanup snapshot: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".run-cleanup-*.tmp")
	if err != nil {
		return fmt.Errorf("create run cleanup snapshot: %w", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod run cleanup snapshot: %w", err)
	}
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write run cleanup snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync run cleanup snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close run cleanup snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, snapshot.path); err != nil {
		return fmt.Errorf("commit run cleanup snapshot: %w", err)
	}
	committed = true
	if err := syncStorageDirectory(dir); err != nil {
		return fmt.Errorf("sync run cleanup directory: %w", err)
	}
	return nil
}

func (ms *MailServer) removeRunCleanupFile(snapshot *runCleanupSnapshot) error {
	if err := os.Remove(snapshot.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove run cleanup snapshot: %w", err)
	}
	return syncStorageDirectory(filepath.Dir(snapshot.path))
}

func (ms *MailServer) loadRunCleanupFile(snapshotID string) (runCleanupFile, error) {
	var file runCleanupFile
	encoded, err := os.ReadFile(runCleanupFilePath(ms.mailDir, snapshotID))
	if err != nil {
		return file, err
	}
	if err := json.Unmarshal(encoded, &file); err != nil {
		return file, fmt.Errorf("parse run cleanup snapshot: %w", err)
	}
	if file.Version != runCleanupFileVersion || file.SnapshotID != snapshotID {
		return file, fmt.Errorf("run cleanup snapshot version/id mismatch")
	}
	if err := ValidateRunID(file.RunID); err != nil || file.RunID == "" {
		return file, fmt.Errorf("run cleanup snapshot has invalid run id: %w", err)
	}
	if file.NextIndex < 0 || file.NextIndex > len(file.IDs) {
		return file, fmt.Errorf("run cleanup snapshot has invalid next_index")
	}
	return file, nil
}

// encodeRunCleanupCursor builds the opaque continuation token.
func encodeRunCleanupCursor(snapshotID string, index int) string {
	payload, _ := json.Marshal(struct {
		S string `json:"s"`
		I int    `json:"i"`
	}{S: snapshotID, I: index})
	return runCleanupCursorMagic + "." + base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRunCleanupCursor(token string) (string, int, error) {
	magic, encoded, ok := strings.Cut(token, ".")
	if !ok || magic != runCleanupCursorMagic {
		return "", 0, ErrRunCleanupInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", 0, fmt.Errorf("%w: malformed token", ErrRunCleanupInvalidCursor)
	}
	var payload struct {
		S string `json:"s"`
		I int    `json:"i"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || !validSnapshotID(payload.S) || payload.I < 0 {
		return "", 0, fmt.Errorf("%w: malformed token", ErrRunCleanupInvalidCursor)
	}
	return payload.S, payload.I, nil
}

// CleanupTestRun deletes exactly the members pinned in one snapshot. A request
// without a cursor pins the currently visible matching set (or resumes the
// run's unfinished snapshot); a cursor continues that snapshot. Emails
// arriving after the pin belong to a later round and are never touched here.
func (ms *MailServer) CleanupTestRun(req RunCleanupRequest, deleteOne RunCleanupDeleter) (RunCleanupResult, error) {
	if deleteOne == nil {
		deleteOne = ms.DeleteEmail
	}
	if err := ValidateRunID(req.RunID); err != nil || req.RunID == "" {
		return RunCleanupResult{}, fmt.Errorf("invalid run_id: %w", err)
	}
	limit := req.Limit
	if limit == 0 {
		limit = RunCleanupDefaultLimit
	}
	if limit < 1 || limit > RunCleanupMaxLimit {
		return RunCleanupResult{}, fmt.Errorf("limit must be between 1 and %d", RunCleanupMaxLimit)
	}

	var snapshot *runCleanupSnapshot
	cursorIndex := -1
	withCursor := strings.TrimSpace(req.Cursor) != ""
	if withCursor {
		snapshotID, index, err := decodeRunCleanupCursor(req.Cursor)
		if err != nil {
			return RunCleanupResult{}, err
		}
		snapshot, err = ms.lookupOrLoadSnapshot(snapshotID)
		if err != nil {
			return RunCleanupResult{}, err
		}
		cursorIndex = index
	} else {
		pinned, empty, err := ms.pinOrResumeRunCleanup(req.RunID)
		if err != nil {
			return RunCleanupResult{}, err
		}
		if empty {
			// No visible match: never create a snapshot file for an empty pin.
			return RunCleanupResult{RunID: req.RunID, Deleted: []string{}, Failures: []RunCleanupFailure{}}, nil
		}
		snapshot = pinned
	}

	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.run != req.RunID || snapshot.file.RunID != req.RunID {
		return RunCleanupResult{}, fmt.Errorf("%w: snapshot belongs to another run", ErrRunCleanupInvalidCursor)
	}
	if withCursor && cursorIndex != snapshot.file.NextIndex {
		return RunCleanupResult{}, fmt.Errorf("%w: stale continuation boundary", ErrRunCleanupInvalidCursor)
	}
	if snapshot.file.NextIndex >= len(snapshot.file.IDs) {
		if !withCursor {
			// Another concurrent cursor-less request already finished this
			// snapshot: the round is complete from this caller's view too.
			return RunCleanupResult{RunID: req.RunID, Deleted: []string{}, Failures: []RunCleanupFailure{}}, nil
		}
		return RunCleanupResult{}, fmt.Errorf("%w: snapshot already completed", ErrRunCleanupInvalidCursor)
	}
	return ms.processRunCleanupPage(snapshot, limit, deleteOne)
}

// pinOrResumeRunCleanup returns the run's existing unfinished snapshot or pins
// a new one. Pinning is serialized per run through the registry mutex; the
// visible matching set and the durable snapshot file are produced under one
// storeMutex critical section so a delivery committing afterwards cannot join
// this round. empty is true when the visible matching set is empty.
func (ms *MailServer) pinOrResumeRunCleanup(runID string) (*runCleanupSnapshot, bool, error) {
	for attempt := 0; attempt < 4; attempt++ {
		if existing := ms.runCleanups.lookupRun(runID); existing != nil {
			existing.mu.Lock()
			incomplete := existing.file.NextIndex < len(existing.file.IDs)
			existing.mu.Unlock()
			if incomplete {
				return existing, false, nil
			}
		}
		snapshot, empty, raced, err := ms.createRunCleanupSnapshot(runID)
		if err != nil {
			return nil, false, err
		}
		if empty {
			return nil, true, nil
		}
		if !raced {
			return snapshot, false, nil
		}
		// Another request registered a snapshot while we pinned; loop to
		// pick it up (or retry once its completion unregisters it).
	}
	return nil, false, fmt.Errorf("create run cleanup snapshot: contention settling")
}

// createRunCleanupSnapshot fixes the visible matching set under the store
// lock and persists the snapshot before releasing anything. It returns
// raced when another request already registered a snapshot for the run.
func (ms *MailServer) createRunCleanupSnapshot(runID string) (snapshot *runCleanupSnapshot, empty, raced bool, err error) {
	ms.runCleanups.mu.Lock()
	defer ms.runCleanups.mu.Unlock()
	if ms.runCleanups.byRun[runID] != nil {
		return nil, false, true, nil
	}

	ms.storeMutex.Lock()
	defer ms.storeMutex.Unlock()
	ids := make([]string, 0)
	for _, id := range ms.storeOrder {
		email := ms.storeByID[id]
		if email != nil && email.RunID == runID {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		if ms.duringRunCleanupPin != nil {
			ms.duringRunCleanupPin(runID, ids)
		}
		return nil, true, false, nil
	}

	snapshotID, idErr := newSnapshotID()
	if idErr != nil {
		common.Error("Failed to allocate run cleanup snapshot id: %v", idErr)
		return nil, false, false, fmt.Errorf("create run cleanup snapshot: %w", idErr)
	}
	now := time.Now().UTC()
	snapshot = &runCleanupSnapshot{
		id:   snapshotID,
		run:  runID,
		path: runCleanupFilePath(ms.mailDir, snapshotID),
		file: runCleanupFile{
			Version:    runCleanupFileVersion,
			SnapshotID: snapshotID,
			RunID:      runID,
			Sequence:   ms.deliverySequence,
			CreatedAt:  now,
			UpdatedAt:  now,
			IDs:        ids,
			NextIndex:  0,
			Deleted:    []string{},
			Failures:   []RunCleanupFailure{},
		},
	}
	if ms.duringRunCleanupPin != nil {
		ms.duringRunCleanupPin(runID, ids)
	}
	// The snapshot must be durable while the same store lock that fixed the
	// set is still held: a delivery committing afterwards cannot join this
	// round, and no member is deleted from an unpinned set.
	if writeErr := ms.writeRunCleanupFile(snapshot); writeErr != nil {
		common.Error("Failed to persist run cleanup snapshot: %v", writeErr)
		return nil, false, false, writeErr
	}
	ms.runCleanups.byID[snapshotID] = snapshot
	ms.runCleanups.byRun[runID] = snapshot
	return snapshot, false, false, nil
}

// processRunCleanupPage deletes at most limit members from the snapshot's
// current boundary. Every member advances the boundary regardless of outcome.
// The file is rewritten after the page, and the snapshot file is removed
// atomically on completion.
func (ms *MailServer) processRunCleanupPage(snapshot *runCleanupSnapshot, limit int, deleteOne RunCleanupDeleter) (RunCleanupResult, error) {
	result := RunCleanupResult{
		RunID:    snapshot.run,
		Deleted:  make([]string, 0),
		Failures: make([]RunCleanupFailure, 0),
	}
	start := snapshot.file.NextIndex
	end := start + limit
	if end > len(snapshot.file.IDs) {
		end = len(snapshot.file.IDs)
	}
	page := snapshot.file.IDs[start:end]
	for _, id := range page {
		reason, err := ms.classifyRunCleanupDeletion(id, deleteOne)
		snapshot.file.NextIndex++
		if err == nil {
			result.Deleted = append(result.Deleted, id)
			snapshot.file.Deleted = append(snapshot.file.Deleted, id)
		} else {
			failure := RunCleanupFailure{ID: id, Reason: reason}
			result.Failures = append(result.Failures, failure)
			snapshot.file.Failures = append(snapshot.file.Failures, failure)
		}
	}
	result.Failed = len(result.Failures)
	result.MorePossible = snapshot.file.NextIndex < len(snapshot.file.IDs)
	if !result.MorePossible {
		if err := ms.removeRunCleanupFile(snapshot); err != nil {
			return result, err
		}
		ms.unregisterSnapshot(snapshot)
		return result, nil
	}
	if err := ms.writeRunCleanupFile(snapshot); err != nil {
		return result, err
	}
	result.NextCursor = encodeRunCleanupCursor(snapshot.id, snapshot.file.NextIndex)
	return result, nil
}

// ResumeRunCleanups takes over every parked or active snapshot after startup.
// It never rescans the mailbox: each snapshot's pinned ids are the only
// boundary. Members already deleted before the crash advance silently without
// delete events or counters; members with a deletion fence converge through
// the layered fence recovery; still-visible members go through the injected
// deleter so queued relay sources are preserved.
func (ms *MailServer) ResumeRunCleanups(deleteOne RunCleanupDeleter) ([]RunCleanupResult, error) {
	if deleteOne == nil {
		deleteOne = ms.DeleteEmailQuiet
	}
	ms.runCleanups.mu.Lock()
	snapshots := make([]*runCleanupSnapshot, 0, len(ms.runCleanups.byID))
	for _, snapshot := range ms.runCleanups.byID {
		snapshots = append(snapshots, snapshot)
	}
	ms.runCleanups.mu.Unlock()
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].file.CreatedAt.Equal(snapshots[j].file.CreatedAt) {
			return snapshots[i].id < snapshots[j].id
		}
		return snapshots[i].file.CreatedAt.Before(snapshots[j].file.CreatedAt)
	})

	results := make([]RunCleanupResult, 0, len(snapshots))
	var resumeErrors []error
	for _, snapshot := range snapshots {
		snapshot.mu.Lock()
		if snapshot.file.NextIndex >= len(snapshot.file.IDs) {
			// A boundary exhausted with failures is parked for another takeover
			// attempt: reset the boundary so released sources get retried while
			// members already gone reconcile as silently confirmed.
			if len(snapshot.file.Failures) == 0 {
				snapshot.mu.Unlock()
				continue
			}
			snapshot.file.NextIndex = 0
			snapshot.file.Failures = []RunCleanupFailure{}
			if err := ms.writeRunCleanupFile(snapshot); err != nil {
				snapshot.mu.Unlock()
				resumeErrors = append(resumeErrors, fmt.Errorf("reset run cleanup %s: %w", snapshot.run, err))
				continue
			}
		}
		result, err := ms.resumeRunCleanupSnapshot(snapshot, deleteOne)
		snapshot.mu.Unlock()
		results = append(results, result)
		if err != nil {
			resumeErrors = append(resumeErrors, fmt.Errorf("resume run cleanup %s: %w", snapshot.run, err))
		}
	}
	return results, errors.Join(resumeErrors...)
}

// resumeRunCleanupSnapshot reconciles one snapshot member by member with the
// snapshot file rewritten after every member.
func (ms *MailServer) resumeRunCleanupSnapshot(snapshot *runCleanupSnapshot, deleteOne RunCleanupDeleter) (RunCleanupResult, error) {
	result := RunCleanupResult{
		RunID:    snapshot.run,
		Deleted:  make([]string, 0),
		Failures: make([]RunCleanupFailure, 0),
	}
	var checkpointErr error
	for snapshot.file.NextIndex < len(snapshot.file.IDs) {
		id := snapshot.file.IDs[snapshot.file.NextIndex]
		outcome := ms.reconcileRunCleanupMember(id, deleteOne)
		snapshot.file.NextIndex++
		switch outcome.kind {
		case runCleanupOutcomeDeleted:
			result.Deleted = append(result.Deleted, id)
			snapshot.file.Deleted = append(snapshot.file.Deleted, id)
			ms.deletedMessages.Add(1)
		case runCleanupOutcomeConfirmedGone:
			// Advance without event, counter, or deleted list: the prior
			// process already accounted for this removal.
		default:
			failure := RunCleanupFailure{ID: id, Reason: outcome.reason}
			result.Failures = append(result.Failures, failure)
			snapshot.file.Failures = append(snapshot.file.Failures, failure)
		}
		if err := ms.writeRunCleanupFile(snapshot); err != nil {
			checkpointErr = err
			break
		}
	}
	result.Failed = len(result.Failures)
	result.MorePossible = snapshot.file.NextIndex < len(snapshot.file.IDs)
	if !result.MorePossible && checkpointErr == nil && result.Failed == 0 {
		if err := ms.removeRunCleanupFile(snapshot); err != nil {
			return result, err
		}
		ms.unregisterSnapshot(snapshot)
		return result, nil
	}
	// Boundary exhausted but a member still fails: keep the parked snapshot so
	// the next takeover (or a later cleanup round) retries the source.
	if checkpointErr == nil {
		if err := ms.writeRunCleanupFile(snapshot); err != nil {
			return result, err
		}
	}
	if result.MorePossible {
		result.NextCursor = encodeRunCleanupCursor(snapshot.id, snapshot.file.NextIndex)
	}
	return result, checkpointErr
}

type runCleanupOutcomeKind int

const (
	runCleanupOutcomeConfirmedGone runCleanupOutcomeKind = iota
	runCleanupOutcomeDeleted
	runCleanupOutcomeFailed
)

type runCleanupOutcome struct {
	kind   runCleanupOutcomeKind
	reason string
}

// reconcileRunCleanupMember classifies one snapshot member during takeover
// without rescanning or emitting delete events.
func (ms *MailServer) reconcileRunCleanupMember(id string, deleteOne RunCleanupDeleter) runCleanupOutcome {
	// An in-flight fence means the previous process had already authorized
	// deletion: converge the remaining layers directly.
	if _, err := os.Stat(deletionFencePath(ms.mailDir, id)); err == nil {
		if err := ms.cleanupDeletionFencedEmail(id); err != nil {
			return runCleanupOutcome{kind: runCleanupOutcomeFailed, reason: RunCleanupReasonStorageUnavailable}
		}
		// A crash between fence creation and runtime removal leaves the email
		// visible in this process; detach it quietly now.
		ms.storeMutex.RLock()
		_, visible := ms.storeByID[id]
		ms.storeMutex.RUnlock()
		if visible {
			ms.deleteEmailFromRuntimeState(id)
		}
		return runCleanupOutcome{kind: runCleanupOutcomeDeleted}
	}

	ms.storeMutex.RLock()
	_, visible := ms.storeByID[id]
	ms.storeMutex.RUnlock()
	if !visible && !ms.emailArtifactsExist(id) {
		// Neither fence, runtime state, nor artifacts: the prior process
		// confirmed this delete; only the boundary needs advancing.
		return runCleanupOutcome{kind: runCleanupOutcomeConfirmedGone}
	}

	reason, err := ms.classifyRunCleanupDeletion(id, deleteOne)
	if err == nil {
		return runCleanupOutcome{kind: runCleanupOutcomeDeleted}
	}
	return runCleanupOutcome{kind: runCleanupOutcomeFailed, reason: reason}
}

// emailArtifactsExist reports whether any on-disk artifact remains for the id.
func (ms *MailServer) emailArtifactsExist(id string) bool {
	for _, path := range []string{
		filepath.Join(ms.mailDir, id+".eml"),
		filepath.Join(ms.mailDir, id),
		ms.metadataPath(id),
	} {
		if _, err := os.Lstat(path); err == nil {
			return true
		}
	}
	return false
}

// classifyRunCleanupDeletion maps a deleter outcome to the failure reason
// taxonomy. Relay/source failures preserve all source data; only a nil error
// means the member was actually removed.
func (ms *MailServer) classifyRunCleanupDeletion(id string, deleteOne RunCleanupDeleter) (string, error) {
	err := deleteOne(id)
	switch {
	case err == nil:
		return "", nil
	case errors.Is(err, ErrRunEmailRelayPending):
		return RunCleanupReasonRelayPending, err
	case errors.Is(err, ErrEmailSourceInUse):
		return RunCleanupReasonSourceInUse, err
	default:
		return RunCleanupReasonStorageUnavailable, err
	}
}

// loadParkedRunCleanups registers unfinished snapshot files after startup or
// reload. It never executes deletions. Corrupt snapshots are reported and
// skipped so they cannot cause an email to be deleted through a bad boundary.
func (ms *MailServer) loadParkedRunCleanups() []error {
	dir := ms.runCleanupDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return []error{fmt.Errorf("read run cleanup directory: %w", err)}
	}
	var loadErrors []error
	ms.runCleanups.mu.Lock()
	defer ms.runCleanups.mu.Unlock()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		snapshotID := strings.TrimSuffix(entry.Name(), ".json")
		if !validSnapshotID(snapshotID) {
			loadErrors = append(loadErrors, fmt.Errorf("ignore run cleanup snapshot with invalid id %q", entry.Name()))
			continue
		}
		if _, exists := ms.runCleanups.byID[snapshotID]; exists {
			continue
		}
		file, err := ms.loadRunCleanupFile(snapshotID)
		if err != nil {
			loadErrors = append(loadErrors, fmt.Errorf("load run cleanup snapshot %s: %w", snapshotID, err))
			continue
		}
		if file.NextIndex >= len(file.IDs) {
			if len(file.Failures) == 0 {
				// Completed snapshot whose removal crashed: drop it directly.
				if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					loadErrors = append(loadErrors, fmt.Errorf("remove completed run cleanup snapshot %s: %w", snapshotID, err))
				}
				continue
			}
			// Boundary exhausted with members still failing: register it so
			// the next takeover retries them.
		}
		snapshot := &runCleanupSnapshot{
			id:   file.SnapshotID,
			run:  file.RunID,
			path: filepath.Join(dir, entry.Name()),
			file: file,
		}
		ms.runCleanups.byID[snapshotID] = snapshot
		// Completed snapshots are unregistered, so a present byRun mapping
		// is always an unfinished run owned by another snapshot.
		if ms.runCleanups.byRun[file.RunID] == nil {
			ms.runCleanups.byRun[file.RunID] = snapshot
		}
	}
	return loadErrors
}

// RunCleanupSnapshots returns the registered (active and parked) snapshots.
func (ms *MailServer) RunCleanupSnapshots() []RunCleanupStatus {
	ms.runCleanups.mu.Lock()
	snapshots := make([]*runCleanupSnapshot, 0, len(ms.runCleanups.byID))
	for _, snapshot := range ms.runCleanups.byID {
		snapshots = append(snapshots, snapshot)
	}
	ms.runCleanups.mu.Unlock()
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].id < snapshots[j].id })
	statuses := make([]RunCleanupStatus, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshot.mu.Lock()
		statuses = append(statuses, RunCleanupStatus{
			RunID:      snapshot.run,
			SnapshotID: snapshot.id,
			Total:      len(snapshot.file.IDs),
			NextIndex:  snapshot.file.NextIndex,
			Remaining:  len(snapshot.file.IDs) - snapshot.file.NextIndex,
		})
		snapshot.mu.Unlock()
	}
	return statuses
}
