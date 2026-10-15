package mailserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	deletionFencePrefix  = storageTempPrefix + "delete-"
	deletionFenceSuffix  = ".fence"
	deletionFenceState   = "delete"
	deletionFenceVersion = 1
)

const (
	deletionLayerRemoteAttachments = "remote_attachments"
	deletionLayerLocalAttachments  = "local_attachments"
	deletionLayerEML               = "eml"
	deletionLayerMetadata          = "metadata"
)

// deletionLayers is the only authoritative deletion order. A crashed or
// failed delete resumes at the first layer absent from the fence checkpoint,
// so the order must never be reshuffled: later layers assume earlier layers
// have already been made durable.
var deletionLayers = []string{
	deletionLayerRemoteAttachments,
	deletionLayerLocalAttachments,
	deletionLayerEML,
	deletionLayerMetadata,
}

// deletionFenceCheckpoint is the versioned JSON payload of a deletion fence.
// Completed lists layer names that have already succeeded durably.
type deletionFenceCheckpoint struct {
	Version   int       `json:"version"`
	ID        string    `json:"id"`
	UpdatedAt time.Time `json:"updated_at"`
	Completed []string  `json:"completed"`
}

func deletionFencePath(mailDir, id string) string {
	return filepath.Join(mailDir, deletionFencePrefix+id+deletionFenceSuffix)
}

func deletionFenceID(name string) (string, bool) {
	if !strings.HasPrefix(name, deletionFencePrefix) || !strings.HasSuffix(name, deletionFenceSuffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, deletionFencePrefix), deletionFenceSuffix)
	return id, validateEmailID(id) == nil
}

// ensureDeletionFence records deletion intent before any local or remote
// artifact is removed. An existing fence, in either the legacy plain-text or
// the versioned checkpoint format, makes retries idempotent.
func (ms *MailServer) ensureDeletionFence(id string) error {
	if err := validateEmailID(id); err != nil {
		return err
	}
	fencePath := deletionFencePath(ms.mailDir, id)
	fence, err := os.OpenFile(fencePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create email deletion fence: %w", err)
	}
	committed := false
	defer func() {
		_ = fence.Close()
		if !committed {
			_ = os.Remove(fencePath)
			_ = syncDirectory(ms.mailDir)
		}
	}()
	encoded, err := json.Marshal(deletionFenceCheckpoint{
		Version:   deletionFenceVersion,
		ID:        id,
		UpdatedAt: time.Now().UTC(),
		Completed: []string{},
	})
	if err != nil {
		return fmt.Errorf("encode email deletion fence: %w", err)
	}
	if _, err := fence.Write(encoded); err != nil {
		return fmt.Errorf("write email deletion fence: %w", err)
	}
	if err := fence.Sync(); err != nil {
		return fmt.Errorf("sync email deletion fence: %w", err)
	}
	if err := fence.Close(); err != nil {
		return fmt.Errorf("close email deletion fence: %w", err)
	}
	if err := syncDirectory(ms.mailDir); err != nil {
		return fmt.Errorf("sync email deletion fence directory: %w", err)
	}
	committed = true
	return nil
}

// readDeletionFenceCheckpoint loads the durable deletion progress. Legacy
// plain-text fences ("delete\n") and unparseable payloads conservatively mean
// "no layer completed yet", so an interrupted delete from an older build
// replays every idempotent layer. Semantically invalid JSON (wrong version or
// id) is an error rather than a silent reset, because it signals fence
// corruption that could otherwise bind progress to the wrong message.
func (ms *MailServer) readDeletionFenceCheckpoint(id string) (deletionFenceCheckpoint, error) {
	empty := deletionFenceCheckpoint{Version: deletionFenceVersion, ID: id, Completed: []string{}}
	encoded, err := os.ReadFile(deletionFencePath(ms.mailDir, id))
	if err != nil {
		return empty, fmt.Errorf("read email deletion fence: %w", err)
	}
	payload := strings.TrimSpace(string(encoded))
	if payload == "" || payload == deletionFenceState {
		return empty, nil
	}
	var checkpoint deletionFenceCheckpoint
	if err := json.Unmarshal(encoded, &checkpoint); err != nil {
		return empty, nil
	}
	if checkpoint.Version != deletionFenceVersion || checkpoint.ID != id {
		return empty, fmt.Errorf("invalid email deletion fence for %s (version=%d, id=%q)", id, checkpoint.Version, checkpoint.ID)
	}
	completed := make([]string, 0, len(checkpoint.Completed))
	known := make(map[string]struct{}, len(deletionLayers))
	for _, layer := range deletionLayers {
		known[layer] = struct{}{}
	}
	for _, layer := range checkpoint.Completed {
		if _, ok := known[layer]; !ok {
			return empty, fmt.Errorf("invalid email deletion fence for %s: unknown layer %q", id, layer)
		}
		completed = append(completed, layer)
	}
	checkpoint.Completed = completed
	return checkpoint, nil
}

// writeDeletionFenceCheckpoint atomically replaces the fence payload with the
// ordered set of completed layers. The temporary file lives next to the fence
// so rename is atomic on the same filesystem.
func (ms *MailServer) writeDeletionFenceCheckpoint(id string, completed []string) error {
	checkpoint := deletionFenceCheckpoint{
		Version:   deletionFenceVersion,
		ID:        id,
		UpdatedAt: time.Now().UTC(),
		Completed: append([]string{}, completed...),
	}
	encoded, err := json.Marshal(&checkpoint)
	if err != nil {
		return fmt.Errorf("encode email deletion checkpoint: %w", err)
	}
	tmp, err := os.CreateTemp(ms.mailDir, storageTempPrefix+"fence-*.tmp")
	if err != nil {
		return fmt.Errorf("create email deletion checkpoint: %w", err)
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
		return fmt.Errorf("chmod email deletion checkpoint: %w", err)
	}
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write email deletion checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync email deletion checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close email deletion checkpoint: %w", err)
	}
	if ms.beforeDeletionCheckpointRename != nil {
		if err := ms.beforeDeletionCheckpointRename(id); err != nil {
			return fmt.Errorf("delete checkpoint commit rejected: %w", err)
		}
	}
	if err := os.Rename(tmpPath, deletionFencePath(ms.mailDir, id)); err != nil {
		return fmt.Errorf("commit email deletion checkpoint: %w", err)
	}
	committed = true
	if err := syncDirectory(ms.mailDir); err != nil {
		return fmt.Errorf("sync email deletion checkpoint directory: %w", err)
	}
	return nil
}

// cleanupDeletionFencedEmail completes an idempotent delete. Layers run in the
// fixed deletionLayers order and each successful layer is durably checkpointed
// before the next one starts, so a crash, storage outage, or in-process retry
// resumes at the first unfinished layer without replaying completed work. The
// fence is removed only after every artifact deletion has been synced.
func (ms *MailServer) cleanupDeletionFencedEmail(id string) error {
	checkpoint, err := ms.readDeletionFenceCheckpoint(id)
	if err != nil {
		return err
	}
	completed := make(map[string]struct{}, len(checkpoint.Completed))
	for _, layer := range checkpoint.Completed {
		completed[layer] = struct{}{}
	}
	ordered := make([]string, 0, len(deletionLayers))
	for _, layer := range deletionLayers {
		if _, done := completed[layer]; done {
			ordered = append(ordered, layer)
			continue
		}
		if ms.beforeDeletionLayer != nil {
			if err := ms.beforeDeletionLayer(id, layer); err != nil {
				return fmt.Errorf("delete %s: %w", layer, err)
			}
		}
		if err := ms.runDeletionLayer(id, layer); err != nil {
			return fmt.Errorf("delete %s: %w", layer, err)
		}
		ordered = append(ordered, layer)
		if err := ms.writeDeletionFenceCheckpoint(id, ordered); err != nil {
			return err
		}
	}

	// The per-layer checkpoints sync the metadata directory as part of the
	// metadata layer, but removal of the EML and attachment directory entries
	// needs one final directory sync before the fence can disappear.
	if err := syncDirectory(ms.mailDir); err != nil {
		return fmt.Errorf("sync mail directory: %w", err)
	}

	fencePath := deletionFencePath(ms.mailDir, id)
	if err := os.Remove(fencePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove email deletion fence: %w", err)
	}
	if err := syncDirectory(ms.mailDir); err != nil {
		return fmt.Errorf("sync deleted email fence: %w", err)
	}
	return nil
}

// runDeletionLayer executes exactly one idempotent deletion layer. Every
// operation ignores already-absent artifacts so layer replay after a crash is
// safe.
func (ms *MailServer) runDeletionLayer(id, layer string) error {
	switch layer {
	case deletionLayerRemoteAttachments:
		return ms.deleteRemoteAttachments(id)
	case deletionLayerLocalAttachments:
		if err := os.RemoveAll(filepath.Join(ms.mailDir, id)); err != nil {
			return err
		}
		return nil
	case deletionLayerEML:
		if err := os.Remove(filepath.Join(ms.mailDir, id+".eml")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case deletionLayerMetadata:
		if err := ms.deleteEmailMetadata(id); err != nil {
			return err
		}
		return syncDirectoryIfExists(filepath.Join(ms.mailDir, metadataDirectoryName))
	default:
		return fmt.Errorf("unknown deletion layer %q", layer)
	}
}

func syncDirectoryIfExists(path string) error {
	err := syncDirectory(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (ms *MailServer) deletionCandidates() ([]string, error) {
	seen := make(map[string]struct{})
	ids := make([]string, 0)
	add := func(id string) {
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	ms.storeMutex.RLock()
	for _, id := range ms.storeOrder {
		add(id)
	}
	for id := range ms.storeByID {
		add(id)
	}
	ms.storeMutex.RUnlock()

	entries, err := os.ReadDir(ms.mailDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		entryPath := filepath.Join(ms.mailDir, entry.Name())
		if ms.mailboxIndex != nil && ms.mailboxIndex.OwnsPath(entryPath) {
			continue
		}
		if id, ok := deletionFenceID(entry.Name()); ok {
			add(id)
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".eml") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".eml")
		if validateEmailID(id) == nil {
			add(id)
		}
	}
	return ids, nil
}

// deleteEmailFromRuntimeState keeps the derived index and authoritative
// in-memory view synchronized with read-state upserts, which use the same
// store lock. The deleted message is retained in the delivery graveyard so
// waiters registered before its sequence can still observe it.
func (ms *MailServer) deleteEmailFromRuntimeState(id string) {
	ms.deleteEmailRuntimeState(id, true)
}

// deleteEmailRuntimeState performs the runtime removal. bury controls whether
// the message enters the delivery graveyard; whole-mailbox clears pass false
// because they reset the graveyard wholesale.
func (ms *MailServer) deleteEmailRuntimeState(id string, bury bool) {
	ms.storeMutex.Lock()
	defer ms.storeMutex.Unlock()
	ms.deleteMailboxIndexLocked(id)
	email := ms.storeByID[id]
	sequence := ms.deliverySequences[id]
	delete(ms.storeByID, id)
	ms.removeDeliveryLocked(id)
	delete(ms.receivedAtByID, id)
	delete(ms.storePositionByID, id)
	for i, storedID := range ms.storeOrder {
		if storedID == id {
			ms.storeOrder = append(ms.storeOrder[:i], ms.storeOrder[i+1:]...)
			break
		}
	}
	if bury {
		ms.buryDeletedEmailLocked(sequence, email)
		ms.notifyDeliveryChangedLocked()
	}
}
