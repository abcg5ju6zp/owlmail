package mailserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidCursor means the cursor is malformed, from another process, or ahead of this mailbox.
var ErrInvalidCursor = errors.New("invalid or expired delivery cursor")

// ErrCursorExpired means the cursor is well formed but its replay window has
// been reclaimed (deletion or read-only refresh advanced past it). The caller
// must resynchronize with a fresh cursor instead of continuing to wait.
var ErrCursorExpired = errors.New("delivery cursor expired")

// ErrWaitCapacity means all bounded delivery wait slots are occupied.
var ErrWaitCapacity = errors.New("delivery wait capacity reached")

// ErrMailboxClosed means the mailbox stopped while a caller was waiting.
var ErrMailboxClosed = errors.New("mailbox closed")

const (
	// deliveryGraveyardRetention bounds how long deleted messages remain
	// replayable for delivery waiters that registered before the deletion.
	deliveryGraveyardRetention = 150 * time.Second
	// deliveryGraveyardCapacity is the hard upper bound on retained deleted
	// messages. Overflow reclaims the oldest entries and expires cursors that
	// still need them rather than growing without limit.
	deliveryGraveyardCapacity = 10000
)

// graveyardEntry keeps a detached deleted message replayable for waiters that
// registered at a cursor before its sequence.
type graveyardEntry struct {
	sequence  uint64
	email     *Email
	deletedAt time.Time
}

// DeliveryFilter groups test messages without granting a separate security boundary.
// Recipient matches the complete SMTP envelope address, case-sensitively.
type DeliveryFilter struct {
	Recipient      string `json:"recipient,omitempty"`
	RunID          string `json:"run_id,omitempty"`
	Subject        string `json:"subject,omitempty"`
	Text           string `json:"text,omitempty"`
	subjectPattern *regexp.Regexp
	textPattern    *regexp.Regexp
}

// ValidateRunID accepts a bounded, portable test-run identifier. Empty means ungrouped.
func ValidateRunID(id string) error {
	if len(id) > 128 {
		return fmt.Errorf("run_id cannot exceed 128 bytes")
	}
	for _, c := range id {
		valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:-", c)
		if !valid {
			return fmt.Errorf("run_id must contain only ASCII letters, digits, dot, underscore, colon, or hyphen")
		}
	}
	return nil
}

// Validate checks filter bounds before waiting or querying.
func (f DeliveryFilter) Validate() error {
	if err := ValidateRunID(f.RunID); err != nil {
		return err
	}
	for _, s := range []string{f.Recipient, f.Subject, f.Text} {
		if len(s) > 1024 {
			return fmt.Errorf("delivery filters cannot exceed 1024 bytes")
		}
	}
	return nil
}

// Matches applies exact run/envelope selectors and case-insensitive content substrings.
func (f DeliveryFilter) Matches(e *Email) bool {
	if e == nil || f.RunID != "" && e.RunID != f.RunID {
		return false
	}
	if f.Recipient != "" {
		found := false
		if e.Envelope != nil {
			for _, r := range e.Envelope.To {
				if r == f.Recipient {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	return foldedContains(e.Subject, f.Subject, f.subjectPattern) && (f.Text == "" || foldedContains(e.Text, f.Text, f.textPattern) || foldedContains(e.HTML, f.Text, f.textPattern))
}

func foldedContains(value, literal string, pattern *regexp.Regexp) bool {
	if literal == "" {
		return true
	}
	if pattern == nil {
		var err error
		pattern, err = regexp.Compile("(?i)" + regexp.QuoteMeta(literal))
		if err != nil {
			return false
		}
	}
	return pattern.MatchString(value)
}

type deliveryEntry struct {
	id       string
	sequence uint64
}

func (ms *MailServer) removeDeliveryLocked(id string) {
	delete(ms.deliverySequences, id)
	for i, entry := range ms.deliveryOrder {
		if entry.id == id {
			ms.deliveryOrder = append(ms.deliveryOrder[:i], ms.deliveryOrder[i+1:]...)
			break
		}
	}
}

func (ms *MailServer) initDeliveriesLocked() {
	if ms.deliveryEpoch == "" {
		ms.deliveryEpoch = rand.Text()
		ms.deliverySequences = make(map[string]uint64)
		ms.deliveryChanged = make(chan struct{})
		ms.deliveryWaiterCursors = make(map[uint64]uint64)
	}
}

// now returns the injectable wall clock used for graveyard retention.
func (ms *MailServer) now() time.Time {
	if ms.nowFunc != nil {
		return ms.nowFunc()
	}
	return time.Now()
}

// notifyDeliveryChangedLocked wakes every blocked delivery waiter. The
// close/recreate channel swap is the established broadcast primitive.
func (ms *MailServer) notifyDeliveryChangedLocked() {
	ms.initDeliveriesLocked()
	if !ms.deliveryClosed {
		close(ms.deliveryChanged)
		ms.deliveryChanged = make(chan struct{})
	}
}

func (ms *MailServer) recordDeliveryLocked(id string) {
	ms.initDeliveriesLocked()
	if _, exists := ms.deliverySequences[id]; exists {
		return
	}
	ms.deliverySequence++
	ms.deliverySequences[id] = ms.deliverySequence
	ms.deliveryOrder = append(ms.deliveryOrder, deliveryEntry{id: id, sequence: ms.deliverySequence})
	ms.reapDeliveryGraveyardLocked()
	ms.notifyDeliveryChangedLocked()
}

// buryDeletedEmailLocked moves a just-deleted message into the graveyard so a
// waiter registered before its sequence can still observe a matching message.
// It must run in the same storeMutex critical section that removes the live
// delivery record.
func (ms *MailServer) buryDeletedEmailLocked(sequence uint64, email *Email) {
	ms.initDeliveriesLocked()
	if sequence == 0 {
		return
	}
	entry := graveyardEntry{sequence: sequence, deletedAt: ms.now().UTC()}
	if email != nil {
		entry.email = cloneEmail(email)
	}
	// The live record is removed by the caller. Sequences stay monotonic, so
	// the insertion point keeps the graveyard ascending even after reclamation.
	index := sort.Search(len(ms.deliveryGraveyard), func(i int) bool { return ms.deliveryGraveyard[i].sequence >= sequence })
	ms.deliveryGraveyard = append(ms.deliveryGraveyard, graveyardEntry{})
	copy(ms.deliveryGraveyard[index+1:], ms.deliveryGraveyard[index:])
	ms.deliveryGraveyard[index] = entry
	ms.reapDeliveryGraveyardLocked()
}

// invalidateDeliveriesBefore advances the graveyard floor to floor, expiring
// every cursor below it. It is used by whole-mailbox resets for which no
// per-message graveyard record exists.
func (ms *MailServer) invalidateDeliveriesBeforeLocked(floor uint64) {
	ms.initDeliveriesLocked()
	if floor > ms.deliveryGraveyardFloor {
		ms.deliveryGraveyardFloor = floor
	}
}

// reapDeliveryGraveyardLocked reclaims expired graveyard entries lazily at
// critical-section exits. Age-based reclamation additionally requires every
// registered waiter to start at or after the entry sequence. Capacity-driven
// reclamation may reclaim entries waiters still need; the floor then expires
// those waiters deterministically on their next scan.
func (ms *MailServer) reapDeliveryGraveyardLocked() {
	if len(ms.deliveryGraveyard) == 0 {
		return
	}
	cutoff := ms.now().UTC().Add(-deliveryGraveyardRetention)
	minWaiter := ^uint64(0)
	for _, start := range ms.deliveryWaiterCursors {
		if start < minWaiter {
			minWaiter = start
		}
	}
	prefix := 0
	for _, entry := range ms.deliveryGraveyard {
		agedOut := !entry.deletedAt.After(cutoff)
		if agedOut && (len(ms.deliveryWaiterCursors) == 0 || entry.sequence <= minWaiter) {
			prefix++
			continue
		}
		break
	}
	if overflow := len(ms.deliveryGraveyard) - deliveryGraveyardCapacity; overflow > prefix {
		prefix = overflow
	}
	if prefix == 0 {
		return
	}
	if last := ms.deliveryGraveyard[prefix-1].sequence; last > ms.deliveryGraveyardFloor {
		ms.deliveryGraveyardFloor = last
	}
	ms.deliveryGraveyard = append([]graveyardEntry(nil), ms.deliveryGraveyard[prefix:]...)
}
func (ms *MailServer) cursorLocked() string {
	return ms.deliveryEpoch + ":" + strconv.FormatUint(ms.deliverySequence, 10)
}

// DeliveryCursor identifies the last committed message in this process. Restarts invalidate it.
func (ms *MailServer) DeliveryCursor() string {
	ms.storeMutex.Lock()
	defer ms.storeMutex.Unlock()
	ms.initDeliveriesLocked()
	return ms.cursorLocked()
}
func (ms *MailServer) parseCursorLocked(cursor string) (uint64, error) {
	epoch, seq, ok := strings.Cut(cursor, ":")
	n, err := strconv.ParseUint(seq, 10, 64)
	if !ok || err != nil || epoch != ms.deliveryEpoch || n > ms.deliverySequence || seq != strconv.FormatUint(n, 10) {
		return 0, ErrInvalidCursor
	}
	return n, nil
}

// DeliveryResult contains detached committed messages, including a normal timeout outcome.
type DeliveryResult struct {
	Emails             []*Email `json:"emails"`
	Cursor             string   `json:"cursor"`
	TimedOut           bool     `json:"timed_out"`
	WaitedMilliseconds int64    `json:"waited_milliseconds"`
}

// WaitDeliveries replays retained messages after a cursor and atomically subscribes
// to subsequent commits. It never polls, changes read state, or installs permanent listeners.
// Count is an at-least threshold (1..100), not an exactly-once delivery guarantee.
func (ms *MailServer) WaitDeliveries(ctx context.Context, after string, filter DeliveryFilter, count int, timeout time.Duration) (DeliveryResult, error) {
	result := DeliveryResult{Emails: make([]*Email, 0)}
	if err := filter.Validate(); err != nil {
		return result, err
	}
	var compileErr error
	filter.subjectPattern, compileErr = regexp.Compile("(?i)" + regexp.QuoteMeta(filter.Subject))
	if compileErr != nil {
		return result, compileErr
	}
	filter.textPattern, compileErr = regexp.Compile("(?i)" + regexp.QuoteMeta(filter.Text))
	if compileErr != nil {
		return result, compileErr
	}
	if count < 1 || count > 100 || timeout <= 0 || timeout > 2*time.Minute {
		return result, fmt.Errorf("count must be 1..100 and timeout must be positive and at most 120 seconds")
	}
	ms.storeMutex.Lock()
	ms.initDeliveriesLocked()
	sequence, err := ms.parseCursorLocked(after)
	if err != nil {
		ms.storeMutex.Unlock()
		return result, err
	}
	if ms.deliveryGraveyardFloor > sequence {
		ms.storeMutex.Unlock()
		return result, ErrCursorExpired
	}
	if ms.deliveryClosed {
		ms.storeMutex.Unlock()
		return result, ErrMailboxClosed
	}
	if ms.deliveryWaiters >= 64 {
		ms.storeMutex.Unlock()
		return result, ErrWaitCapacity
	}
	ms.deliveryWaiters++
	waiterID := ms.deliveryWaiterNextID
	ms.deliveryWaiterNextID++
	ms.deliveryWaiterCursors[waiterID] = sequence
	// Registering a waiter can make aged graveyard entries reclaimable; a
	// capacity-driven reclaim can in turn expire this brand-new cursor.
	ms.reapDeliveryGraveyardLocked()
	if ms.deliveryGraveyardFloor > sequence {
		delete(ms.deliveryWaiterCursors, waiterID)
		ms.deliveryWaiters--
		ms.storeMutex.Unlock()
		return result, ErrCursorExpired
	}
	ms.storeMutex.Unlock()
	defer func() {
		ms.storeMutex.Lock()
		ms.deliveryWaiters--
		delete(ms.deliveryWaiterCursors, waiterID)
		ms.reapDeliveryGraveyardLocked()
		ms.storeMutex.Unlock()
	}()
	started := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		ms.storeMutex.RLock()
		if ms.deliveryClosed {
			ms.storeMutex.RUnlock()
			return result, ErrMailboxClosed
		}
		if ms.deliveryGraveyardFloor > sequence {
			ms.storeMutex.RUnlock()
			return result, ErrCursorExpired
		}
		matches, scannedSequence, satisfied := ms.scanDeliveriesLocked(sequence, &filter, count-len(result.Emails))
		sequence = scannedSequence
		result.Cursor = ms.deliveryEpoch + ":" + strconv.FormatUint(scannedSequence, 10)
		changed := ms.deliveryChanged
		if ms.beforeDeliveryWaitSelect != nil {
			ms.beforeDeliveryWaitSelect()
		}
		ms.storeMutex.RUnlock()
		result.Emails = append(result.Emails, matches...)
		result.WaitedMilliseconds = time.Since(started).Milliseconds()
		if satisfied || len(result.Emails) >= count {
			return result, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return result, ctx.Err()
		case <-timer.C:
			result.TimedOut = true
			result.WaitedMilliseconds = time.Since(started).Milliseconds()
			return result, nil
		}
	}
}

// scanDeliveriesLocked merges live deliveries with graveyard records of
// deleted messages after sequence, in ascending sequence order, so cleanup can
// never make a waiter skip a matching message it registered before. It returns
// at most remaining matches, the sequence the caller should advance to, and
// whether the requested count was satisfied in this scan.
func (ms *MailServer) scanDeliveriesLocked(sequence uint64, filter *DeliveryFilter, remaining int) ([]*Email, uint64, bool) {
	head := ms.deliverySequence
	if remaining <= 0 {
		return nil, head, true
	}
	liveIndex := sort.Search(len(ms.deliveryOrder), func(i int) bool { return ms.deliveryOrder[i].sequence > sequence })
	deadIndex := sort.Search(len(ms.deliveryGraveyard), func(i int) bool { return ms.deliveryGraveyard[i].sequence > sequence })
	matches := make([]*Email, 0)
	for liveIndex < len(ms.deliveryOrder) || deadIndex < len(ms.deliveryGraveyard) {
		var liveSeq, deadSeq uint64
		hasLive, hasDead := liveIndex < len(ms.deliveryOrder), deadIndex < len(ms.deliveryGraveyard)
		if hasLive {
			liveSeq = ms.deliveryOrder[liveIndex].sequence
		}
		if hasDead {
			deadSeq = ms.deliveryGraveyard[deadIndex].sequence
		}
		var email *Email
		var nextSequence uint64
		switch {
		case hasLive && (!hasDead || liveSeq <= deadSeq):
			entry := ms.deliveryOrder[liveIndex]
			liveIndex++
			nextSequence = entry.sequence
			email = ms.storeByID[entry.id]
		default:
			entry := ms.deliveryGraveyard[deadIndex]
			deadIndex++
			nextSequence = entry.sequence
			email = entry.email
		}
		if email != nil && filter.Matches(email) {
			matches = append(matches, cloneEmail(email))
			if len(matches) >= remaining {
				return matches, nextSequence, true
			}
		}
	}
	return matches, head, false
}
