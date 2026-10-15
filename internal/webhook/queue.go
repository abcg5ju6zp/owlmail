package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisPrefix = "owlmail:webhooks"
	redisConsumerGroup = "owlmail"
	redisClaimIdle     = 30 * time.Second
	redisReadBlock     = time.Second
	// redisLeaseTTL bounds how long a claimed job stays fenced after its last
	// heartbeat. It must stay above redisClaimIdle so a renewing worker cannot
	// have its fence expire before another consumer is allowed to reclaim.
	redisLeaseTTL = redisClaimIdle + redisClaimIdle/2
	// redisStateRetention bounds marker/state growth for finalized handoffs.
	// It only needs to outlive local outbox reconciliation and restarts.
	redisStateRetention = 7 * 24 * time.Hour

	redisHandoffDone = "done"
)

// ErrLeaseNotOwned reports that a worker can no longer fence writes for a
// claimed job: another generation reclaimed the lease.
var ErrLeaseNotOwned = errors.New("webhook job lease is owned by another consumer")

// Claim scripts atomically pair a consumer-group claim with a fencing token.
// The token is a random value per claim generation; only the holder of the
// current token may record results or finalize the job.

const redisClaimReclaimScript = `
local response = redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[6], '0-0', 'COUNT', 1)
local claimed = response[1]
if not claimed or #claimed == 0 then
  return nil
end
local entry = claimed[1]
redis.call('SET', ARGV[5] .. entry[1], ARGV[3], 'EX', ARGV[4])
return {entry[1], entry[2]}
`

const redisClaimNewScript = `
local response = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', 1, 'STREAMS', KEYS[1], '>')
if not response or #response == 0 or #response[1][2] == 0 then
  return nil
end
local entry = response[1][2][1]
redis.call('SET', ARGV[5] .. entry[1], ARGV[3], 'EX', ARGV[4])
return {entry[1], entry[2]}
`

const redisRenewLeaseScript = `
local leaseKey = ARGV[5] .. ARGV[3]
if redis.call('GET', leaseKey) ~= ARGV[4] then
  return -1
end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if not pending or #pending == 0 then
  return 0
end
if pending[1][2] ~= ARGV[2] then
  return -1
end
redis.call('EXPIRE', leaseKey, ARGV[6])
return 1
`

const redisRecordTargetScript = `
local leaseKey = ARGV[5] .. ARGV[3]
if redis.call('GET', leaseKey) ~= ARGV[4] then
  return -1
end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if not pending or #pending == 0 then
  return 0
end
if pending[1][2] ~= ARGV[2] then
  return -1
end
redis.call('HSET', KEYS[2], ARGV[6], ARGV[7])
redis.call('EXPIRE', KEYS[2], ARGV[8])
return 1
`

const redisFinalizeScript = `
local leaseKey = ARGV[5] .. ARGV[3]
if redis.call('GET', leaseKey) ~= ARGV[4] then
  return -1
end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if not pending or #pending == 0 then
  return 0
end
if pending[1][2] ~= ARGV[2] then
  return -1
end
local expected = cjson.decode(ARGV[7])
local existing = redis.call('HKEYS', KEYS[2])
local wanted = {}
for _, name in ipairs(expected) do
  wanted[name] = true
end
for _, name in ipairs(existing) do
  if not wanted[name] then
    redis.call('HDEL', KEYS[2], name)
  end
end
local allTerminal = true
for _, name in ipairs(expected) do
  local raw = redis.call('HGET', KEYS[2], name)
  if not raw then
    allTerminal = false
    break
  end
  local state = cjson.decode(raw)
  if state.s ~= 'succeeded' and state.s ~= 'failed' then
    allTerminal = false
    break
  end
end
if not allTerminal then
  -- Release the lease: the entry stays in the PEL and a later claim retries
  -- the remaining targets, skipping every terminal target.
  redis.call('DEL', leaseKey)
  return 2
end
if ARGV[8] ~= '' then
  redis.call('XADD', KEYS[3], '*', 'record', ARGV[8])
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
redis.call('XDEL', KEYS[1], ARGV[3])
redis.call('HSET', ARGV[10], 's', 'done', 't', ARGV[9])
redis.call('EXPIRE', ARGV[10], ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[6])
redis.call('DEL', leaseKey)
return 1
`

const redisEnqueueScript = `
local current = redis.call('HGET', KEYS[2], 's')
if not current then
  local id = redis.call('XADD', KEYS[1], '*', 'job', ARGV[1])
  redis.call('HSET', KEYS[2], 's', id, 'j', ARGV[2])
  redis.call('EXPIRE', KEYS[2], ARGV[3])
  return {id, 0}
end
return {current, 1}
`

const redisReopenScript = `
for index = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[2], ARGV[index], ARGV[index + 1])
end
redis.call('EXPIRE', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
return 1
`

// queueReceipt couples a claimed stream entry with its claim-generation
// fencing token.
type queueReceipt struct {
	id    string
	token string
	job   deliveryJob
}

type leaseDisposition int

const (
	leaseHeld leaseDisposition = iota
	leaseFinalized
	leaseLost
)

type finalizeDisposition int

const (
	finalizeCompleted finalizeDisposition = iota
	finalizeRetained
	finalizeAlreadyDone
	finalizeLeaseLost
)

// terminalHandoff is a job whose per-target stages already reached a terminal
// marker, used to backfill the queryable history after a restart.
type terminalHandoff struct {
	jobID       string
	emailID     string
	completedAt time.Time
	states      map[string]targetState
}

type deliveryQueue interface {
	// Enqueue is idempotent for a job ID: an uncertain response is reconciled
	// against the durable handoff marker instead of appending a duplicate.
	Enqueue(context.Context, deliveryJob) error
	Claim(context.Context) (*queueReceipt, error)
	Renew(context.Context, *queueReceipt) error
	RecordResult(context.Context, *queueReceipt, string, targetState) (leaseDisposition, error)
	Finalize(context.Context, *queueReceipt, []string) (finalizeDisposition, error)
	TargetStates(context.Context, string) (map[string]targetState, error)
	ReopenRetry(context.Context, deliveryJob, []string) error
	RecoverTerminal(context.Context) ([]terminalHandoff, error)
	Close() error
}

type redisDeliveryQueue struct {
	client        *redis.Client
	stream        string
	deadLetter    string
	consumer      string
	handoffPrefix string
	statePrefix   string
	leasePrefix   string
	retentionSecs int64
	leaseTTLsecs  int64
	claimIdleMS   int64
}

type deadLetterRecord struct {
	Job      deliveryJob `json:"job"`
	FailedAt time.Time   `json:"failedAt"`
	Failures []failure   `json:"failures"`
}

type failure struct {
	Target     string `json:"target"`
	StatusCode int    `json:"statusCode,omitempty"`
	Attempts   int    `json:"attempts"`
	Error      string `json:"error"`
}

func newRedisDeliveryQueue(ctx context.Context, rawURL, prefix, consumer string) (*redisDeliveryQueue, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse webhook Redis URL: %w", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to webhook Redis: %w", err)
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultRedisPrefix
	}
	queue := &redisDeliveryQueue{
		client:        client,
		stream:        prefix + ":events",
		deadLetter:    prefix + ":dead-letter",
		consumer:      consumer,
		handoffPrefix: prefix + ":handoff:",
		statePrefix:   prefix + ":deliveries:",
		leasePrefix:   prefix + ":lease:",
		retentionSecs: int64(redisStateRetention / time.Second),
		leaseTTLsecs:  int64(redisLeaseTTL / time.Second),
		claimIdleMS:   redisClaimIdle.Milliseconds(),
	}
	if err := client.XGroupCreateMkStream(ctx, queue.stream, redisConsumerGroup, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		_ = client.Close()
		return nil, fmt.Errorf("create webhook Redis consumer group: %w", err)
	}
	return queue, nil
}

func (queue *redisDeliveryQueue) markerKey(jobID string) string {
	return queue.handoffPrefix + jobID
}

func (queue *redisDeliveryQueue) stateKey(jobID string) string {
	return queue.statePrefix + jobID
}

// Enqueue appends the job exactly once per handoff identity. When the handoff
// marker already exists (a prior response may have been lost), the original
// stream entry is verified by job identity instead of appending again.
func (queue *redisDeliveryQueue) Enqueue(ctx context.Context, job deliveryJob) error {
	encoded, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode webhook job: %w", err)
	}
	emailID := ""
	if job.Email != nil {
		emailID = job.Email.ID
	}
	reply, err := queue.client.Eval(ctx, redisEnqueueScript,
		[]string{queue.stream, queue.markerKey(job.ID)},
		string(encoded), emailID, queue.retentionSecs).Result()
	if err != nil {
		return fmt.Errorf("enqueue webhook job: %w", err)
	}
	values, ok := reply.([]any)
	if !ok || len(values) != 2 {
		return fmt.Errorf("enqueue webhook job: unexpected reply %T", reply)
	}
	marker, _ := values[0].(string)
	existed, _ := values[1].(int64)
	if existed == 0 || marker == redisHandoffDone {
		return nil
	}
	return queue.verifyHandoffIdentity(ctx, marker, job.ID)
}

// verifyHandoffIdentity confirms a pre-existing marker points at the original
// job. A missing stream entry means the handoff already completed; a job ID
// mismatch is durable-state corruption.
func (queue *redisDeliveryQueue) verifyHandoffIdentity(ctx context.Context, streamID, jobID string) error {
	messages, err := queue.client.XRange(ctx, queue.stream, streamID, streamID).Result()
	if errors.Is(err, redis.Nil) || len(messages) == 0 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify webhook handoff %s: %w", streamID, err)
	}
	receipt, err := decodeRedisMessage(messages[0])
	if err != nil {
		return err
	}
	if receipt.job.ID != jobID {
		return fmt.Errorf("webhook handoff %s belongs to job %s, want %s", streamID, receipt.job.ID, jobID)
	}
	return nil
}

func (queue *redisDeliveryQueue) Claim(ctx context.Context) (*queueReceipt, error) {
	token := uuid.NewString()
	commonArgs := []any{
		redisConsumerGroup, queue.consumer, token, queue.leaseTTLsecs, queue.leasePrefix, queue.claimIdleMS,
	}
	if receipt, err := queue.claimWithScript(ctx, redisClaimReclaimScript, commonArgs); err != nil {
		return nil, fmt.Errorf("reclaim webhook job: %w", err)
	} else if receipt != nil {
		receipt.token = token
		return receipt, nil
	}
	if receipt, err := queue.claimWithScript(ctx, redisClaimNewScript, commonArgs[:5]); err != nil {
		return nil, fmt.Errorf("claim webhook job: %w", err)
	} else if receipt != nil {
		receipt.token = token
		return receipt, nil
	}
	// Non-blocking Lua claims cannot use XREADGROUP BLOCK; park in Go so idle
	// workers do not spin while waiting for a new entry or the reclaim window.
	timer := time.NewTimer(redisReadBlock)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, nil
	}
}

func (queue *redisDeliveryQueue) claimWithScript(ctx context.Context, script string, args []any) (*queueReceipt, error) {
	reply, err := queue.client.Eval(ctx, script, []string{queue.stream}, args...).Result()
	if errors.Is(err, redis.Nil) || reply == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	message, err := luaStreamEntry(reply)
	if err != nil {
		return nil, err
	}
	return decodeRedisMessage(message)
}

// luaStreamEntry converts a Lua {id, {field, value, ...}} reply into the
// XMessage shape decodeRedisMessage expects.
func luaStreamEntry(reply any) (redis.XMessage, error) {
	entry, ok := reply.([]any)
	if !ok || len(entry) != 2 {
		return redis.XMessage{}, fmt.Errorf("unexpected claim reply shape %T", reply)
	}
	id, ok := entry[0].(string)
	if !ok {
		return redis.XMessage{}, fmt.Errorf("unexpected claim entry id type %T", entry[0])
	}
	fields, ok := entry[1].([]any)
	if !ok {
		return redis.XMessage{}, fmt.Errorf("unexpected claim fields type %T", entry[1])
	}
	values := make(map[string]any, len(fields)/2)
	for index := 0; index+1 < len(fields); index += 2 {
		name, nameOK := fields[index].(string)
		if !nameOK {
			return redis.XMessage{}, fmt.Errorf("unexpected claim field type %T", fields[index])
		}
		values[name] = fields[index+1]
	}
	return redis.XMessage{ID: id, Values: values}, nil
}

func decodeRedisMessage(message redis.XMessage) (*queueReceipt, error) {
	raw, exists := message.Values["job"]
	if !exists {
		return nil, fmt.Errorf("webhook Redis entry %s has no job field", message.ID)
	}
	var encoded []byte
	switch value := raw.(type) {
	case string:
		encoded = []byte(value)
	case []byte:
		encoded = value
	default:
		return nil, fmt.Errorf("webhook Redis entry %s has invalid job field", message.ID)
	}
	var job deliveryJob
	if err := json.Unmarshal(encoded, &job); err != nil {
		return nil, fmt.Errorf("decode webhook Redis entry %s: %w", message.ID, err)
	}
	return &queueReceipt{id: message.ID, job: job}, nil
}

// Renew resets the fencing token TTL while the HTTP delivery is active.
func (queue *redisDeliveryQueue) Renew(ctx context.Context, receipt *queueReceipt) error {
	if receipt == nil {
		return nil
	}
	result, err := queue.client.Eval(ctx, redisRenewLeaseScript, []string{queue.stream},
		redisConsumerGroup, queue.consumer, receipt.id, receipt.token,
		queue.leasePrefix, queue.leaseTTLsecs).Int64()
	if err != nil {
		return fmt.Errorf("renew webhook job lease: %w", err)
	}
	switch result {
	case 1:
		return nil
	case 0:
		return nil
	default:
		return ErrLeaseNotOwned
	}
}

// RecordResult durably stores one target's stage, but only for the current
// lease generation. A worker that lost its lease cannot submit late results.
func (queue *redisDeliveryQueue) RecordResult(ctx context.Context, receipt *queueReceipt, target string, state targetState) (leaseDisposition, error) {
	if receipt == nil {
		return leaseFinalized, nil
	}
	encoded, err := encodeTargetState(state)
	if err != nil {
		return leaseLost, fmt.Errorf("encode webhook target state: %w", err)
	}
	result, err := queue.client.Eval(ctx, redisRecordTargetScript,
		[]string{queue.stream, queue.stateKey(receipt.job.ID)},
		redisConsumerGroup, queue.consumer, receipt.id, receipt.token,
		queue.leasePrefix, target, encoded, queue.retentionSecs).Int64()
	if err != nil {
		return leaseLost, fmt.Errorf("record webhook target result: %w", err)
	}
	switch result {
	case 1:
		return leaseHeld, nil
	case 0:
		return leaseFinalized, nil
	default:
		return leaseLost, nil
	}
}

// Finalize acknowledges and removes the job only when every expected target
// reached a terminal stage. Retry-pending targets keep the entry in the PEL
// and release the current lease for the next claim. Permanent failures are
// appended to the dead-letter stream without blocking sibling targets.
func (queue *redisDeliveryQueue) Finalize(ctx context.Context, receipt *queueReceipt, expected []string) (finalizeDisposition, error) {
	if receipt == nil {
		return finalizeAlreadyDone, nil
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return finalizeRetained, fmt.Errorf("encode webhook target plan: %w", err)
	}
	states, err := queue.TargetStates(ctx, receipt.job.ID)
	if err != nil {
		return finalizeRetained, err
	}
	deadRecord := encodeDeadLetter(receipt.job, expected, states)
	result, err := queue.client.Eval(ctx, redisFinalizeScript,
		[]string{queue.stream, queue.stateKey(receipt.job.ID), queue.deadLetter},
		redisConsumerGroup, queue.consumer, receipt.id, receipt.token,
		queue.leasePrefix, queue.retentionSecs, string(expectedJSON), deadRecord,
		time.Now().UTC().Format(time.RFC3339Nano), queue.markerKey(receipt.job.ID)).Int64()
	if err != nil {
		return finalizeRetained, fmt.Errorf("finalize webhook job: %w", err)
	}
	switch result {
	case 1:
		return finalizeCompleted, nil
	case 2:
		return finalizeRetained, nil
	case 0:
		return finalizeAlreadyDone, nil
	default:
		return finalizeLeaseLost, nil
	}
}

// TargetStates returns the durable per-target stages for a handoff.
func (queue *redisDeliveryQueue) TargetStates(ctx context.Context, jobID string) (map[string]targetState, error) {
	values, err := queue.client.HGetAll(ctx, queue.stateKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("read webhook target states for %s: %w", jobID, err)
	}
	states := make(map[string]targetState, len(values))
	for target, encoded := range values {
		state, decodeErr := decodeTargetState(encoded)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode webhook target state for %s/%s: %w", jobID, target, decodeErr)
		}
		states[target] = state
	}
	return states, nil
}

// ReopenRetry resets selected permanently failed targets to pending and
// clears the handoff marker so an explicit replay enqueues a fresh stream
// entry under the same stable identities.
func (queue *redisDeliveryQueue) ReopenRetry(ctx context.Context, job deliveryJob, targets []string) error {
	now := time.Now().UTC()
	args := make([]any, 0, 1+2*len(targets))
	args = append(args, queue.retentionSecs)
	for _, target := range targets {
		state, err := encodeTargetState(pendingTargetState(job.ID, target, now))
		if err != nil {
			return fmt.Errorf("encode replayed webhook target %s: %w", target, err)
		}
		args = append(args, target, state)
	}
	if err := queue.client.Eval(ctx, redisReopenScript,
		[]string{queue.markerKey(job.ID), queue.stateKey(job.ID)}, args...).Err(); err != nil {
		return fmt.Errorf("reopen webhook retry: %w", err)
	}
	return nil
}

// RecoverTerminal lists handoffs whose marker says "done" (or whose stream
// entry vanished while the marker lags), so the service can rebuild queryable
// terminal records after a restart.
func (queue *redisDeliveryQueue) RecoverTerminal(ctx context.Context) ([]terminalHandoff, error) {
	var handoffs []terminalHandoff
	var cursor uint64
	for {
		keys, next, err := queue.client.Scan(ctx, cursor, queue.handoffPrefix+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan webhook handoffs: %w", err)
		}
		for _, key := range keys {
			marker, err := queue.client.HGetAll(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("read webhook handoff marker %s: %w", key, err)
			}
			jobID := strings.TrimPrefix(key, queue.handoffPrefix)
			if !queue.markerTerminal(ctx, marker["s"]) {
				continue
			}
			states, err := queue.TargetStates(ctx, jobID)
			if err != nil {
				return nil, err
			}
			if len(states) == 0 {
				continue
			}
			handoff := terminalHandoff{jobID: jobID, emailID: marker["j"], states: states}
			if completedAt, parseErr := time.Parse(time.RFC3339Nano, marker["t"]); parseErr == nil {
				handoff.completedAt = completedAt.UTC()
			}
			handoffs = append(handoffs, handoff)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return handoffs, nil
}

func (queue *redisDeliveryQueue) markerTerminal(ctx context.Context, marker string) bool {
	if marker == redisHandoffDone {
		return true
	}
	if marker == "" {
		return false
	}
	messages, err := queue.client.XRange(ctx, queue.stream, marker, marker).Result()
	if err != nil {
		return false
	}
	return len(messages) == 0
}

func encodeDeadLetter(job deliveryJob, expected []string, states map[string]targetState) string {
	record := deadLetterRecord{Job: job, FailedAt: time.Now().UTC()}
	for _, target := range expected {
		state, ok := states[target]
		if !ok || state.Stage != targetStageFailed {
			continue
		}
		record.Failures = append(record.Failures, failure{
			Target: target, StatusCode: state.StatusCode,
			Attempts: state.Attempts, Error: state.Error,
		})
	}
	if len(record.Failures) == 0 {
		return ""
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func (queue *redisDeliveryQueue) Close() error {
	return queue.client.Close()
}
