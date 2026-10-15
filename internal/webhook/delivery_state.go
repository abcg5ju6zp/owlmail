package webhook

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Recoverable per-target delivery stages persisted alongside the Redis job:
//
//	pending -> retry <-> ... -> succeeded
//	                          \-> failed
//
// "succeeded" and "failed" are terminal. A retryable HTTP outcome keeps the
// target in "retry" so a later claim performs the next attempt; a permanent
// outcome becomes a queryable terminal state and never blocks other targets.
const (
	targetStagePending   = "pending"
	targetStageRetry     = "retry"
	targetStageSucceeded = "succeeded"
	targetStageFailed    = "failed"
)

// targetState is the durable delivery identity and stage for one
// (handoff job, target) pair. Field names stay short because the state is
// stored once per target in a Redis hash.
type targetState struct {
	Stage      string    `json:"s"`
	DeliveryID string    `json:"d"`
	Attempts   int       `json:"a"`
	StatusCode int       `json:"c,omitempty"`
	Error      string    `json:"e,omitempty"`
	Category   string    `json:"g,omitempty"`
	NotBefore  time.Time `json:"n,omitempty"`
	UpdatedAt  time.Time `json:"u"`
}

// terminal reports whether the target reached a final stage.
func (state targetState) terminal() bool {
	return state.Stage == targetStageSucceeded || state.Stage == targetStageFailed
}

// targetDeliveryID derives the stable per-target delivery identity. It is a
// deterministic function of the durable handoff identity (the job ID) and the
// target name, so recovery, reclaim, and manual replay all observe the same
// identity without trusting a previous process's memory.
func targetDeliveryID(jobID, target string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("urn:owlmail:webhook:"+jobID+"#"+target)).String()
}

// classifyTargetResult folds one HTTP dispatch result into the persisted
// target stage. Attempt counts accumulate across claims.
func classifyTargetResult(jobID string, previous targetState, result Result, now time.Time) targetState {
	state := targetState{
		DeliveryID: targetDeliveryID(jobID, result.Target),
		Attempts:   previous.Attempts + result.Attempts,
		UpdatedAt:  now,
	}
	if result.Err == nil {
		state.Stage = targetStageSucceeded
		state.StatusCode = result.StatusCode
		return state
	}
	state.StatusCode = result.StatusCode
	state.Error = result.Err.Error()
	state.Category = resultErrorCategory(result)
	state.NotBefore = now
	if result.Retryable {
		state.Stage = targetStageRetry
	} else {
		state.Stage = targetStageFailed
	}
	return state
}

func resultErrorCategory(result Result) string {
	if result.Target == "" {
		return "queue"
	}
	if result.StatusCode != 0 {
		return "http_status"
	}
	return "transport"
}

func encodeTargetState(state targetState) (string, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeTargetState(encoded string) (targetState, error) {
	var state targetState
	if err := json.Unmarshal([]byte(encoded), &state); err != nil {
		return targetState{}, err
	}
	return state, nil
}

// pendingTargetState is the reset stage written when a failed target is
// reopened for an explicit replay.
func pendingTargetState(jobID, target string, now time.Time) targetState {
	return targetState{
		Stage:      targetStagePending,
		DeliveryID: targetDeliveryID(jobID, target),
		UpdatedAt:  now,
	}
}
