package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/soulteary/owlmail/internal/common"
	"github.com/soulteary/owlmail/internal/mailserver"
)

func (api *API) getDeliveryCursor(c fiber.Ctx) error {
	return c.JSON(fiber.Map{"cursor": api.mailServer.DeliveryCursor()})
}
func (api *API) waitDeliveries(c fiber.Ctx) error {
	var input struct {
		After string `json:"after"`
		mailserver.DeliveryFilter
		Count          int `json:"count"`
		TimeoutSeconds int `json:"timeout_seconds"`
	}
	if err := c.Bind().Body(&input); err != nil {
		return invalidEmailQuery(c, err)
	}
	if input.Count == 0 {
		input.Count = 1
	}
	if input.TimeoutSeconds == 0 {
		input.TimeoutSeconds = 30
	}
	if input.TimeoutSeconds < 1 || input.TimeoutSeconds > 120 {
		return c.Status(400).JSON(ErrorResponse(ErrorCodeInvalidRequest, "timeout_seconds must be 1..120"))
	}
	result, err := api.mailServer.WaitDeliveries(c.Context(), input.After, input.DeliveryFilter, input.Count, time.Duration(input.TimeoutSeconds)*time.Second)
	if err != nil {
		status := 400
		code := ErrorCodeInvalidRequest
		if errors.Is(err, mailserver.ErrWaitCapacity) {
			status = 429
			c.Set("Retry-After", "1")
		}
		if errors.Is(err, mailserver.ErrMailboxClosed) {
			status = 503
		}
		if errors.Is(err, mailserver.ErrCursorExpired) {
			code = ErrorCodeCursorExpired
		}
		return c.Status(status).JSON(ErrorResponse(code, err.Error()))
	}
	return c.JSON(result)
}

// protectRunCleanupSource combines the relay-queue ownership check with the
// mailserver delete. A queued relay is reported with the domain sentinel so
// run cleanup records it as relay_pending instead of failing the whole page.
func (api *API) protectRunCleanupSource(id string, delete func(string) error) error {
	err := api.relayJobs.protectSourceDeletion([]string{id}, func() error {
		return delete(id)
	})
	if errors.Is(err, errRelaySourceInUse) {
		return mailserver.ErrRunEmailRelayPending
	}
	return err
}

// runCleanupDeleteOne is the per-member deleter for interactive cleanup pages.
func (api *API) runCleanupDeleteOne(id string) error {
	return api.protectRunCleanupSource(id, api.mailServer.DeleteEmail)
}

// runCleanupResumeDeleteOne is the per-member deleter for startup takeover;
// it suppresses delete events so pre-crash deletions are not rebroadcast.
func (api *API) runCleanupResumeDeleteOne(id string) error {
	return api.protectRunCleanupSource(id, api.mailServer.DeleteEmailQuiet)
}

// resumeRunCleanups takes over unfinished snapshots after startup. Failures are
// logged but never block serving: snapshots stay parked for the next attempt.
func (api *API) resumeRunCleanups() {
	results, err := api.mailServer.ResumeRunCleanups(api.runCleanupResumeDeleteOne)
	if err != nil {
		common.Error("Run cleanup takeover completed with errors: %v", err)
	}
	for _, result := range results {
		common.Log("Run cleanup takeover for %s: %d deleted, %d failed, more_possible=%t",
			result.RunID, len(result.Deleted), result.Failed, result.MorePossible)
	}
}

func (api *API) deleteTestRunEmails(c fiber.Ctx) error {
	runID := c.Params("runID")
	if err := mailserver.ValidateRunID(runID); err != nil || runID == "" {
		return c.Status(400).JSON(ErrorResponse(ErrorCodeInvalidRequest, "Invalid run_id"))
	}
	limit := mailserver.RunCleanupDefaultLimit
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > mailserver.RunCleanupMaxLimit {
			return c.Status(400).JSON(ErrorResponse(ErrorCodeInvalidRequest, "limit must be an integer between 1 and 1000"))
		}
		limit = parsed
	}
	result, err := api.mailServer.CleanupTestRun(mailserver.RunCleanupRequest{
		RunID:  runID,
		Cursor: c.Query("cursor"),
		Limit:  limit,
	}, api.runCleanupDeleteOne)
	if err != nil {
		if errors.Is(err, mailserver.ErrRunCleanupInvalidCursor) {
			return c.Status(400).JSON(ErrorResponse(ErrorCodeInvalidRequest, err.Error()))
		}
		// Snapshot persistence or other server-side failures: request
		// parameters were already validated above.
		return c.Status(500).JSON(ErrorResponse(ErrorCodeInvalidRequest, err.Error()))
	}
	if result.Failures == nil {
		result.Failures = []mailserver.RunCleanupFailure{}
	}
	if result.Deleted == nil {
		result.Deleted = []string{}
	}
	return c.JSON(result)
}
