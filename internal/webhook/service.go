package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/soulteary/owlmail/internal/types"
)

const (
	defaultMemoryQueueSize = 1024
	defaultShutdownTimeout = 15 * time.Second
	defaultLeaseRefresh    = redisClaimIdle / 3
	defaultOutboxRetry     = 250 * time.Millisecond
)

// ServiceOptions configures queued webhook delivery.
type ServiceOptions struct {
	RedisURL        string
	RedisPrefix     string
	MaxConcurrency  int
	ShutdownTimeout time.Duration
	SpoolDir        string
	OnResults       func(string, []Result)
}

type deliveryJob struct {
	ID         string       `json:"id"`
	EnqueuedAt time.Time    `json:"enqueuedAt"`
	Email      *types.Email `json:"email"`
	Targets    []string     `json:"targets,omitempty"`
	RetryOf    string       `json:"retryOf,omitempty"`
}

// Service owns webhook queue workers and their cancellation/drain lifecycle.
type Service struct {
	history         *deliveryHistory
	replays         sync.Map
	dispatcher      *Dispatcher
	queue           deliveryQueue
	memoryQueue     chan deliveryJob
	outbox          *deliveryOutbox
	outboxWake      chan struct{}
	outboxClosing   chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	shutdownTimeout time.Duration
	onResults       func(string, []Result)
	workerCount     int
	leaseRefresh    time.Duration
	unlimited       bool
	accepting       bool
	handoffMutex    sync.RWMutex
	closeOnce       sync.Once
	handoffs        sync.WaitGroup
	workers         sync.WaitGroup
	outboxWorkers   sync.WaitGroup
	deliveries      sync.WaitGroup
	promotions      sync.Map
	stagedHandoffs  sync.Map
	closeErr        error
}

// NewService creates and starts a queued delivery service. Redis-backed mode
// uses a consumer-group stream and dead-letter stream; an empty Redis URL keeps
// a process-local queue while retaining cancellation and graceful drain.
func NewService(dispatcher *Dispatcher, options ServiceOptions) (*Service, error) {
	if dispatcher == nil {
		return nil, fmt.Errorf("webhook dispatcher is nil")
	}
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("webhook max concurrency must be zero or greater")
	}
	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = defaultShutdownTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		dispatcher: dispatcher.withMaxConcurrency(options.MaxConcurrency), ctx: ctx, cancel: cancel,
		shutdownTimeout: options.ShutdownTimeout, onResults: options.OnResults,
		workerCount: options.MaxConcurrency, unlimited: options.MaxConcurrency == 0,
		leaseRefresh: defaultLeaseRefresh,
	}
	if options.SpoolDir != "" {
		outbox, err := newDeliveryOutbox(options.SpoolDir)
		if err != nil {
			cancel()
			return nil, err
		}
		service.outbox = outbox
		service.outboxWake = make(chan struct{}, 1)
		service.outboxClosing = make(chan struct{})
	}
	if options.RedisURL != "" {
		consumer := fmt.Sprintf("%s-%s", runtime.GOOS, uuid.NewString())
		queue, err := newRedisDeliveryQueue(ctx, options.RedisURL, options.RedisPrefix, consumer)
		if err != nil {
			cancel()
			return nil, err
		}
		service.queue = queue
	} else {
		service.memoryQueue = make(chan deliveryJob, defaultMemoryQueueSize)
	}
	history, err := newDeliveryHistory(options.SpoolDir)
	if err != nil {
		// Diagnostic history must never take the delivery service offline.
		// Preserve the recovery error separately from subsequent write health.
		history.recoveryErr = err
	}
	service.history = history
	service.accepting = true
	service.start()
	// Rebuild queryable terminal records from the durable Redis generation
	// before accepting new handoffs; failures stay diagnostic.
	service.recoverTerminalDeliveries()
	return service, nil
}

func (service *Service) start() {
	if service.outbox != nil {
		service.outboxWorkers.Add(1)
		go service.runOutbox()
	}
	if service.unlimited {
		service.workers.Add(1)
		go service.runPump()
		return
	}
	for i := 0; i < service.workerCount; i++ {
		service.workers.Add(1)
		go service.runWorker()
	}
}

// Enqueue durably hands an email to the local outbox when configured. Delivery
// remains at-least-once: receivers should deduplicate X-OwlMail-Delivery-ID.
func (service *Service) Enqueue(email *types.Email) error {
	if service == nil || email == nil {
		return fmt.Errorf("webhook email is nil")
	}
	reservationID := ""
	service.handoffMutex.Lock()
	if !service.accepting {
		service.handoffMutex.Unlock()
		return fmt.Errorf("webhook service is shutting down")
	}
	service.handoffs.Add(1)
	if service.outbox != nil {
		reservationID = uuid.NewString()
		service.stagedHandoffs.Store(reservationID, email.ID)
	}
	service.handoffMutex.Unlock()
	releaseHandoff := true
	defer func() {
		if releaseHandoff {
			if reservationID != "" {
				service.releaseStagedReservation(reservationID)
			} else {
				service.handoffs.Done()
			}
		}
	}()
	emailSnapshot, err := cloneDeliveryEmail(email)
	if err != nil {
		return err
	}
	job := deliveryJob{ID: uuid.NewString(), EnqueuedAt: time.Now().UTC(), Email: emailSnapshot}
	if service.outbox != nil {
		if err := service.outbox.Store(job); err != nil {
			service.wakeOutbox()
			return err
		}
		releaseHandoff = false
		return nil
	}
	if service.queue != nil {
		if err := service.queue.Enqueue(service.ctx, job); err != nil {
			return err
		}
		return nil
	}
	select {
	case service.memoryQueue <- job:
		return nil
	case <-service.ctx.Done():
		return service.ctx.Err()
	}
}

// Commit makes staged outbox jobs consumable after the mail server has durably
// recorded acceptance. Calling it again is safe and supports startup recovery.
func (service *Service) Commit(emailID string) error {
	if service == nil || service.outbox == nil || emailID == "" {
		return nil
	}
	err := service.outbox.Commit(emailID)
	if err != nil {
		service.promotions.Store(emailID, struct{}{})
	} else {
		service.promotions.Delete(emailID)
		service.releaseStagedHandoffs(emailID)
	}
	service.wakeOutbox()
	return err
}

// Abort releases a staged handoff for a mail transaction that was rejected.
func (service *Service) Abort(emailID string) error {
	if service == nil || service.outbox == nil || emailID == "" {
		return nil
	}
	err := service.outbox.Discard(emailID)
	service.releaseStagedHandoffs(emailID)
	service.wakeOutbox()
	return err
}

func (service *Service) releaseStagedReservation(reservationID string) {
	if _, tracked := service.stagedHandoffs.LoadAndDelete(reservationID); tracked {
		service.handoffs.Done()
	}
}

func (service *Service) releaseStagedHandoffs(emailID string) {
	service.stagedHandoffs.Range(func(key, value any) bool {
		trackedEmailID, ok := value.(string)
		if !ok || trackedEmailID != emailID {
			return true
		}
		if _, tracked := service.stagedHandoffs.LoadAndDelete(key); tracked {
			service.handoffs.Done()
		}
		return true
	})
}

func (service *Service) releaseAllStagedHandoffs() {
	service.stagedHandoffs.Range(func(key, _ any) bool {
		if _, tracked := service.stagedHandoffs.LoadAndDelete(key); tracked {
			service.handoffs.Done()
		}
		return true
	})
}

// RecoverAcceptedPending promotes accepted jobs using durable fence state,
// independent of whether the email is still present in the mailbox.
func (service *Service) RecoverAcceptedPending() error {
	if service == nil || service.outbox == nil {
		return nil
	}
	ids, err := service.outbox.AcceptedPendingEmailIDs()
	if err != nil {
		return err
	}
	var recoveryErrors []error
	for _, emailID := range ids {
		if err := service.Commit(emailID); err != nil {
			recoveryErrors = append(recoveryErrors, fmt.Errorf("recover accepted webhook job for %s: %w", emailID, err))
		}
	}
	return errors.Join(recoveryErrors...)
}

func (service *Service) wakeOutbox() {
	select {
	case service.outboxWake <- struct{}{}:
	default:
	}
}

func (service *Service) runOutbox() {
	defer service.outboxWorkers.Done()
	closing := false
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	for {
		pending, err := service.flushOutbox()
		if err != nil {
			service.report("", []Result{{Err: err}})
			retry.Reset(defaultOutboxRetry)
			if closing {
				select {
				case <-retry.C:
				case <-service.outboxWake:
					stopOutboxRetry(retry)
				case <-service.ctx.Done():
					return
				}
			} else {
				select {
				case <-retry.C:
				case <-service.outboxWake:
					stopOutboxRetry(retry)
				case <-service.outboxClosing:
					closing = true
					stopOutboxRetry(retry)
				case <-service.ctx.Done():
					return
				}
			}
			continue
		}
		if pending {
			continue
		}
		if closing {
			return
		}
		retry.Reset(defaultOutboxRetry)
		select {
		case <-retry.C:
		case <-service.outboxWake:
			stopOutboxRetry(retry)
		case <-service.outboxClosing:
			closing = true
			stopOutboxRetry(retry)
		case <-service.ctx.Done():
			return
		}
	}
}

func stopOutboxRetry(retry *time.Timer) {
	if !retry.Stop() {
		select {
		case <-retry.C:
		default:
		}
	}
}

func (service *Service) flushOutbox() (bool, error) {
	service.retryPromotions()
	if err := service.outbox.PruneRejectedPending(); err != nil {
		service.report("", []Result{{Err: err}})
	}
	entries, err := service.outbox.List()
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	entry := entries[0]
	if service.queue != nil {
		if err := service.queue.Enqueue(service.ctx, entry.job); err != nil {
			return true, err
		}
	} else {
		select {
		case service.memoryQueue <- entry.job:
		case <-service.ctx.Done():
			return true, service.ctx.Err()
		}
	}
	if err := service.outbox.Remove(entry.path); err != nil {
		return true, err
	}
	return true, nil
}

func (service *Service) retryPromotions() {
	var promotionErrors []error
	service.promotions.Range(func(key, _ any) bool {
		emailID, ok := key.(string)
		if !ok {
			service.promotions.Delete(key)
			return true
		}
		if err := service.outbox.Commit(emailID); err != nil {
			promotionErrors = append(promotionErrors, fmt.Errorf("promote accepted webhook job for %s: %w", emailID, err))
			return true
		}
		service.promotions.Delete(emailID)
		service.releaseStagedHandoffs(emailID)
		return true
	})
	if err := errors.Join(promotionErrors...); err != nil {
		service.report("", []Result{{Err: err}})
	}
}

func cloneDeliveryEmail(email *types.Email) (*types.Email, error) {
	encoded, err := json.Marshal(email)
	if err != nil {
		return nil, fmt.Errorf("encode webhook email snapshot: %w", err)
	}
	var snapshot types.Email
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return nil, fmt.Errorf("decode webhook email snapshot: %w", err)
	}
	return &snapshot, nil
}

func (service *Service) runWorker() {
	defer service.workers.Done()
	for {
		job, receipt, ok := service.nextJob()
		if !ok {
			return
		}
		service.deliver(job, receipt)
	}
}

func (service *Service) runPump() {
	defer service.workers.Done()
	for {
		job, receipt, ok := service.nextJob()
		if !ok {
			return
		}
		service.deliveries.Add(1)
		go func() {
			defer service.deliveries.Done()
			service.deliver(job, receipt)
		}()
	}
}

func (service *Service) nextJob() (deliveryJob, *queueReceipt, bool) {
	if service.queue == nil {
		select {
		case job, open := <-service.memoryQueue:
			return job, nil, open
		case <-service.ctx.Done():
			return deliveryJob{}, nil, false
		}
	}
	for {
		receipt, err := service.queue.Claim(service.ctx)
		if err != nil {
			if service.ctx.Err() != nil {
				return deliveryJob{}, nil, false
			}
			service.report("", []Result{{Err: err}})
			continue
		}
		if receipt != nil {
			return receipt.job, receipt, true
		}
		// While draining, do not wait to reclaim entries this process just
		// released: the restarted process takes over the same queue generation
		// after the idle window.
		if !service.isAccepting() {
			return deliveryJob{}, nil, false
		}
	}
}

func (service *Service) isAccepting() bool {
	service.handoffMutex.RLock()
	defer service.handoffMutex.RUnlock()
	return service.accepting
}

func (service *Service) deliver(job deliveryJob, receipt *queueReceipt) {
	if service.queue == nil {
		service.deliverInMemory(job)
		return
	}
	service.deliverDurable(job, receipt)
}

// deliverInMemory keeps the non-durable path unchanged: no Redis, no
// restart-safe per-target stages, one dispatch and one diagnostic record.
func (service *Service) deliverInMemory(job deliveryJob) {
	results := service.dispatcher.dispatchTargets(service.ctx, job.Email, job.ID, job.Targets)
	if service.history != nil {
		service.history.record(job, results)
	}
	service.replays.Delete(job.ID)
	service.report(job.ID, results)
}

// deliverDurable walks the recoverable per-target stages. Each target stage is
// written to Redis behind the claim-generation fence; only targets without a
// terminal stage receive an HTTP attempt. The job is acknowledged only after
// every expected target is terminal, while retryable targets retain the entry
// for a later claim and permanent failures become queryable terminal state.
func (service *Service) deliverDurable(job deliveryJob, receipt *queueReceipt) {
	deliveryCtx, cancel := context.WithCancel(service.ctx)
	var leaseStolen atomic.Bool
	stopLease := service.startLeaseHeartbeat(receipt, func() {
		leaseStolen.Store(true)
		cancel()
	})

	selected := service.dispatcher.selectTargets(job.Email, job.Targets)
	states, err := service.queue.TargetStates(deliveryCtx, job.ID)
	if err != nil {
		stopLease()
		if service.ctx.Err() == nil && !leaseStolen.Load() {
			service.report(job.ID, []Result{{Err: err}})
		}
		return
	}
	plan := make([]compiledTarget, 0, len(selected))
	for _, target := range selected {
		state, seen := states[target.name]
		if !seen {
			plan = append(plan, target)
			continue
		}
		if state.terminal() {
			continue
		}
		if state.Stage == targetStageRetry && !state.NotBefore.IsZero() && state.NotBefore.After(time.Now().UTC()) {
			continue
		}
		plan = append(plan, target)
	}
	results := service.dispatcher.dispatchSelected(deliveryCtx, job.Email, job.ID, plan)
	stopLease()

	// Shutdown or a stolen lease aborted the attempt: the PEL entry stays for
	// the next claim generation, so no late result is written.
	if service.ctx.Err() != nil || leaseStolen.Load() {
		if leaseStolen.Load() {
			service.report(job.ID, []Result{{Err: ErrLeaseNotOwned}})
		}
		return
	}

	now := time.Now().UTC()
	for _, result := range results {
		state := classifyTargetResult(job.ID, states[result.Target], result, now)
		disposition, recordErr := service.queue.RecordResult(service.ctx, receipt, result.Target, state)
		if recordErr != nil {
			service.report(job.ID, []Result{{Err: recordErr}})
			return
		}
		switch disposition {
		case leaseHeld:
			states[result.Target] = state
		case leaseFinalized:
			return
		case leaseLost:
			service.report(job.ID, []Result{{Err: ErrLeaseNotOwned}})
			return
		}
	}

	expected := targetNames(selected)
	disposition, err := service.queue.Finalize(service.ctx, receipt, expected)
	if err != nil {
		service.report(job.ID, []Result{{Err: err}})
		return
	}
	switch disposition {
	case finalizeCompleted, finalizeAlreadyDone:
		latest, statesErr := service.queue.TargetStates(service.ctx, job.ID)
		if statesErr != nil {
			service.report(job.ID, []Result{{Err: statesErr}})
			return
		}
		if service.history != nil {
			service.history.recordTerminal(job, latest, time.Now().UTC())
		}
		service.replays.Delete(job.ID)
		service.report(job.ID, resultsFromStates(expected, latest))
	case finalizeLeaseLost:
		service.report(job.ID, []Result{{Err: ErrLeaseNotOwned}})
	case finalizeRetained:
		// Retryable targets keep the stream entry for the next claim; the
		// fence was released server-side. This round's outcomes stay visible.
		service.report(job.ID, results)
	}
}

// resultsFromStates rebuilds the ordered outcome list from durable stages.
func resultsFromStates(expected []string, states map[string]targetState) []Result {
	results := make([]Result, 0, len(expected))
	for _, name := range expected {
		state, ok := states[name]
		if !ok {
			continue
		}
		result := Result{Target: name, StatusCode: state.StatusCode, Attempts: state.Attempts}
		if state.Stage == targetStageFailed {
			result.Err = errors.New(state.Error)
		}
		if state.Stage == targetStageRetry {
			result.Retryable = true
			result.Err = errors.New(state.Error)
		}
		results = append(results, result)
	}
	return results
}

// startLeaseHeartbeat keeps the claim-generation fence alive while HTTP
// delivery is active. Losing the fence invokes onLeaseLost so the delivery
// context is canceled before any late result can be submitted.
func (service *Service) startLeaseHeartbeat(receipt *queueReceipt, onLeaseLost func()) func() {
	if service.queue == nil || receipt == nil || service.leaseRefresh <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(service.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(service.leaseRefresh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := service.queue.Renew(ctx, receipt)
				if err == nil {
					continue
				}
				if errors.Is(err, ErrLeaseNotOwned) {
					if onLeaseLost != nil {
						onLeaseLost()
					}
					return
				}
				if ctx.Err() == nil {
					service.report(receipt.job.ID, []Result{{Err: err}})
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// recoverTerminalDeliveries backfills the queryable history from the durable
// Redis generation after a restart. The stream/PEL and per-target states are
// the source of truth; this only restores the diagnostic view.
func (service *Service) recoverTerminalDeliveries() {
	if service.queue == nil || service.history == nil {
		return
	}
	handoffs, err := service.queue.RecoverTerminal(service.ctx)
	if err != nil {
		service.report("", []Result{{Err: err}})
		return
	}
	for _, handoff := range handoffs {
		service.history.backfillTerminal(handoff)
	}
}

func (service *Service) report(jobID string, results []Result) {
	if service.onResults != nil {
		service.onResults(jobID, results)
	}
}

// Close stops accepting jobs, drains queued and active delivery until the
// configured deadline, then cancels remaining HTTP and Redis operations.
func (service *Service) Close() error {
	if service == nil {
		return nil
	}
	service.closeOnce.Do(func() {
		service.handoffMutex.Lock()
		service.accepting = false
		service.handoffMutex.Unlock()
		drained := make(chan struct{})
		go func() {
			service.handoffs.Wait()
			if service.outbox != nil {
				close(service.outboxClosing)
				service.wakeOutbox()
				service.outboxWorkers.Wait()
			}
			if service.memoryQueue != nil {
				close(service.memoryQueue)
			}
			service.workers.Wait()
			service.deliveries.Wait()
			close(drained)
		}()
		timer := time.NewTimer(service.shutdownTimeout)
		defer timer.Stop()
		select {
		case <-drained:
		case <-timer.C:
			service.closeErr = fmt.Errorf("webhook drain exceeded %s", service.shutdownTimeout)
			service.cancel()
			service.releaseAllStagedHandoffs()
			// Filesystem operations cannot be canceled. Let admitted work finish
			// and clean up asynchronously rather than extending the deadline.
			go func() {
				<-drained
				if service.queue != nil {
					_ = service.queue.Close()
				}
			}()
			return
		}
		service.cancel()
		if service.queue != nil {
			if err := service.queue.Close(); err != nil && !errors.Is(err, context.Canceled) && service.closeErr == nil {
				service.closeErr = err
			}
		}
	})
	return service.closeErr
}
