package messaging

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

const pendingWorkerName = "pending-reference"

type PendingReferenceService interface {
	ClaimPendingReferences(context.Context, string, int, time.Duration) ([]postgres.PendingReferenceClaim, error)
	ResumePendingReference(context.Context, string, string) (application.TransactionResult, error)
}

type PendingWorkerOptions struct {
	WorkerID     string
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	Jitter       func(time.Duration) time.Duration
}

type PendingWorker struct {
	service    PendingReferenceService
	metrics    WorkerMetrics
	logger     *slog.Logger
	options    PendingWorkerOptions
	mu         sync.Mutex
	started    bool
	pollCancel context.CancelFunc
	workCancel context.CancelFunc
	wg         sync.WaitGroup
}

func NewPendingWorker(service PendingReferenceService, metrics WorkerMetrics, logger *slog.Logger, options PendingWorkerOptions) (*PendingWorker, error) {
	if service == nil || options.WorkerID == "" || options.BatchSize <= 0 || options.PollInterval <= 0 || options.Lease <= 0 {
		return nil, errors.New("messaging: invalid pending-reference worker")
	}
	if options.Jitter == nil {
		options.Jitter = jitterInterval
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &PendingWorker{service: service, metrics: metrics, logger: logger, options: options}, nil
}

func (worker *PendingWorker) Start(context.Context) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.started {
		return errors.New("messaging: pending-reference worker already started")
	}
	pollContext, pollCancel := context.WithCancel(context.Background())
	workContext, workCancel := context.WithCancel(context.Background())
	worker.started = true
	worker.pollCancel = pollCancel
	worker.workCancel = workCancel
	worker.wg.Add(1)
	go worker.run(pollContext, workContext)
	return nil
}

func (worker *PendingWorker) run(pollContext, workContext context.Context) {
	defer worker.wg.Done()
	for {
		claims, err := worker.service.ClaimPendingReferences(pollContext, worker.options.WorkerID, worker.options.BatchSize, worker.options.Lease)
		if err != nil && pollContext.Err() == nil {
			worker.retry("claim_error")
			worker.logger.WarnContext(pollContext, "pending-reference claim failed")
		}
		for _, claim := range claims {
			if pollContext.Err() != nil {
				return
			}
			if claim.Record.Transaction == nil {
				worker.retry("invalid_claim")
				continue
			}
			startedAt := time.Now()
			result, err := worker.service.ResumePendingReference(workContext, claim.Record.Transaction.ID(), worker.options.WorkerID)
			if err != nil && !errors.Is(err, postgres.ErrClaimLost) && workContext.Err() == nil {
				worker.retry("resume_error")
				worker.logger.WarnContext(workContext, "pending-reference resume failed", slog.String("transactionId", claim.Record.Transaction.ID()))
				continue
			}
			if err == nil && result.Transaction != nil {
				details := result.Transaction.Details()
				status := string(result.Transaction.State())
				if worker.metrics != nil {
					worker.metrics.RecordTransactionResult(string(details.Kind), status, string(result.Transaction.FailureCode()))
					worker.metrics.ObserveProcessingLatency(string(details.Kind), status, time.Since(startedAt))
				}
				worker.logger.InfoContext(workContext, "pending reference resolved",
					slog.String("transactionId", result.Transaction.ID()), slog.String("status", status))
			}
		}
		if !wait(pollContext, worker.options.Jitter(worker.options.PollInterval)) {
			return
		}
	}
}

func jitterInterval(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	quarter := delay / 4
	value, err := rand.Int(rand.Reader, big.NewInt(int64(quarter*2)+1))
	if err != nil {
		return delay
	}
	return delay - quarter + time.Duration(value.Int64())
}

func (worker *PendingWorker) retry(result string) {
	if worker.metrics != nil {
		worker.metrics.RecordRetry(pendingWorkerName, result)
	}
}

func (worker *PendingWorker) Shutdown(ctx context.Context) error {
	worker.mu.Lock()
	if !worker.started {
		worker.mu.Unlock()
		return nil
	}
	worker.started = false
	worker.pollCancel()
	workCancel := worker.workCancel
	worker.mu.Unlock()
	done := make(chan struct{})
	go func() { worker.wg.Wait(); close(done) }()
	select {
	case <-done:
		workCancel()
		return nil
	case <-ctx.Done():
		workCancel()
		return ctx.Err()
	}
}
