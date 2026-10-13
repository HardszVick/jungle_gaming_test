package messaging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

const outboxWorkerName = "outbox"

type Clock interface{ Now() time.Time }

type OutboxOptions struct {
	PublisherID    string
	QueueURL       string
	BatchSize      int
	PollInterval   time.Duration
	Lease          time.Duration
	BackoffBase    time.Duration
	BackoffMaximum time.Duration
	// Boundary hooks are nil in normal binaries and let integration tests stop a
	// publisher at the two crash-ambiguous outbox boundaries.
	BeforePublish          func(context.Context, postgres.OutboxClaim) error
	AfterPublishBeforeMark func(context.Context, postgres.OutboxClaim) error
}

type OutboxPublisher struct {
	repository OutboxRepository
	broker     Broker
	clock      Clock
	metrics    OutboxMetrics
	logger     *slog.Logger
	options    OutboxOptions
	mu         sync.Mutex
	started    bool
	pollCancel context.CancelFunc
	workCancel context.CancelFunc
	wg         sync.WaitGroup
}

func NewOutboxPublisher(repository OutboxRepository, broker Broker, clock Clock, metrics OutboxMetrics, logger *slog.Logger, options OutboxOptions) (*OutboxPublisher, error) {
	if repository == nil || broker == nil || clock == nil || options.PublisherID == "" || options.QueueURL == "" ||
		options.BatchSize <= 0 || options.PollInterval <= 0 || options.Lease <= 0 ||
		options.BackoffBase <= 0 || options.BackoffMaximum < options.BackoffBase {
		return nil, errors.New("messaging: invalid outbox publisher")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &OutboxPublisher{repository: repository, broker: broker, clock: clock, metrics: metrics, logger: logger, options: options}, nil
}

func (publisher *OutboxPublisher) Start(context.Context) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.started {
		return errors.New("messaging: outbox publisher already started")
	}
	pollContext, pollCancel := context.WithCancel(context.Background())
	workContext, workCancel := context.WithCancel(context.Background())
	publisher.started = true
	publisher.pollCancel = pollCancel
	publisher.workCancel = workCancel
	publisher.wg.Add(1)
	go publisher.run(pollContext, workContext)
	return nil
}

func (publisher *OutboxPublisher) run(pollContext, workContext context.Context) {
	defer publisher.wg.Done()
	for {
		claims, err := publisher.repository.ClaimOutbox(pollContext, publisher.options.PublisherID, publisher.options.BatchSize, publisher.options.Lease)
		if err != nil {
			if pollContext.Err() != nil {
				return
			}
			publisher.retry("claim_error")
			publisher.logger.WarnContext(pollContext, "outbox claim failed")
		} else {
			for _, claim := range claims {
				if pollContext.Err() != nil {
					return
				}
				publisher.publish(workContext, claim)
			}
		}
		if !wait(pollContext, publisher.options.PollInterval) {
			return
		}
	}
}

func (publisher *OutboxPublisher) publish(ctx context.Context, claim postgres.OutboxClaim) {
	now := publisher.clock.Now().UTC()
	if publisher.metrics != nil {
		publisher.metrics.ObserveOutboxLag(now.Sub(claim.CreatedAt))
	}
	if hook := publisher.options.BeforePublish; hook != nil {
		if err := hook(ctx, claim); err != nil {
			return
		}
	}
	err := publisher.broker.Publish(ctx, publisher.options.QueueURL, claim.Envelope, claim.AggregateID, claim.EventID)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		publisher.publication("failure")
		publisher.retry("publish_error")
		publisher.logger.WarnContext(ctx, "outbox publication failed", slog.String("eventId", claim.EventID), slog.String("aggregateId", claim.AggregateID))
		delay := publisher.backoff(claim.AttemptCount + 1)
		if rescheduleErr := publisher.repository.RescheduleOutbox(ctx, claim.EventID, publisher.options.PublisherID, delay, sanitizePublishError(err)); rescheduleErr != nil {
			publisher.retry("reschedule_error")
		}
		return
	}
	if hook := publisher.options.AfterPublishBeforeMark; hook != nil {
		if err := hook(ctx, claim); err != nil {
			return
		}
	}
	// A mark failure intentionally leaves the claim recoverable. The next
	// publication uses the same event ID as its FIFO deduplication identity.
	if err := publisher.repository.MarkOutboxPublished(ctx, claim.EventID, publisher.options.PublisherID); err != nil {
		publisher.publication("mark_failure")
		publisher.retry("mark_error")
		publisher.logger.WarnContext(ctx, "outbox publication mark failed", slog.String("eventId", claim.EventID), slog.String("aggregateId", claim.AggregateID))
		return
	}
	publisher.publication("success")
}

func (publisher *OutboxPublisher) backoff(attempt int) time.Duration {
	delay := publisher.options.BackoffBase
	for current := 1; current < attempt && delay < publisher.options.BackoffMaximum; current++ {
		if delay > publisher.options.BackoffMaximum/2 {
			return publisher.options.BackoffMaximum
		}
		delay *= 2
	}
	if delay > publisher.options.BackoffMaximum {
		return publisher.options.BackoffMaximum
	}
	return delay
}

func sanitizePublishError(error) string { return "SQS publication failed" }

func (publisher *OutboxPublisher) retry(result string) {
	if publisher.metrics != nil {
		publisher.metrics.RecordRetry(outboxWorkerName, result)
	}
}

func (publisher *OutboxPublisher) publication(result string) {
	if publisher.metrics != nil {
		publisher.metrics.RecordOutboxPublication(result)
	}
}

func (publisher *OutboxPublisher) Shutdown(ctx context.Context) error {
	publisher.mu.Lock()
	if !publisher.started {
		publisher.mu.Unlock()
		return nil
	}
	publisher.started = false
	publisher.pollCancel()
	workCancel := publisher.workCancel
	publisher.mu.Unlock()
	done := make(chan struct{})
	go func() { publisher.wg.Wait(); close(done) }()
	select {
	case <-done:
		workCancel()
		return nil
	case <-ctx.Done():
		workCancel()
		publisher.wg.Wait()
		return ctx.Err()
	}
}
