package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

const consumerWorkerName = "sqs-consumer"

type TransactionProcessor interface {
	ProcessTransaction(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error)
}

type ConsumerOptions struct {
	QueueURL        string
	ConsumerName    string
	BatchSize       int
	WaitTime        time.Duration
	Visibility      time.Duration
	ErrorDelay      time.Duration
	BackoffBase     time.Duration
	BackoffMaximum  time.Duration
	MaxReceiveCount int
	// AfterCommitBeforeDelete is an integration seam for the ambiguous crash
	// boundary after the database commit and before SQS acknowledgement.
	AfterCommitBeforeDelete func(context.Context) error
}

type Consumer struct {
	broker    Broker
	processor TransactionProcessor
	metrics   ConsumerMetrics
	logger    *slog.Logger
	options   ConsumerOptions

	mu            sync.Mutex
	started       bool
	pollCancel    context.CancelFunc
	handlerCancel context.CancelFunc
	active        map[string]struct{}
	pollWG        sync.WaitGroup
	handlerWG     sync.WaitGroup
}

func NewConsumer(broker Broker, processor TransactionProcessor, metrics ConsumerMetrics, logger *slog.Logger, options ConsumerOptions) (*Consumer, error) {
	if broker == nil || processor == nil {
		return nil, errors.New("messaging: consumer dependencies are required")
	}
	if options.QueueURL == "" || options.ConsumerName == "" || options.BatchSize < 1 || options.BatchSize > 10 ||
		options.WaitTime <= 0 || options.Visibility <= 0 || options.ErrorDelay <= 0 ||
		options.BackoffBase <= 0 || options.BackoffMaximum < options.BackoffBase || options.MaxReceiveCount <= 0 {
		return nil, errors.New("messaging: invalid consumer options")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Consumer{broker: broker, processor: processor, metrics: metrics, logger: logger, options: options, active: make(map[string]struct{})}, nil
}

func (consumer *Consumer) Start(context.Context) error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.started {
		return errors.New("messaging: consumer already started")
	}
	pollContext, pollCancel := context.WithCancel(context.Background())
	handlerContext, handlerCancel := context.WithCancel(context.Background())
	consumer.started = true
	consumer.pollCancel = pollCancel
	consumer.handlerCancel = handlerCancel
	consumer.pollWG.Add(1)
	go consumer.poll(pollContext, handlerContext)
	return nil
}

func (consumer *Consumer) poll(ctx, handlerContext context.Context) {
	defer consumer.pollWG.Done()
	for {
		messages, err := consumer.broker.Receive(ctx, consumer.options.QueueURL, consumer.options.BatchSize, consumer.options.WaitTime, consumer.options.Visibility)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			consumer.recordRetry("receive_error")
			if !wait(ctx, consumer.options.ErrorDelay) {
				return
			}
			continue
		}
		groups := groupMessages(messages)
		var batch sync.WaitGroup
		for index, group := range groups {
			if ctx.Err() != nil {
				for _, unstartedGroup := range groups[index:] {
					for _, unstarted := range unstartedGroup {
						consumer.release(unstarted.ReceiptHandle)
					}
				}
				return
			}
			for _, message := range group {
				consumer.register(message.ReceiptHandle)
			}
			batch.Add(1)
			consumer.handlerWG.Add(1)
			go func(group []Message) {
				defer batch.Done()
				defer consumer.handlerWG.Done()
				for index, message := range group {
					if handlerContext.Err() != nil {
						for _, unstarted := range group[index:] {
							consumer.unregister(unstarted.ReceiptHandle)
							consumer.release(unstarted.ReceiptHandle)
						}
						return
					}
					if !consumer.handle(handlerContext, message) {
						for _, unstarted := range group[index+1:] {
							consumer.unregister(unstarted.ReceiptHandle)
							consumer.release(unstarted.ReceiptHandle)
						}
						return
					}
				}
			}(group)
		}
		batch.Wait()
	}
}

func groupMessages(messages []Message) [][]Message {
	groups := make([][]Message, 0, len(messages))
	groupIndexes := make(map[string]int, len(messages))
	for _, message := range messages {
		index, exists := groupIndexes[message.MessageGroupID]
		if !exists {
			index = len(groups)
			groupIndexes[message.MessageGroupID] = index
			groups = append(groups, nil)
		}
		groups[index] = append(groups[index], message)
	}
	return groups
}

func (consumer *Consumer) handle(ctx context.Context, message Message) bool {
	consumer.register(message.ReceiptHandle)
	defer consumer.unregister(message.ReceiptHandle)

	if message.ReceiveCount > 1 && consumer.metrics != nil {
		consumer.metrics.RecordDuplicate("sqs-message")
	}
	parsed, err := ParseWagerTransactionRequested([]byte(message.Body))
	if err != nil {
		consumer.permanentFailure(ctx, message, "INVALID_MESSAGE")
		return false
	}
	ctx = observability.WithIdentifiers(ctx, observability.Identifiers{
		CorrelationID: parsed.MessageID, MessageID: parsed.MessageID,
		WalletID: parsed.Command.WalletID(), ProviderID: parsed.Command.ProviderID(),
	})
	observability.EnrichLogger(ctx, consumer.logger).DebugContext(ctx, "SQS message received", slog.Int("receiveCount", message.ReceiveCount))
	startedAt := time.Now()
	metricStatus := "ERROR"
	defer func() {
		if consumer.metrics != nil {
			consumer.metrics.ObserveProcessingLatency(string(parsed.Command.Kind()), metricStatus, time.Since(startedAt))
		}
	}()
	result, err := consumer.processor.ProcessTransaction(ctx, application.ProcessTransactionRequest{
		Command: parsed.Command, IdempotencyKey: parsed.IdempotencyKey,
		CorrelationID: parsed.MessageID, CausationID: parsed.MessageID,
		Inbox: &application.InboxMessage{ConsumerName: consumer.options.ConsumerName, MessageID: parsed.MessageID, PayloadHash: parsed.PayloadHash},
	})
	if err != nil {
		if isPermanentConsumerError(err) {
			consumer.permanentFailure(ctx, message, "PERMANENT_CONFLICT")
			return false
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			consumer.release(message.ReceiptHandle)
			return false
		}
		consumer.recordRetry("transient")
		observability.Log(ctx, consumer.logger, slog.LevelWarn, "transient SQS processing failure")
		consumer.recordDLQThreshold(ctx, message, "TRANSIENT_FAILURE")
		_ = consumer.broker.ChangeVisibility(ctx, consumer.options.QueueURL, message.ReceiptHandle, consumer.backoff(message.ReceiveCount))
		return false
	}

	if result.Transaction != nil {
		metricStatus = string(result.Transaction.State())
		ctx = observability.WithIdentifiers(ctx, observability.Identifiers{TransactionID: result.Transaction.ID()})
	}
	consumer.recordResult(result)
	if hook := consumer.options.AfterCommitBeforeDelete; hook != nil {
		if err := hook(ctx); err != nil {
			consumer.recordRetry("post_commit_interrupted")
			return false
		}
	}
	if err := consumer.broker.Delete(ctx, consumer.options.QueueURL, message.ReceiptHandle); err != nil {
		consumer.recordRetry("delete_error")
		_ = consumer.broker.ChangeVisibility(ctx, consumer.options.QueueURL, message.ReceiptHandle, consumer.backoff(message.ReceiveCount))
		return false
	}
	observability.Log(ctx, consumer.logger, slog.LevelInfo, "SQS message processed and deleted")
	return true
}

func (consumer *Consumer) permanentFailure(ctx context.Context, message Message, code string) {
	if consumer.recordDLQThreshold(ctx, message, code) {
		return
	}
	observability.EnrichLogger(ctx, consumer.logger).Log(ctx, slog.LevelWarn, "permanent SQS processing failure",
		slog.Int("receiveCount", message.ReceiveCount), slog.String("code", code))
}

func (consumer *Consumer) recordDLQThreshold(ctx context.Context, message Message, code string) bool {
	if message.ReceiveCount < consumer.options.MaxReceiveCount {
		return false
	}
	if consumer.metrics != nil {
		consumer.metrics.RecordDLQ(consumerWorkerName, code)
	}
	observability.EnrichLogger(ctx, consumer.logger).Log(ctx, slog.LevelWarn, "SQS message reached broker redrive threshold",
		slog.Int("receiveCount", message.ReceiveCount), slog.String("code", code))
	return true
}

func (consumer *Consumer) recordResult(result application.TransactionResult) {
	if result.Transaction == nil || consumer.metrics == nil {
		return
	}
	details := result.Transaction.Details()
	code := string(result.Transaction.FailureCode())
	consumer.metrics.RecordTransactionResult(string(details.Kind), string(result.Transaction.State()), code)
	if result.IdempotentReplay {
		consumer.metrics.RecordDuplicate("transaction")
	}
}

func (consumer *Consumer) recordRetry(result string) {
	if consumer.metrics != nil {
		consumer.metrics.RecordRetry(consumerWorkerName, result)
	}
}

func (consumer *Consumer) backoff(receiveCount int) time.Duration {
	delay := consumer.options.BackoffBase
	for attempt := 1; attempt < receiveCount && delay < consumer.options.BackoffMaximum; attempt++ {
		if delay > consumer.options.BackoffMaximum/2 {
			return consumer.options.BackoffMaximum
		}
		delay *= 2
	}
	if delay > consumer.options.BackoffMaximum {
		return consumer.options.BackoffMaximum
	}
	return delay
}

func isPermanentConsumerError(err error) bool {
	return errors.Is(err, ErrInvalidEnvelope) || errors.Is(err, application.ErrInvalidRequest) ||
		errors.Is(err, application.ErrInvalidTransactionCommand) || errors.Is(err, postgres.ErrInboxHashConflict) ||
		errors.Is(err, postgres.ErrIdempotencyConflict) || errors.Is(err, postgres.ErrExternalIDConflict) ||
		errors.Is(err, domain.ErrInvalidTransaction) || errors.Is(err, domain.ErrInvalidMoney)
}

func (consumer *Consumer) register(receipt string) {
	consumer.mu.Lock()
	consumer.active[receipt] = struct{}{}
	consumer.mu.Unlock()
}

func (consumer *Consumer) unregister(receipt string) {
	consumer.mu.Lock()
	delete(consumer.active, receipt)
	consumer.mu.Unlock()
}

func (consumer *Consumer) release(receipt string) {
	releaseContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = consumer.broker.ChangeVisibility(releaseContext, consumer.options.QueueURL, receipt, 0)
}

func (consumer *Consumer) Shutdown(ctx context.Context) error {
	consumer.mu.Lock()
	if !consumer.started {
		consumer.mu.Unlock()
		return nil
	}
	consumer.started = false
	pollCancel := consumer.pollCancel
	handlerCancel := consumer.handlerCancel
	consumer.mu.Unlock()

	pollCancel()
	done := make(chan struct{})
	go func() {
		consumer.pollWG.Wait()
		consumer.handlerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		handlerCancel()
		consumer.releaseActive()
		return nil
	case <-ctx.Done():
		handlerCancel()
		consumer.releaseActive()
		return fmt.Errorf("messaging: consumer shutdown: %w", ctx.Err())
	}
}

func (consumer *Consumer) releaseActive() {
	consumer.mu.Lock()
	receipts := make([]string, 0, len(consumer.active))
	for receipt := range consumer.active {
		receipts = append(receipts, receipt)
	}
	consumer.mu.Unlock()
	for _, receipt := range receipts {
		consumer.release(receipt)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
