package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type fakeBroker struct {
	mu           sync.Mutex
	operations   []string
	visibility   []time.Duration
	visible      []string
	receive      func(context.Context) ([]Message, error)
	deleteErr    error
	publishErr   error
	publications []publication
}

type publication struct {
	body, group, dedup string
}

func (fake *fakeBroker) Receive(ctx context.Context, _ string, _ int, _, _ time.Duration) ([]Message, error) {
	if fake.receive != nil {
		return fake.receive(ctx)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (fake *fakeBroker) Delete(context.Context, string, string) error {
	fake.mu.Lock()
	fake.operations = append(fake.operations, "delete")
	fake.mu.Unlock()
	return fake.deleteErr
}

func (fake *fakeBroker) ChangeVisibility(_ context.Context, _, receipt string, visibility time.Duration) error {
	fake.mu.Lock()
	fake.operations = append(fake.operations, "visibility")
	fake.visibility = append(fake.visibility, visibility)
	fake.visible = append(fake.visible, receipt)
	fake.mu.Unlock()
	return nil
}

func (fake *fakeBroker) Publish(_ context.Context, _ string, body []byte, group, dedup string) error {
	fake.mu.Lock()
	fake.operations = append(fake.operations, "publish")
	fake.publications = append(fake.publications, publication{string(body), group, dedup})
	fake.mu.Unlock()
	return fake.publishErr
}

type processorFunc func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error)

func (function processorFunc) ProcessTransaction(ctx context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
	return function(ctx, request)
}

type fakeMetrics struct {
	mu           sync.Mutex
	retries      []string
	dlq          []string
	duplicates   int
	results      int
	latencies    []consumerLatency
	publications []string
}

type consumerLatency struct {
	kind, status string
	value        time.Duration
}

func (metrics *fakeMetrics) RecordDuplicate(string) {
	metrics.mu.Lock()
	metrics.duplicates++
	metrics.mu.Unlock()
}
func (metrics *fakeMetrics) RecordRetry(_, result string) {
	metrics.mu.Lock()
	metrics.retries = append(metrics.retries, result)
	metrics.mu.Unlock()
}
func (metrics *fakeMetrics) RecordDLQ(_, code string) {
	metrics.mu.Lock()
	metrics.dlq = append(metrics.dlq, code)
	metrics.mu.Unlock()
}
func (metrics *fakeMetrics) RecordTransactionResult(_, _, _ string) {
	metrics.mu.Lock()
	metrics.results++
	metrics.mu.Unlock()
}
func (metrics *fakeMetrics) ObserveProcessingLatency(kind, status string, value time.Duration) {
	metrics.mu.Lock()
	metrics.latencies = append(metrics.latencies, consumerLatency{kind: kind, status: status, value: value})
	metrics.mu.Unlock()
}
func (metrics *fakeMetrics) ObserveOutboxLag(time.Duration) {}
func (metrics *fakeMetrics) RecordOutboxPublication(result string) {
	metrics.mu.Lock()
	metrics.publications = append(metrics.publications, result)
	metrics.mu.Unlock()
}

func TestConsumerDeletesOnlyAfterCommittedResult(t *testing.T) {
	broker := &fakeBroker{}
	processor := processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		broker.mu.Lock()
		broker.operations = append(broker.operations, "commit")
		broker.mu.Unlock()
		if request.Inbox == nil || request.Inbox.MessageID != "message-1" || request.Inbox.ConsumerName != "consumer-a" {
			t.Fatalf("inbox was not passed to application service")
		}
		return application.TransactionResult{}, nil
	})
	consumer := newTestConsumer(t, broker, processor, &fakeMetrics{})
	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 1})
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.operations) != 2 || broker.operations[0] != "commit" || broker.operations[1] != "delete" {
		t.Fatalf("unexpected operation order: %v", broker.operations)
	}
}

func TestConsumerLogsSuccessfulProcessingAndDeletionWithoutFinancialPayload(t *testing.T) {
	var output bytes.Buffer
	logger := observability.NewJSONLogger(&output, slog.LevelInfo)
	transaction := pendingTransaction(t, "transaction-1")
	consumer := newTestConsumerWithLogger(t, &fakeBroker{}, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{Transaction: transaction}, nil
	}), &fakeMetrics{}, logger)

	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 1})

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode success log: %v\n%s", err, output.String())
	}
	want := map[string]string{
		"level":         "INFO",
		"msg":           "SQS message processed and deleted",
		"correlationId": "message-1",
		"messageId":     "message-1",
		"transactionId": "transaction-1",
		"walletId":      testWalletID,
		"providerId":    "provider-a",
	}
	for key, value := range want {
		if record[key] != value {
			t.Errorf("success log field %q = %v, want %q", key, record[key], value)
		}
	}
	for _, sensitive := range []string{"25.00", "BRL", "idem-1", "external-1", testPlayerID, "round-1", "game-1"} {
		if bytes.Contains(output.Bytes(), []byte(sensitive)) {
			t.Errorf("success log contains sensitive payload value %q", sensitive)
		}
	}
}

func TestConsumerTransientErrorChangesVisibilityWithBackoff(t *testing.T) {
	broker := &fakeBroker{}
	metrics := &fakeMetrics{}
	consumer := newTestConsumer(t, broker, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{}, errors.New("database unavailable")
	}), metrics)
	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 3})
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.visibility) != 1 || broker.visibility[0] != 4*time.Second {
		t.Fatalf("unexpected visibility backoff: %v", broker.visibility)
	}
	if contains(broker.operations, "delete") {
		t.Fatalf("transient failure was deleted")
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if len(metrics.latencies) != 1 || metrics.latencies[0].kind != "WIN" || metrics.latencies[0].status != "ERROR" || metrics.latencies[0].value < 0 {
		t.Fatalf("unexpected processing latency: %+v", metrics.latencies)
	}
}

func TestConsumerTransientErrorAtMaxReceiveCountRecordsDLQThreshold(t *testing.T) {
	var output bytes.Buffer
	metrics := &fakeMetrics{}
	consumer := newTestConsumerWithLogger(t, &fakeBroker{}, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{}, errors.New("database unavailable")
	}), metrics, observability.NewJSONLogger(&output, slog.LevelInfo))
	consumer.options.MaxReceiveCount = 3

	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 3})

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if len(metrics.dlq) != 1 || metrics.dlq[0] != "TRANSIENT_FAILURE" {
		t.Fatalf("DLQ threshold was not tracked: %v", metrics.dlq)
	}
	if !strings.Contains(output.String(), "SQS message reached broker redrive threshold") ||
		!strings.Contains(output.String(), `"code":"TRANSIENT_FAILURE"`) {
		t.Fatalf("DLQ threshold log missing: %s", output.String())
	}
}

func TestConsumerPermanentErrorsUseBrokerRedrive(t *testing.T) {
	broker := &fakeBroker{}
	metrics := &fakeMetrics{}
	consumer := newTestConsumer(t, broker, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{}, postgres.ErrInboxHashConflict
	}), metrics)
	consumer.options.MaxReceiveCount = 2
	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 2})
	broker.mu.Lock()
	if len(broker.operations) != 0 {
		t.Fatalf("permanent error must not delete or change visibility: %v", broker.operations)
	}
	broker.mu.Unlock()
	if len(metrics.dlq) != 1 || metrics.dlq[0] != "PERMANENT_CONFLICT" {
		t.Fatalf("DLQ threshold was not tracked: %v", metrics.dlq)
	}
}

func TestConsumerDoesNotClaimBrokerRedriveBeforeThreshold(t *testing.T) {
	var output bytes.Buffer
	metrics := &fakeMetrics{}
	consumer := newTestConsumerWithLogger(t, &fakeBroker{}, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{}, postgres.ErrInboxHashConflict
	}), metrics, observability.NewJSONLogger(&output, slog.LevelInfo))
	consumer.options.MaxReceiveCount = 2

	consumer.handle(context.Background(), Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 1})

	if len(metrics.dlq) != 0 {
		t.Fatalf("DLQ recorded before threshold: %v", metrics.dlq)
	}
	if strings.Contains(output.String(), "redrive") {
		t.Fatalf("log claimed broker redrive before threshold: %s", output.String())
	}
}

func TestConsumerDuplicateMessageIdentityIsStableAndDeleted(t *testing.T) {
	broker := &fakeBroker{}
	var first [32]byte
	calls := 0
	consumer := newTestConsumer(t, broker, processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		calls++
		if calls == 1 {
			first = request.Inbox.PayloadHash
		} else if request.Inbox.PayloadHash != first || request.Inbox.MessageID != "message-1" {
			t.Fatalf("duplicate inbox identity changed")
		}
		return application.TransactionResult{IdempotentReplay: calls > 1}, nil
	}), &fakeMetrics{})
	message := Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 2}
	consumer.handle(context.Background(), message)
	consumer.handle(context.Background(), message)
	broker.mu.Lock()
	deletes := 0
	for _, operation := range broker.operations {
		if operation == "delete" {
			deletes++
		}
	}
	broker.mu.Unlock()
	if calls != 2 || deletes != 2 {
		t.Fatalf("calls=%d deletes=%d", calls, deletes)
	}
}

func TestConsumerRetriesCommitFailureAndDeletesDurableFailedReplay(t *testing.T) {
	broker := &fakeBroker{}
	failed := pendingTransaction(t, "failed-transaction")
	if err := failed.Fail(failed.UpdatedAt().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	committed := false
	calls := 0
	consumer := newTestConsumer(t, broker, processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		calls++
		if calls == 1 {
			return application.TransactionResult{}, errors.New("commit failed")
		}
		replay := committed
		committed = true
		return application.TransactionResult{Transaction: failed, IdempotentReplay: replay}, nil
	}), &fakeMetrics{})
	message := Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 1}

	consumer.handle(context.Background(), message)
	if committed {
		t.Fatal("failed commit was treated as durable")
	}
	consumer.handle(context.Background(), message)

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if calls != 2 || !committed || !contains(broker.operations, "visibility") || !contains(broker.operations, "delete") {
		t.Fatalf("calls=%d committed=%t operations=%v", calls, committed, broker.operations)
	}
}

func TestConsumerPostCommitInterruptionReplaysFailedWithoutMovement(t *testing.T) {
	broker := &fakeBroker{}
	failed := pendingTransaction(t, "failed-replay")
	if err := failed.Fail(failed.UpdatedAt().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	committed := false
	terminalizations := 0
	movements := 0
	var replays []bool
	processor := processorFunc(func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		replay := committed
		replays = append(replays, replay)
		if !committed {
			committed = true
			terminalizations++
		}
		return application.TransactionResult{Transaction: failed, IdempotentReplay: replay}, nil
	})
	consumer := newTestConsumer(t, broker, processor, &fakeMetrics{})
	interrupt := true
	consumer.options.AfterCommitBeforeDelete = func(context.Context) error {
		if interrupt {
			interrupt = false
			return errors.New("consumer stopped after commit")
		}
		return nil
	}
	message := Message{Body: validEnvelope("WIN"), ReceiptHandle: "receipt", ReceiveCount: 1}

	consumer.handle(context.Background(), message)
	consumer.handle(context.Background(), message)

	broker.mu.Lock()
	defer broker.mu.Unlock()
	deletes := 0
	for _, operation := range broker.operations {
		if operation == "delete" {
			deletes++
		}
	}
	if !committed || terminalizations != 1 || movements != 0 || deletes != 1 ||
		len(replays) != 2 || replays[0] || !replays[1] {
		t.Fatalf("committed=%t terminalizations=%d movements=%d replays=%v deletes=%d operations=%v",
			committed, terminalizations, movements, replays, deletes, broker.operations)
	}
}

func TestConsumerShutdownReleasesUncommittedMessage(t *testing.T) {
	delivered := make(chan struct{})
	var once sync.Once
	broker := &fakeBroker{}
	broker.receive = func(ctx context.Context) ([]Message, error) {
		var messages []Message
		once.Do(func() {
			messages = []Message{
				{Body: validEnvelope("WIN"), ReceiptHandle: "active", ReceiveCount: 1, MessageGroupID: "wallet-a"},
				{Body: validEnvelope("WIN"), ReceiptHandle: "queued", ReceiveCount: 1, MessageGroupID: "wallet-a"},
			}
		})
		if messages != nil {
			return messages, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	processor := processorFunc(func(ctx context.Context, _ application.ProcessTransactionRequest) (application.TransactionResult, error) {
		close(delivered)
		<-ctx.Done()
		return application.TransactionResult{}, ctx.Err()
	})
	consumer := newTestConsumer(t, broker, processor, &fakeMetrics{})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-delivered
	shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := consumer.Shutdown(shutdownContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected shutdown deadline, got %v", err)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	zeroVisibilityChanges := 0
	for _, visibility := range broker.visibility {
		if visibility == 0 {
			zeroVisibilityChanges++
		}
	}
	if zeroVisibilityChanges < 2 || contains(broker.operations, "delete") {
		t.Fatalf("active and queued same-group messages were not safely released: %v", broker.visibility)
	}
}

func TestConsumerProcessesSameMessageGroupInBatchSequentially(t *testing.T) {
	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseFirst) }) }

	broker := batchBroker([]Message{
		{Body: envelopeWithMessageID("first"), ReceiptHandle: "first", ReceiveCount: 1, MessageGroupID: "wallet-a"},
		{Body: envelopeWithMessageID("second"), ReceiptHandle: "second", ReceiveCount: 1, MessageGroupID: "wallet-a"},
	})
	consumer := newTestConsumer(t, broker, processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		started <- request.Inbox.MessageID
		if request.Inbox.MessageID == "first" {
			<-releaseFirst
		}
		return application.TransactionResult{}, nil
	}), &fakeMetrics{})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer shutdownTestConsumer(consumer)
	defer unblock()

	if got := receiveWithin(t, started); got != "first" {
		t.Fatalf("first started message = %q", got)
	}
	select {
	case got := <-started:
		t.Fatalf("same-group message %q started before first completed", got)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	if got := receiveWithin(t, started); got != "second" {
		t.Fatalf("second started message = %q", got)
	}
}

func TestConsumerStopsMessageGroupAfterTransientFailure(t *testing.T) {
	started := make(chan string, 2)
	broker := batchBroker([]Message{
		{Body: envelopeWithMessageID("first"), ReceiptHandle: "first", ReceiveCount: 1, MessageGroupID: "wallet-a"},
		{Body: envelopeWithMessageID("second"), ReceiptHandle: "second", ReceiveCount: 1, MessageGroupID: "wallet-a"},
	})
	consumer := newTestConsumer(t, broker, processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		started <- request.Inbox.MessageID
		return application.TransactionResult{}, errors.New("database unavailable")
	}), &fakeMetrics{})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer shutdownTestConsumer(consumer)

	if got := receiveWithin(t, started); got != "first" {
		t.Fatalf("first started message = %q", got)
	}
	waitForVisibility(t, broker, "second", 0)
	assertNotStarted(t, started)
}

func TestConsumerStopsMessageGroupAfterPermanentFailure(t *testing.T) {
	started := make(chan string, 2)
	broker := batchBroker([]Message{
		{Body: envelopeWithMessageID("first"), ReceiptHandle: "first", ReceiveCount: 1, MessageGroupID: "wallet-a"},
		{Body: envelopeWithMessageID("second"), ReceiptHandle: "second", ReceiveCount: 1, MessageGroupID: "wallet-a"},
	})
	consumer := newTestConsumer(t, broker, processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		started <- request.Inbox.MessageID
		return application.TransactionResult{}, postgres.ErrInboxHashConflict
	}), &fakeMetrics{})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer shutdownTestConsumer(consumer)

	if got := receiveWithin(t, started); got != "first" {
		t.Fatalf("first started message = %q", got)
	}
	waitForVisibility(t, broker, "second", 0)
	assertNotStarted(t, started)
}

func TestConsumerProcessesDifferentMessageGroupsInBatchConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }

	broker := batchBroker([]Message{
		{Body: envelopeWithMessageID("first"), ReceiptHandle: "first", ReceiveCount: 1, MessageGroupID: "wallet-a"},
		{Body: envelopeWithMessageID("second"), ReceiptHandle: "second", ReceiveCount: 1, MessageGroupID: "wallet-b"},
	})
	consumer := newTestConsumer(t, broker, processorFunc(func(_ context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
		started <- request.Inbox.MessageID
		<-release
		return application.TransactionResult{}, nil
	}), &fakeMetrics{})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer shutdownTestConsumer(consumer)
	defer unblock()

	seen := map[string]bool{receiveWithin(t, started): true, receiveWithin(t, started): true}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("started messages = %v", seen)
	}
	unblock()
}

func batchBroker(messages []Message) *fakeBroker {
	broker := &fakeBroker{}
	var once sync.Once
	broker.receive = func(ctx context.Context) ([]Message, error) {
		var batch []Message
		once.Do(func() { batch = messages })
		if batch != nil {
			return batch, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return broker
}

func envelopeWithMessageID(messageID string) string {
	return strings.Replace(validEnvelope("WIN"), "message-1", messageID, 1)
}

func receiveWithin(t *testing.T, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
		return ""
	}
}

func waitForVisibility(t *testing.T, broker *fakeBroker, receipt string, visibility time.Duration) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		broker.mu.Lock()
		for index, visibleReceipt := range broker.visible {
			if visibleReceipt == receipt && broker.visibility[index] == visibility {
				broker.mu.Unlock()
				return
			}
		}
		broker.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("receipt %q was not released with visibility %s", receipt, visibility)
}

func assertNotStarted(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case messageID := <-started:
		t.Fatalf("same-group message %q started after first message failed", messageID)
	case <-time.After(50 * time.Millisecond):
	}
}

func shutdownTestConsumer(consumer *Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = consumer.Shutdown(ctx)
}

func newTestConsumer(t *testing.T, broker Broker, processor TransactionProcessor, metrics ConsumerMetrics) *Consumer {
	return newTestConsumerWithLogger(t, broker, processor, metrics, nil)
}

func newTestConsumerWithLogger(t *testing.T, broker Broker, processor TransactionProcessor, metrics ConsumerMetrics, logger *slog.Logger) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(broker, processor, metrics, logger, ConsumerOptions{
		QueueURL: "input", ConsumerName: "consumer-a", BatchSize: 10,
		WaitTime: time.Second, Visibility: time.Minute, ErrorDelay: time.Millisecond,
		BackoffBase: time.Second, BackoffMaximum: 8 * time.Second, MaxReceiveCount: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
