package messaging

import (
	"context"
	"time"
)

// Message contains only the SQS fields required by the consumer.
type Message struct {
	Body           string
	ReceiptHandle  string
	ReceiveCount   int
	MessageGroupID string
}

// Broker isolates worker logic from the AWS SDK and keeps it easy to test.
type Broker interface {
	Receive(context.Context, string, int, time.Duration, time.Duration) ([]Message, error)
	Delete(context.Context, string, string) error
	ChangeVisibility(context.Context, string, string, time.Duration) error
	Publish(context.Context, string, []byte, string, string) error
}

type ConsumerMetrics interface {
	RecordDuplicate(string)
	RecordRetry(string, string)
	RecordDLQ(string, string)
	RecordTransactionResult(string, string, string)
	ObserveProcessingLatency(string, string, time.Duration)
}

type OutboxMetrics interface {
	ObserveOutboxLag(time.Duration)
	RecordOutboxPublication(string)
	RecordRetry(string, string)
}

type WorkerMetrics interface {
	RecordRetry(string, string)
	RecordTransactionResult(string, string, string)
	ObserveProcessingLatency(string, string, time.Duration)
}
