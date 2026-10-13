package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fakeOutboxRepository struct {
	mu          sync.Mutex
	claims      []postgres.OutboxClaim
	claimCalls  int
	marked      []string
	rescheduled []reschedule
	markErr     error
}

type reschedule struct {
	eventID, reason string
	delay           time.Duration
}

func (fake *fakeOutboxRepository) ClaimOutbox(context.Context, string, int, time.Duration) ([]postgres.OutboxClaim, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.claimCalls++
	claims := fake.claims
	fake.claims = nil
	return claims, nil
}
func (fake *fakeOutboxRepository) MarkOutboxPublished(_ context.Context, eventID, _ string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.marked = append(fake.marked, eventID)
	return fake.markErr
}
func (fake *fakeOutboxRepository) RescheduleOutbox(_ context.Context, eventID, _ string, delay time.Duration, reason string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.rescheduled = append(fake.rescheduled, reschedule{eventID, reason, delay})
	return nil
}

func TestOutboxPublishesFIFOThenMarks(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	repository := &fakeOutboxRepository{}
	broker := &fakeBroker{}
	publisher := newTestPublisher(t, repository, broker, fixedClock{now: now})
	claim := outboxClaim(now)
	publisher.publish(context.Background(), claim)
	broker.mu.Lock()
	if len(broker.publications) != 1 || broker.publications[0].group != "aggregate-1" || broker.publications[0].dedup != "event-1" {
		t.Fatalf("unexpected FIFO publication: %+v", broker.publications)
	}
	broker.mu.Unlock()
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.marked) != 1 || repository.marked[0] != "event-1" || len(repository.rescheduled) != 0 {
		t.Fatalf("unexpected repository state: marked=%v rescheduled=%v", repository.marked, repository.rescheduled)
	}
}

func TestOutboxFailureReschedulesWithSanitizedBackoff(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	repository := &fakeOutboxRepository{}
	broker := &fakeBroker{publishErr: errors.New("https://secret.invalid?token=credential")}
	publisher := newTestPublisher(t, repository, broker, fixedClock{now: now})
	claim := outboxClaim(now)
	claim.AttemptCount = 2
	publisher.publish(context.Background(), claim)
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.marked) != 0 || len(repository.rescheduled) != 1 {
		t.Fatalf("unexpected outbox result")
	}
	got := repository.rescheduled[0]
	if got.reason != "SQS publication failed" || got.delay != 4*time.Second {
		t.Fatalf("error was not sanitized/backed off: %+v", got)
	}
}

func TestOutboxMarkFailureRepublishesStableEventID(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	repository := &fakeOutboxRepository{markErr: postgres.ErrClaimLost}
	broker := &fakeBroker{}
	publisher := newTestPublisher(t, repository, broker, fixedClock{now: now})
	claim := outboxClaim(now)
	publisher.publish(context.Background(), claim)
	publisher.publish(context.Background(), claim)
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.publications) != 2 || broker.publications[0].dedup != "event-1" || broker.publications[1].dedup != "event-1" {
		t.Fatalf("event identity changed across ambiguous publish: %+v", broker.publications)
	}
}

func TestOutboxLifecycleClaimsOnceAndStops(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	repository := &fakeOutboxRepository{claims: []postgres.OutboxClaim{outboxClaim(now)}}
	broker := &fakeBroker{}
	publisher := newTestPublisher(t, repository, broker, fixedClock{now: now})
	if err := publisher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		broker.mu.Lock()
		published := len(broker.publications)
		broker.mu.Unlock()
		if published == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("publisher did not publish claim")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := publisher.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
}

func newTestPublisher(t *testing.T, repository OutboxRepository, broker Broker, clock Clock) *OutboxPublisher {
	t.Helper()
	publisher, err := NewOutboxPublisher(repository, broker, clock, &fakeMetrics{}, nil, OutboxOptions{
		PublisherID: "publisher-a", QueueURL: "events", BatchSize: 10,
		PollInterval: time.Hour, Lease: time.Minute, BackoffBase: time.Second, BackoffMaximum: 8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func outboxClaim(now time.Time) postgres.OutboxClaim {
	return postgres.OutboxClaim{
		EventID: "event-1", AggregateID: "aggregate-1", Envelope: json.RawMessage(`{"eventId":"event-1"}`),
		ClaimedBy: "publisher-a", CreatedAt: now.Add(-time.Minute),
	}
}
