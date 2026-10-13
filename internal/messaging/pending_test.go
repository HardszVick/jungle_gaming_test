package messaging

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type fakePendingService struct {
	mu        sync.Mutex
	claims    map[string][]postgres.PendingReferenceClaim
	resumed   map[string]string
	resumeHit chan struct{}
	result    application.TransactionResult
}

func (fake *fakePendingService) ClaimPendingReferences(_ context.Context, workerID string, _ int, _ time.Duration) ([]postgres.PendingReferenceClaim, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	claims := fake.claims[workerID]
	delete(fake.claims, workerID)
	return claims, nil
}

func (fake *fakePendingService) ResumePendingReference(_ context.Context, transactionID, workerID string) (application.TransactionResult, error) {
	fake.mu.Lock()
	fake.resumed[transactionID] = workerID
	fake.mu.Unlock()
	select {
	case fake.resumeHit <- struct{}{}:
	default:
	}
	return fake.result, nil
}

func TestPendingWorkersUseIndependentClaims(t *testing.T) {
	service := &fakePendingService{
		claims: map[string][]postgres.PendingReferenceClaim{
			"worker-a": {{Record: postgres.TransactionRecord{Transaction: pendingTransaction(t, "transaction-a")}}},
			"worker-b": {{Record: postgres.TransactionRecord{Transaction: pendingTransaction(t, "transaction-b")}}},
		},
		resumed: make(map[string]string), resumeHit: make(chan struct{}, 2),
	}
	workerA := newTestPendingWorker(t, service, "worker-a")
	workerB := newTestPendingWorker(t, service, "worker-b")
	if err := workerA.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := workerB.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-service.resumeHit
	<-service.resumeHit
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := workerA.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := workerB.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.resumed["transaction-a"] != "worker-a" || service.resumed["transaction-b"] != "worker-b" {
		t.Fatalf("claims crossed workers: %v", service.resumed)
	}
}

func TestPendingWorkerAppliesJitterAndStops(t *testing.T) {
	service := &fakePendingService{claims: make(map[string][]postgres.PendingReferenceClaim), resumed: make(map[string]string), resumeHit: make(chan struct{}, 1)}
	jittered := make(chan time.Duration, 1)
	worker, err := NewPendingWorker(service, &fakeMetrics{}, nil, PendingWorkerOptions{
		WorkerID: "worker", BatchSize: 2, PollInterval: time.Hour, Lease: time.Minute,
		Jitter: func(delay time.Duration) time.Duration { jittered <- delay; return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := <-jittered; got != time.Hour {
		t.Fatalf("unexpected jitter input: %v", got)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestPendingWorkerRecordsTerminalResult(t *testing.T) {
	transaction := pendingTransaction(t, "transaction-result")
	balance, err := domain.ParseMoneyValue("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.MarkProcessed(balance, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	service := &fakePendingService{
		claims: map[string][]postgres.PendingReferenceClaim{
			"worker": {{Record: postgres.TransactionRecord{Transaction: transaction}}},
		},
		resumed: make(map[string]string), resumeHit: make(chan struct{}, 1),
		result: application.TransactionResult{Transaction: transaction},
	}
	metrics := &fakeMetrics{}
	worker, err := NewPendingWorker(service, metrics, nil, PendingWorkerOptions{
		WorkerID: "worker", BatchSize: 1, PollInterval: time.Hour, Lease: time.Minute,
		Jitter: func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-service.resumeHit
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.results != 1 || len(metrics.latencies) != 1 || metrics.latencies[0].status != string(domain.TransactionStateProcessed) {
		t.Fatalf("pending metrics = results %d, latencies %+v", metrics.results, metrics.latencies)
	}
}

func newTestPendingWorker(t *testing.T, service PendingReferenceService, id string) *PendingWorker {
	t.Helper()
	worker, err := NewPendingWorker(service, &fakeMetrics{}, nil, PendingWorkerOptions{
		WorkerID: id, BatchSize: 1, PollInterval: time.Hour, Lease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func pendingTransaction(t *testing.T, id string) *domain.WagerTransaction {
	t.Helper()
	money, err := domain.ParseMoneyValue("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	transaction, err := domain.NewWagerTransaction(id, domain.WagerTransactionDetails{
		ProviderID: "provider", ExternalTransactionID: id + "-external", PlayerID: "player",
		WalletID: "wallet", RoundID: "round", GameID: "game", Kind: domain.TransactionKindRefund,
		Money: money, ReferenceExternalTransactionID: "reference",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.MarkPendingReference(now); err != nil {
		t.Fatal(err)
	}
	return transaction
}
