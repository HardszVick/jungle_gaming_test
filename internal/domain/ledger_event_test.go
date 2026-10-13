package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestWalletLedgerEntryValidatesBalanceEquation(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Now().UTC()
	tests := []struct {
		name      string
		direction LedgerDirection
		before    string
		amount    string
		after     string
	}{
		{"credit", LedgerDirectionCredit, "10.00", "2.50", "12.50"},
		{"debit", LedgerDirectionDebit, "10.00", "2.50", "7.50"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry, err := NewWalletLedgerEntry(
				"entry-1", "wallet-1", "tx-1", test.direction,
				testMoney(t, test.amount, brl), testMoney(t, test.before, brl),
				testMoney(t, test.after, brl), now,
			)
			if err != nil {
				t.Fatal(err)
			}
			if entry.Direction() != test.direction || entry.BalanceAfter().String() != test.after {
				t.Fatalf("unexpected entry: direction=%s after=%s", entry.Direction(), entry.BalanceAfter())
			}
		})
	}

	_, err := NewWalletLedgerEntry(
		"entry-1", "wallet-1", "tx-1", LedgerDirectionCredit,
		testMoney(t, "2.50", brl), testMoney(t, "10.00", brl), testMoney(t, "12.49", brl), now,
	)
	if !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Fatalf("inconsistent equation error = %v", err)
	}
	_, err = NewWalletLedgerEntry(
		"entry-1", "wallet-1", "tx-1", LedgerDirectionDebit,
		testMoney(t, "0.00", brl), testMoney(t, "10.00", brl), testMoney(t, "10.00", brl), now,
	)
	if !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Fatalf("zero entry error = %v", err)
	}
}

func TestTypedEventsAndGoldenSerialization(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Date(2026, 7, 8, 12, 34, 56, 0, time.FixedZone("BRT", -3*60*60))
	entry, err := NewWalletLedgerEntry(
		"entry-1", "wallet-1", "tx-1", LedgerDirectionDebit,
		testMoney(t, "10.00", brl), testMoney(t, "100.00", brl), testMoney(t, "90.00", brl), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	data, err := NewWalletBalanceChangedData(entry, 2)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewWalletBalanceChangedEvent("event-1", "correlation-1", "cause-1", now, data)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"eventId":"event-1","eventType":"WalletBalanceChanged","aggregateId":"wallet-1","correlationId":"correlation-1","causationId":"cause-1","occurredAt":"2026-07-08T15:34:56Z","version":1,"data":{"walletId":"wallet-1","transactionId":"tx-1","direction":"DEBIT","money":{"amount":"10.00","currency":"BRL"},"balanceBefore":{"amount":"100.00","currency":"BRL"},"balanceAfter":{"amount":"90.00","currency":"BRL"},"walletVersion":2}}`
	if string(encoded) != want {
		t.Fatalf("event JSON:\n got %s\nwant %s", encoded, want)
	}
	if event.OccurredAt().Location() != time.UTC || event.Version() != DomainEventVersion {
		t.Fatalf("event time/version = %v/%d", event.OccurredAt(), event.Version())
	}
}

func TestWalletBalanceChangedAllowsOpeningVersion(t *testing.T) {
	brl := testCurrency(t, "BRL")
	now := time.Now().UTC()
	entry, err := NewWalletLedgerEntry(
		"entry-opening", "wallet-1", "tx-opening", LedgerDirectionCredit,
		testMoney(t, "100.00", brl), testMoney(t, "0.00", brl), testMoney(t, "100.00", brl), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewWalletBalanceChangedData(entry, 1); err != nil {
		t.Fatalf("opening wallet version must be accepted: %v", err)
	}
}

func TestAllTransactionEventConstructors(t *testing.T) {
	now := time.Now().UTC()
	brl := testCurrency(t, "BRL")
	balance := testMoney(t, "90.00", brl)

	processed, _ := NewWagerTransaction("processed", testTransactionDetails(t, TransactionKindBet), now)
	if err := processed.MarkProcessed(balance, now); err != nil {
		t.Fatal(err)
	}
	processedData, err := NewWagerTransactionProcessedData(processed, balance)
	if err != nil {
		t.Fatal(err)
	}
	processedEvent, err := NewWagerTransactionProcessedEvent("event-processed", "correlation", "", now, processedData)
	if err != nil || processedEvent.Type() != EventTypeWagerTransactionProcessed {
		t.Fatalf("processed event = %v, %v", processedEvent, err)
	}

	rejected, _ := NewWagerTransaction("rejected", testTransactionDetails(t, TransactionKindBet), now)
	if err := rejected.Reject(FailureInsufficientFunds, now); err != nil {
		t.Fatal(err)
	}
	rejectedData, err := NewWagerTransactionRejectedData(rejected)
	if err != nil {
		t.Fatal(err)
	}
	rejectedEvent, err := NewWagerTransactionRejectedEvent("event-rejected", "correlation", "cause", now, rejectedData)
	if err != nil || rejectedEvent.Data().FailureCode() != FailureInsufficientFunds {
		t.Fatalf("rejected event = %v, %v", rejectedEvent, err)
	}

	pending, _ := NewWagerTransaction("pending", testTransactionDetails(t, TransactionKindRefund), now)
	if err := pending.MarkPendingReference(now); err != nil {
		t.Fatal(err)
	}
	pendingData, err := NewWagerTransactionPendingReferenceData(pending)
	if err != nil {
		t.Fatal(err)
	}
	pendingEvent, err := NewWagerTransactionPendingReferenceEvent("event-pending", "correlation", "cause", now, pendingData)
	if err != nil || pendingEvent.Data().ReferenceExternalTransactionID() != "reference-1" {
		t.Fatalf("pending event = %v, %v", pendingEvent, err)
	}
	winDetails := testTransactionDetails(t, TransactionKindWin)
	winDetails.ReferenceExternalTransactionID = "bet-1"
	pendingWin, _ := NewWagerTransaction("pending-win", winDetails, now)
	if err := pendingWin.MarkPendingReference(now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWagerTransactionPendingReferenceData(pendingWin); err != nil {
		t.Fatalf("pending WIN event data: %v", err)
	}

	var _ DomainEvent = processedEvent
	var _ DomainEvent = rejectedEvent
	var _ DomainEvent = pendingEvent
	var _ DomainEvent = eventForInterface(t, now)
}

func eventForInterface(t *testing.T, now time.Time) EventEnvelope[WalletBalanceChangedData] {
	t.Helper()
	brl := testCurrency(t, "BRL")
	entry, err := NewWalletLedgerEntry(
		"entry-interface", "wallet-interface", "tx-interface", LedgerDirectionCredit,
		testMoney(t, "1.00", brl), testMoney(t, "0.00", brl), testMoney(t, "1.00", brl), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	data, err := NewWalletBalanceChangedData(entry, 2)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewWalletBalanceChangedEvent("event-interface", "correlation", "", now, data)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestEventConstructorsRejectInvalidSnapshots(t *testing.T) {
	now := time.Now().UTC()
	tx, _ := NewWagerTransaction("pending", testTransactionDetails(t, TransactionKindBet), now)
	if _, err := NewWagerTransactionProcessedData(tx, testMoney(t, "1.00", testCurrency(t, "BRL"))); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("pending processed-data error = %v", err)
	}
	if _, err := NewWalletBalanceChangedEvent("", "correlation", "", now, WalletBalanceChangedData{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("invalid envelope error = %v", err)
	}
}
