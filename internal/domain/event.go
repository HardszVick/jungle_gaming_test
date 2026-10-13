package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventTypeWagerTransactionProcessed        EventType = "WagerTransactionProcessed"
	EventTypeWagerTransactionRejected         EventType = "WagerTransactionRejected"
	EventTypeWagerTransactionPendingReference EventType = "WagerTransactionPendingReference"
	EventTypeWalletBalanceChanged             EventType = "WalletBalanceChanged"
	DomainEventVersion                                  = 1
)

type DomainEvent interface {
	ID() string
	Type() EventType
	AggregateID() string
	CorrelationID() string
	CausationID() (string, bool)
	OccurredAt() time.Time
	Version() int
	json.Marshaler
}

// EventEnvelope captures an immutable value snapshot. T should itself be a
// value type; all event data types in this package contain no maps, slices, or
// pointers and are therefore safe to return by value.
type EventEnvelope[T any] struct {
	eventID       string
	eventType     EventType
	aggregateID   string
	correlationID string
	causationID   string
	occurredAt    time.Time
	version       int
	data          T
}

func newEventEnvelope[T any](
	eventID string,
	eventType EventType,
	aggregateID, correlationID, causationID string,
	occurredAt time.Time,
	data T,
) (EventEnvelope[T], error) {
	if eventID == "" || aggregateID == "" || correlationID == "" || occurredAt.IsZero() {
		return EventEnvelope[T]{}, fmt.Errorf("%w: event, aggregate, correlation, and time are required", ErrInvalidEvent)
	}
	switch eventType {
	case EventTypeWagerTransactionProcessed, EventTypeWagerTransactionRejected,
		EventTypeWagerTransactionPendingReference, EventTypeWalletBalanceChanged:
	default:
		return EventEnvelope[T]{}, fmt.Errorf("%w: unknown event type", ErrInvalidEvent)
	}
	return EventEnvelope[T]{
		eventID: eventID, eventType: eventType, aggregateID: aggregateID,
		correlationID: correlationID, causationID: causationID,
		occurredAt: occurredAt.UTC(), version: DomainEventVersion, data: data,
	}, nil
}

func (e EventEnvelope[T]) ID() string            { return e.eventID }
func (e EventEnvelope[T]) Type() EventType       { return e.eventType }
func (e EventEnvelope[T]) AggregateID() string   { return e.aggregateID }
func (e EventEnvelope[T]) CorrelationID() string { return e.correlationID }
func (e EventEnvelope[T]) OccurredAt() time.Time { return e.occurredAt }
func (e EventEnvelope[T]) Version() int          { return e.version }
func (e EventEnvelope[T]) Data() T               { return e.data }
func (e EventEnvelope[T]) CausationID() (string, bool) {
	return e.causationID, e.causationID != ""
}

func (e EventEnvelope[T]) MarshalJSON() ([]byte, error) {
	if e.eventID == "" || e.version != DomainEventVersion {
		return nil, ErrInvalidEvent
	}
	type envelope struct {
		EventID       string    `json:"eventId"`
		EventType     EventType `json:"eventType"`
		AggregateID   string    `json:"aggregateId"`
		CorrelationID string    `json:"correlationId"`
		CausationID   string    `json:"causationId,omitempty"`
		OccurredAt    string    `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          T         `json:"data"`
	}
	return json.Marshal(envelope{
		EventID: e.eventID, EventType: e.eventType, AggregateID: e.aggregateID,
		CorrelationID: e.correlationID, CausationID: e.causationID,
		OccurredAt: e.occurredAt.UTC().Format(time.RFC3339Nano), Version: e.version, Data: e.data,
	})
}

type WagerTransactionProcessedData struct {
	transactionID string
	providerID    string
	externalID    string
	walletID      string
	playerID      string
	kind          WagerTransactionKind
	money         Money
	resultBalance Money
}

func NewWagerTransactionProcessedData(tx *WagerTransaction, resultBalance Money) (WagerTransactionProcessedData, error) {
	if tx == nil || tx.state != TransactionStateProcessed {
		return WagerTransactionProcessedData{}, fmt.Errorf("%w: transaction is not processed", ErrInvalidEvent)
	}
	storedBalance, ok := tx.ResultBalance()
	if !ok {
		return WagerTransactionProcessedData{}, fmt.Errorf("%w: processed balance is missing", ErrInvalidEvent)
	}
	equal, err := storedBalance.Equal(resultBalance)
	if err != nil || !equal {
		return WagerTransactionProcessedData{}, fmt.Errorf("%w: result balance differs from transaction snapshot", ErrInvalidEvent)
	}
	d := tx.details
	return WagerTransactionProcessedData{
		transactionID: tx.id, providerID: d.ProviderID, externalID: d.ExternalTransactionID,
		walletID: d.WalletID, playerID: d.PlayerID, kind: d.Kind, money: d.Money,
		resultBalance: resultBalance,
	}, nil
}

func (d WagerTransactionProcessedData) TransactionID() string         { return d.transactionID }
func (d WagerTransactionProcessedData) ProviderID() string            { return d.providerID }
func (d WagerTransactionProcessedData) ExternalTransactionID() string { return d.externalID }
func (d WagerTransactionProcessedData) WalletID() string              { return d.walletID }
func (d WagerTransactionProcessedData) PlayerID() string              { return d.playerID }
func (d WagerTransactionProcessedData) Kind() WagerTransactionKind    { return d.kind }
func (d WagerTransactionProcessedData) Money() Money                  { return d.money }
func (d WagerTransactionProcessedData) ResultBalance() Money          { return d.resultBalance }

func (d WagerTransactionProcessedData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TransactionID         string               `json:"transactionId"`
		ProviderID            string               `json:"providerId,omitempty"`
		ExternalTransactionID string               `json:"externalTransactionId,omitempty"`
		WalletID              string               `json:"walletId"`
		PlayerID              string               `json:"playerId"`
		Kind                  WagerTransactionKind `json:"kind"`
		Money                 Money                `json:"money"`
		ResultBalance         Money                `json:"resultBalance"`
	}{d.transactionID, d.providerID, d.externalID, d.walletID, d.playerID, d.kind, d.money, d.resultBalance})
}

type WagerTransactionRejectedData struct {
	transactionID string
	providerID    string
	externalID    string
	walletID      string
	kind          WagerTransactionKind
	failureCode   FailureCode
}

func NewWagerTransactionRejectedData(tx *WagerTransaction) (WagerTransactionRejectedData, error) {
	if tx == nil || tx.state != TransactionStateRejected || !tx.failureCode.IsValid() {
		return WagerTransactionRejectedData{}, fmt.Errorf("%w: transaction is not rejected", ErrInvalidEvent)
	}
	d := tx.details
	return WagerTransactionRejectedData{
		transactionID: tx.id, providerID: d.ProviderID, externalID: d.ExternalTransactionID,
		walletID: d.WalletID, kind: d.Kind, failureCode: tx.failureCode,
	}, nil
}

func (d WagerTransactionRejectedData) TransactionID() string         { return d.transactionID }
func (d WagerTransactionRejectedData) ProviderID() string            { return d.providerID }
func (d WagerTransactionRejectedData) ExternalTransactionID() string { return d.externalID }
func (d WagerTransactionRejectedData) WalletID() string              { return d.walletID }
func (d WagerTransactionRejectedData) Kind() WagerTransactionKind    { return d.kind }
func (d WagerTransactionRejectedData) FailureCode() FailureCode      { return d.failureCode }

func (d WagerTransactionRejectedData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TransactionID         string               `json:"transactionId"`
		ProviderID            string               `json:"providerId,omitempty"`
		ExternalTransactionID string               `json:"externalTransactionId,omitempty"`
		WalletID              string               `json:"walletId"`
		Kind                  WagerTransactionKind `json:"kind"`
		FailureCode           FailureCode          `json:"failureCode"`
	}{d.transactionID, d.providerID, d.externalID, d.walletID, d.kind, d.failureCode})
}

type WagerTransactionPendingReferenceData struct {
	transactionID string
	providerID    string
	externalID    string
	walletID      string
	referenceID   string
}

func NewWagerTransactionPendingReferenceData(tx *WagerTransaction) (WagerTransactionPendingReferenceData, error) {
	if tx == nil || tx.state != TransactionStatePendingReference || tx.details.ReferenceExternalTransactionID == "" ||
		(!tx.details.Kind.IsReversal() && tx.details.Kind != TransactionKindWin) {
		return WagerTransactionPendingReferenceData{}, fmt.Errorf("%w: transaction is not pending a reference", ErrInvalidEvent)
	}
	d := tx.details
	return WagerTransactionPendingReferenceData{
		transactionID: tx.id, providerID: d.ProviderID, externalID: d.ExternalTransactionID,
		walletID: d.WalletID, referenceID: d.ReferenceExternalTransactionID,
	}, nil
}

func (d WagerTransactionPendingReferenceData) TransactionID() string         { return d.transactionID }
func (d WagerTransactionPendingReferenceData) ProviderID() string            { return d.providerID }
func (d WagerTransactionPendingReferenceData) ExternalTransactionID() string { return d.externalID }
func (d WagerTransactionPendingReferenceData) WalletID() string              { return d.walletID }
func (d WagerTransactionPendingReferenceData) ReferenceExternalTransactionID() string {
	return d.referenceID
}

func (d WagerTransactionPendingReferenceData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TransactionID                  string `json:"transactionId"`
		ProviderID                     string `json:"providerId"`
		ExternalTransactionID          string `json:"externalTransactionId"`
		WalletID                       string `json:"walletId"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	}{d.transactionID, d.providerID, d.externalID, d.walletID, d.referenceID})
}

type WalletBalanceChangedData struct {
	walletID      string
	transactionID string
	direction     LedgerDirection
	money         Money
	balanceBefore Money
	balanceAfter  Money
	walletVersion int64
}

func NewWalletBalanceChangedData(entry WalletLedgerEntry, walletVersion int64) (WalletBalanceChangedData, error) {
	if walletVersion < 1 {
		return WalletBalanceChangedData{}, fmt.Errorf("%w: wallet version must be positive", ErrInvalidEvent)
	}
	if entry.id == "" {
		return WalletBalanceChangedData{}, fmt.Errorf("%w: ledger entry is required", ErrInvalidEvent)
	}
	return WalletBalanceChangedData{
		walletID: entry.walletID, transactionID: entry.transactionID,
		direction: entry.direction, money: entry.money,
		balanceBefore: entry.balanceBefore, balanceAfter: entry.balanceAfter,
		walletVersion: walletVersion,
	}, nil
}

func (d WalletBalanceChangedData) WalletID() string           { return d.walletID }
func (d WalletBalanceChangedData) TransactionID() string      { return d.transactionID }
func (d WalletBalanceChangedData) Direction() LedgerDirection { return d.direction }
func (d WalletBalanceChangedData) Money() Money               { return d.money }
func (d WalletBalanceChangedData) BalanceBefore() Money       { return d.balanceBefore }
func (d WalletBalanceChangedData) BalanceAfter() Money        { return d.balanceAfter }
func (d WalletBalanceChangedData) WalletVersion() int64       { return d.walletVersion }

func (d WalletBalanceChangedData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		WalletID      string          `json:"walletId"`
		TransactionID string          `json:"transactionId"`
		Direction     LedgerDirection `json:"direction"`
		Money         Money           `json:"money"`
		BalanceBefore Money           `json:"balanceBefore"`
		BalanceAfter  Money           `json:"balanceAfter"`
		WalletVersion int64           `json:"walletVersion"`
	}{d.walletID, d.transactionID, d.direction, d.money, d.balanceBefore, d.balanceAfter, d.walletVersion})
}

func NewWagerTransactionProcessedEvent(
	eventID, correlationID, causationID string,
	occurredAt time.Time,
	data WagerTransactionProcessedData,
) (EventEnvelope[WagerTransactionProcessedData], error) {
	return newEventEnvelope(eventID, EventTypeWagerTransactionProcessed, data.transactionID, correlationID, causationID, occurredAt, data)
}

func NewWagerTransactionRejectedEvent(
	eventID, correlationID, causationID string,
	occurredAt time.Time,
	data WagerTransactionRejectedData,
) (EventEnvelope[WagerTransactionRejectedData], error) {
	return newEventEnvelope(eventID, EventTypeWagerTransactionRejected, data.transactionID, correlationID, causationID, occurredAt, data)
}

func NewWagerTransactionPendingReferenceEvent(
	eventID, correlationID, causationID string,
	occurredAt time.Time,
	data WagerTransactionPendingReferenceData,
) (EventEnvelope[WagerTransactionPendingReferenceData], error) {
	return newEventEnvelope(eventID, EventTypeWagerTransactionPendingReference, data.transactionID, correlationID, causationID, occurredAt, data)
}

func NewWalletBalanceChangedEvent(
	eventID, correlationID, causationID string,
	occurredAt time.Time,
	data WalletBalanceChangedData,
) (EventEnvelope[WalletBalanceChangedData], error) {
	return newEventEnvelope(eventID, EventTypeWalletBalanceChanged, data.walletID, correlationID, causationID, occurredAt, data)
}
