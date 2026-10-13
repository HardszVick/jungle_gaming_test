package postgres

import (
	"encoding/json"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type TransactionRecord struct {
	Transaction            *domain.WagerTransaction
	IdempotencyKey         string
	RequestHash            [32]byte
	HasRequestHash         bool
	ReferenceTransactionID string
	ReferenceAttempts      int
	ReferenceNextAttemptAt *time.Time
	ReferenceExpiresAt     *time.Time
	ReferenceClaimedBy     string
	ReferenceClaimUntil    *time.Time
	CompletedAt            *time.Time
}

type LedgerCursor struct {
	CreatedAt time.Time
	ID        string
}

type LedgerPage struct {
	Entries    []domain.WalletLedgerEntry
	NextCursor *LedgerCursor
}

type Reconciliation struct {
	WalletBalance domain.Money
	LedgerBalance domain.Money
	Difference    domain.Money
	EntryCount    int64
	Consistent    bool
}

type InboxStatus string

const (
	InboxStarted   InboxStatus = "STARTED"
	InboxPending   InboxStatus = "PENDING"
	InboxCompleted InboxStatus = "COMPLETED"
)

type InboxResult struct {
	Status        InboxStatus
	TransactionID string
}

type OutboxClaim struct {
	EventID      string
	EventType    domain.EventType
	AggregateID  string
	Envelope     json.RawMessage
	AttemptCount int
	ClaimedBy    string
	ClaimUntil   time.Time
	CreatedAt    time.Time
}

type PendingReferenceClaim struct {
	Record TransactionRecord
}

type TransactionInsert struct {
	Transaction    *domain.WagerTransaction
	IdempotencyKey string
	RequestHash    []byte
	CreatedAt      time.Time
}
