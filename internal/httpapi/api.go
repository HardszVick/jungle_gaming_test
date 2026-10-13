package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

const (
	DefaultMaxBodyBytes     int64 = 1 << 20
	DefaultReadinessTimeout       = 2 * time.Second
	DefaultRetryAfter             = time.Second
	DefaultLedgerLimit            = 50
	MaxLedgerLimit                = 1000
)

// WalletService is the wallet-facing subset implemented by application.Service.
type WalletService interface {
	OpenWallet(context.Context, application.OpenWalletRequest) (*domain.Wallet, error)
	GetWallet(context.Context, string) (*domain.Wallet, error)
	ListLedger(context.Context, string, *postgres.LedgerCursor, int) (postgres.LedgerPage, error)
	Reconcile(context.Context, string) (postgres.Reconciliation, error)
}

// TransactionService is the transaction-facing subset implemented by application.Service.
type TransactionService interface {
	ProcessTransaction(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error)
	GetTransaction(context.Context, string) (postgres.TransactionRecord, error)
	GetTransactionByExternalID(context.Context, string, string) (postgres.TransactionRecord, error)
}

type Service interface {
	WalletService
	TransactionService
}

var _ Service = (*application.Service)(nil)

// HealthChecker is intentionally compatible with postgres.Database and the SQS adapter.
type HealthChecker interface {
	Health(context.Context) error
}

type ReadinessChecks struct {
	PostgreSQL HealthChecker
	SQS        HealthChecker
}

// Metrics is the HTTP-facing subset of operational metrics. Implementations
// must keep label values bounded; request identifiers are never passed here.
type Metrics interface {
	RecordTransactionResult(kind, status, code string)
	RecordDuplicate(kind string)
	RecordConcurrencyConflict(kind string)
	ObserveProcessingLatency(kind, status string, latency time.Duration)
	RecordReconciliationDivergence()
}

type Options struct {
	MaxBodyBytes     int64
	ReadinessTimeout time.Duration
	RetryAfter       time.Duration
	NewCorrelationID func() string
	Metrics          Metrics
	Logger           *slog.Logger
}

type api struct {
	service       Service
	authenticator *auth.Authenticator
	checks        ReadinessChecks
	options       Options
}

// NewHandler constructs the public API. Metrics are deliberately exposed by
// NewInternalHandler instead, so they cannot accidentally share this listener.
func NewHandler(service Service, authenticator *auth.Authenticator, checks ReadinessChecks, options Options) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("httpapi: service is required")
	}
	if authenticator == nil {
		return nil, errors.New("httpapi: authenticator is required")
	}
	if checks.PostgreSQL == nil || checks.SQS == nil {
		return nil, errors.New("httpapi: PostgreSQL and SQS health checks are required")
	}
	if options.MaxBodyBytes == 0 {
		options.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if options.ReadinessTimeout == 0 {
		options.ReadinessTimeout = DefaultReadinessTimeout
	}
	if options.RetryAfter == 0 {
		options.RetryAfter = DefaultRetryAfter
	}
	if options.MaxBodyBytes < 1 || options.ReadinessTimeout < 1 || options.RetryAfter < 1 {
		return nil, errors.New("httpapi: limits and durations must be positive")
	}
	if options.NewCorrelationID == nil {
		options.NewCorrelationID = defaultCorrelationID
	}

	server := &api{service: service, authenticator: authenticator, checks: checks, options: options}
	mux := http.NewServeMux()
	mux.Handle("POST /wallets", server.internal(http.HandlerFunc(server.openWallet)))
	mux.Handle("GET /wallets/{walletId}", server.internal(http.HandlerFunc(server.getWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", server.internal(http.HandlerFunc(server.listLedger)))
	mux.Handle("POST /wallets/{walletId}/reconciliation", server.internal(http.HandlerFunc(server.reconcileWallet)))
	mux.Handle("POST /wagering/transactions", server.authenticator.Middleware("wager:write")(http.HandlerFunc(server.processTransaction)))
	mux.Handle("GET /wagering/transactions/{transactionId}", server.internal(http.HandlerFunc(server.getTransaction)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", server.providerRead(http.HandlerFunc(server.getProviderTransaction)))
	mux.HandleFunc("GET /health/live", server.live)
	mux.HandleFunc("GET /health/ready", server.ready)

	handler := recoveryMiddleware(stableErrorsMiddleware(mux))
	handler = requestLoggingMiddleware(options.Logger)(handler)
	return correlationMiddleware(options.NewCorrelationID)(handler), nil
}

func (server *api) internal(next http.Handler) http.Handler {
	return server.authenticator.Middleware()(auth.RequireInternal(next))
}

func (server *api) providerRead(next http.Handler) http.Handler {
	return server.authenticator.Middleware("wager:read")(
		auth.RequireProviderFrom(func(request *http.Request) string {
			return request.PathValue("providerId")
		})(next),
	)
}
