package httpapi

import (
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

type moneyInput struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type openWalletInput struct {
	PlayerID       string     `json:"playerId"`
	InitialBalance moneyInput `json:"initialBalance"`
}

type walletResponse struct {
	ID        string       `json:"id"`
	PlayerID  string       `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

func newWalletResponse(wallet *domain.Wallet) walletResponse {
	return walletResponse{
		ID: wallet.ID(), PlayerID: wallet.PlayerID(), Balance: wallet.Balance(),
		Version: wallet.Version(), CreatedAt: wallet.CreatedAt(), UpdatedAt: wallet.UpdatedAt(),
	}
}

func (server *api) openWallet(response http.ResponseWriter, request *http.Request) {
	var input openWalletInput
	if !decodeJSON(response, request, server.options.MaxBodyBytes, &input) {
		return
	}
	balance, err := domain.ParseMoneyValue(input.InitialBalance.Amount, strings.ToUpper(input.InitialBalance.Currency))
	if err != nil || input.PlayerID == "" || strings.TrimSpace(input.PlayerID) != input.PlayerID {
		writeError(response, http.StatusBadRequest, "INVALID_REQUEST", "wallet request is invalid", nil)
		return
	}
	wallet, err := server.service.OpenWallet(request.Context(), application.OpenWalletRequest{
		PlayerID: input.PlayerID, InitialBalance: balance, CorrelationID: correlationID(request.Context()),
	})
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, newWalletResponse(wallet))
}

func (server *api) getWallet(response http.ResponseWriter, request *http.Request) {
	walletID := request.PathValue("walletId")
	if !validUUID(walletID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "walletId is invalid", nil)
		return
	}
	wallet, err := server.service.GetWallet(request.Context(), walletID)
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, newWalletResponse(wallet))
}

type ledgerEntryResponse struct {
	LedgerEntryID string                 `json:"ledgerEntryId"`
	WalletID      string                 `json:"walletId"`
	TransactionID string                 `json:"transactionId"`
	Direction     domain.LedgerDirection `json:"direction"`
	Money         domain.Money           `json:"money"`
	BalanceBefore domain.Money           `json:"balanceBefore"`
	BalanceAfter  domain.Money           `json:"balanceAfter"`
	CreatedAt     time.Time              `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func (server *api) listLedger(response http.ResponseWriter, request *http.Request) {
	walletID := request.PathValue("walletId")
	if !validUUID(walletID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "walletId is invalid", nil)
		return
	}
	limit := DefaultLedgerLimit
	if rawLimit := request.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > MaxLedgerLimit {
			writeError(response, http.StatusBadRequest, "INVALID_PAGINATION", "limit must be between 1 and 1000", nil)
			return
		}
		limit = parsed
	}
	var cursor *postgres.LedgerCursor
	if rawCursor := request.URL.Query().Get("cursor"); rawCursor != "" {
		parsed, err := decodeLedgerCursor(rawCursor)
		if err != nil {
			writeError(response, http.StatusBadRequest, "INVALID_CURSOR", "cursor is invalid", nil)
			return
		}
		cursor = &parsed
	}
	if _, err := server.service.GetWallet(request.Context(), walletID); err != nil {
		server.writeServiceError(response, err)
		return
	}
	page, err := server.service.ListLedger(request.Context(), walletID, cursor, limit)
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	result := ledgerResponse{Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, entry := range page.Entries {
		result.Entries = append(result.Entries, ledgerEntryResponse{
			LedgerEntryID: entry.ID(), WalletID: entry.WalletID(), TransactionID: entry.TransactionID(),
			Direction: entry.Direction(), Money: entry.Money(), BalanceBefore: entry.BalanceBefore(),
			BalanceAfter: entry.BalanceAfter(), CreatedAt: entry.CreatedAt(),
		})
	}
	if page.NextCursor != nil {
		result.NextCursor = encodeLedgerCursor(*page.NextCursor)
	}
	writeJSON(response, http.StatusOK, result)
}

type reconciliationResponse struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

func (server *api) reconcileWallet(response http.ResponseWriter, request *http.Request) {
	walletID := request.PathValue("walletId")
	if !validUUID(walletID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "walletId is invalid", nil)
		return
	}
	ctx := observability.WithIdentifiers(request.Context(), observability.Identifiers{WalletID: walletID})
	result, err := server.service.Reconcile(ctx, walletID)
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	if server.options.Logger != nil {
		if result.Consistent {
			observability.Log(ctx, server.options.Logger, slog.LevelInfo, "HTTP reconciliation completed")
		} else {
			observability.Log(ctx, server.options.Logger, slog.LevelWarn, "HTTP reconciliation divergence detected")
		}
	}
	if !result.Consistent && server.options.Metrics != nil {
		server.options.Metrics.RecordReconciliationDivergence()
	}
	writeJSON(response, http.StatusOK, reconciliationResponse{
		WalletID: walletID, StoredBalance: result.WalletBalance, CalculatedBalance: result.LedgerBalance,
		Difference: result.Difference, Consistent: result.Consistent, CheckedEntries: result.EntryCount,
	})
}

type transactionInput struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          moneyInput `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

type transactionResponse struct {
	TransactionID                  string                       `json:"transactionId"`
	ProviderID                     string                       `json:"providerId,omitempty"`
	ExternalTransactionID          string                       `json:"externalTransactionId,omitempty"`
	PlayerID                       string                       `json:"playerId"`
	WalletID                       string                       `json:"walletId"`
	RoundID                        string                       `json:"roundId,omitempty"`
	GameID                         string                       `json:"gameId,omitempty"`
	Kind                           domain.WagerTransactionKind  `json:"kind"`
	Money                          domain.Money                 `json:"money"`
	ReferenceExternalTransactionID string                       `json:"referenceExternalTransactionId,omitempty"`
	Status                         domain.WagerTransactionState `json:"status"`
	Balance                        *domain.Money                `json:"balance,omitempty"`
	FailureCode                    domain.FailureCode           `json:"failureCode,omitempty"`
	IdempotentReplay               *bool                        `json:"idempotentReplay,omitempty"`
	CreatedAt                      time.Time                    `json:"createdAt"`
	UpdatedAt                      time.Time                    `json:"updatedAt"`
}

func newTransactionResponse(transaction *domain.WagerTransaction, replay *bool) transactionResponse {
	details := transaction.Details()
	result := transactionResponse{
		TransactionID: transaction.ID(), ProviderID: details.ProviderID,
		ExternalTransactionID: details.ExternalTransactionID, PlayerID: details.PlayerID,
		WalletID: details.WalletID, RoundID: details.RoundID, GameID: details.GameID,
		Kind: details.Kind, Money: details.Money,
		ReferenceExternalTransactionID: details.ReferenceExternalTransactionID,
		Status:                         transaction.State(), FailureCode: transaction.FailureCode(),
		IdempotentReplay: replay, CreatedAt: transaction.CreatedAt(), UpdatedAt: transaction.UpdatedAt(),
	}
	if balance, ok := transaction.ResultBalance(); ok {
		result.Balance = &balance
	}
	return result
}

func (server *api) processTransaction(response http.ResponseWriter, request *http.Request) {
	keys := request.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" || strings.TrimSpace(keys[0]) != keys[0] {
		writeError(response, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is required", nil)
		return
	}
	var input transactionInput
	if !decodeJSON(response, request, server.options.MaxBodyBytes, &input) {
		return
	}
	if err := auth.AuthorizeProvider(request.Context(), input.ProviderID); err != nil {
		writeAuthorizationError(response, err)
		return
	}
	if !validUUID(input.WalletID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "walletId is invalid", nil)
		return
	}
	command, err := application.NormalizeTransactionCommand(application.TransactionCommandInput{
		ProviderID: input.ProviderID, ExternalTransactionID: input.ExternalTransactionID,
		PlayerID: input.PlayerID, WalletID: input.WalletID, RoundID: input.RoundID,
		GameID: input.GameID, Kind: input.Kind, Amount: input.Money.Amount,
		Currency: input.Money.Currency, ReferenceExternalTransactionID: input.ReferenceExternalTransactionID,
	})
	if err != nil {
		writeError(response, http.StatusBadRequest, "INVALID_TRANSACTION", "transaction request is invalid", nil)
		return
	}
	ctx := observability.WithIdentifiers(request.Context(), observability.Identifiers{
		WalletID: input.WalletID, ProviderID: input.ProviderID,
	})
	startedAt := time.Now()
	metricStatus := "ERROR"
	defer func() {
		if server.options.Metrics != nil {
			server.options.Metrics.ObserveProcessingLatency(string(command.Kind()), metricStatus, time.Since(startedAt))
		}
	}()
	result, err := server.service.ProcessTransaction(ctx, application.ProcessTransactionRequest{
		Command: command, IdempotencyKey: keys[0], CorrelationID: correlationID(request.Context()),
	})
	if err != nil {
		server.recordConflict(err)
		if server.options.Logger != nil {
			observability.Log(ctx, server.options.Logger, slog.LevelWarn, "HTTP transaction processing failed")
		}
		server.writeServiceError(response, err)
		return
	}
	if result.Transaction == nil {
		if server.options.Logger != nil {
			observability.Log(ctx, server.options.Logger, slog.LevelError, "HTTP transaction result missing")
		}
		server.writeServiceError(response, errors.New("transaction result is missing"))
		return
	}
	state := result.Transaction.State()
	metricStatus = string(state)
	ctx = observability.WithIdentifiers(ctx, observability.Identifiers{TransactionID: result.Transaction.ID()})
	if server.options.Metrics != nil {
		if state.IsTerminal() {
			server.options.Metrics.RecordTransactionResult(string(command.Kind()), string(state), string(result.Transaction.FailureCode()))
		}
		if result.IdempotentReplay {
			server.options.Metrics.RecordDuplicate("transaction")
		}
	}
	if server.options.Logger != nil {
		observability.Log(ctx, server.options.Logger, slog.LevelInfo, "HTTP transaction processed")
	}
	status := http.StatusOK
	switch state {
	case domain.TransactionStatePending, domain.TransactionStatePendingReference:
		status = http.StatusAccepted
	case domain.TransactionStateRejected:
		status = http.StatusConflict
	}
	replay := result.IdempotentReplay
	writeJSON(response, status, newTransactionResponse(result.Transaction, &replay))
}

func (server *api) recordConflict(err error) {
	if server.options.Metrics == nil {
		return
	}
	var kind string
	switch {
	case errors.Is(err, postgres.ErrIdempotencyConflict):
		kind = "idempotency"
	case errors.Is(err, postgres.ErrExternalIDConflict):
		kind = "external_transaction"
	case errors.Is(err, postgres.ErrConcurrentUpdate):
		kind = "concurrent_update"
	case errors.Is(err, postgres.ErrSerializationFailure):
		kind = "serialization"
	case errors.Is(err, postgres.ErrDeadlock):
		kind = "deadlock"
	case errors.Is(err, postgres.ErrConstraintViolation):
		kind = "constraint"
	case errors.Is(err, postgres.ErrConflict):
		kind = "database"
	}
	if kind != "" {
		server.options.Metrics.RecordConcurrencyConflict(kind)
	}
}

func (server *api) getTransaction(response http.ResponseWriter, request *http.Request) {
	transactionID := request.PathValue("transactionId")
	if !validUUID(transactionID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "transactionId is invalid", nil)
		return
	}
	record, err := server.service.GetTransaction(request.Context(), transactionID)
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, newTransactionResponse(record.Transaction, nil))
}

func (server *api) getProviderTransaction(response http.ResponseWriter, request *http.Request) {
	providerID := request.PathValue("providerId")
	externalID := request.PathValue("externalTransactionId")
	if !validTextIdentifier(providerID) || !validTextIdentifier(externalID) {
		writeError(response, http.StatusBadRequest, "INVALID_IDENTIFIER", "provider or transaction identifier is invalid", nil)
		return
	}
	record, err := server.service.GetTransactionByExternalID(request.Context(), providerID, externalID)
	if err != nil {
		server.writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, newTransactionResponse(record.Transaction, nil))
}

func (server *api) writeServiceError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, application.ErrInvalidRequest), errors.Is(err, application.ErrInvalidTransactionCommand),
		errors.Is(err, domain.ErrInvalidCurrency), errors.Is(err, domain.ErrInvalidMoney),
		errors.Is(err, domain.ErrMoneyOverflow), errors.Is(err, domain.ErrInvalidWallet),
		errors.Is(err, domain.ErrInvalidTransaction):
		writeError(response, http.StatusBadRequest, "INVALID_REQUEST", "request is invalid", nil)
	case errors.Is(err, postgres.ErrNotFound):
		writeError(response, http.StatusNotFound, "NOT_FOUND", "resource not found", nil)
	case errors.Is(err, application.ErrWalletConflict):
		writeError(response, http.StatusConflict, "WALLET_CONFLICT", "wallet already exists", nil)
	case errors.Is(err, postgres.ErrIdempotencyConflict):
		writeError(response, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key conflicts with an existing request", nil)
	case errors.Is(err, postgres.ErrExternalIDConflict):
		writeError(response, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", "external transaction already exists", nil)
	case errors.Is(err, postgres.ErrConflict), errors.Is(err, postgres.ErrConstraintViolation),
		errors.Is(err, postgres.ErrConcurrentUpdate):
		writeError(response, http.StatusConflict, "CONFLICT", "request conflicts with existing state", nil)
	default:
		response.Header().Set("Retry-After", retryAfterValue(server.options.RetryAfter))
		writeError(response, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "service is temporarily unavailable", nil)
	}
}

func writeAuthorizationError(response http.ResponseWriter, err error) {
	if errors.Is(err, auth.ErrUnauthorized) {
		response.Header().Set("WWW-Authenticate", "Bearer")
		writeError(response, http.StatusUnauthorized, "UNAUTHORIZED", "authentication is required", nil)
		return
	}
	writeError(response, http.StatusForbidden, "FORBIDDEN", "access is forbidden", nil)
}

func validUUID(value string) bool { return uuid.Validate(value) == nil }

func validTextIdentifier(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value
}

func encodeLedgerCursor(cursor postgres.LedgerCursor) string {
	value := cursor.CreatedAt.UTC().Format(time.RFC3339Nano) + "\x00" + cursor.ID
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeLedgerCursor(value string) (postgres.LedgerCursor, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil {
		return postgres.LedgerCursor{}, err
	}
	parts := strings.Split(string(decoded), "\x00")
	if len(parts) != 2 || !validUUID(parts[1]) {
		return postgres.LedgerCursor{}, errors.New("invalid cursor")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return postgres.LedgerCursor{}, err
	}
	return postgres.LedgerCursor{CreatedAt: createdAt.UTC(), ID: parts[1]}, nil
}

func retryAfterValue(duration time.Duration) string {
	seconds := int64((duration + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}
