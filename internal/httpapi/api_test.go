package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/postgres"
)

const (
	walletID      = "018f0000-0000-7000-8000-000000000001"
	transactionID = "018f0000-0000-7000-8000-000000000002"
	ledgerID      = "018f0000-0000-7000-8000-000000000003"
)

var testTime = time.Date(2026, 10, 10, 12, 0, 0, 123000000, time.UTC)

type fakeService struct {
	openWalletFn                 func(context.Context, application.OpenWalletRequest) (*domain.Wallet, error)
	getWalletFn                  func(context.Context, string) (*domain.Wallet, error)
	listLedgerFn                 func(context.Context, string, *postgres.LedgerCursor, int) (postgres.LedgerPage, error)
	reconcileFn                  func(context.Context, string) (postgres.Reconciliation, error)
	processTransactionFn         func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error)
	getTransactionFn             func(context.Context, string) (postgres.TransactionRecord, error)
	getTransactionByExternalIDFn func(context.Context, string, string) (postgres.TransactionRecord, error)
}

func (service *fakeService) OpenWallet(ctx context.Context, request application.OpenWalletRequest) (*domain.Wallet, error) {
	if service.openWalletFn == nil {
		return nil, postgres.ErrNotFound
	}
	return service.openWalletFn(ctx, request)
}

func (service *fakeService) GetWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	if service.getWalletFn == nil {
		return nil, postgres.ErrNotFound
	}
	return service.getWalletFn(ctx, id)
}

func (service *fakeService) ListLedger(ctx context.Context, id string, cursor *postgres.LedgerCursor, limit int) (postgres.LedgerPage, error) {
	if service.listLedgerFn == nil {
		return postgres.LedgerPage{}, postgres.ErrNotFound
	}
	return service.listLedgerFn(ctx, id, cursor, limit)
}

func (service *fakeService) Reconcile(ctx context.Context, id string) (postgres.Reconciliation, error) {
	if service.reconcileFn == nil {
		return postgres.Reconciliation{}, postgres.ErrNotFound
	}
	return service.reconcileFn(ctx, id)
}

func (service *fakeService) ProcessTransaction(ctx context.Context, request application.ProcessTransactionRequest) (application.TransactionResult, error) {
	if service.processTransactionFn == nil {
		return application.TransactionResult{}, postgres.ErrNotFound
	}
	return service.processTransactionFn(ctx, request)
}

func (service *fakeService) GetTransaction(ctx context.Context, id string) (postgres.TransactionRecord, error) {
	if service.getTransactionFn == nil {
		return postgres.TransactionRecord{}, postgres.ErrNotFound
	}
	return service.getTransactionFn(ctx, id)
}

func (service *fakeService) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (postgres.TransactionRecord, error) {
	if service.getTransactionByExternalIDFn == nil {
		return postgres.TransactionRecord{}, postgres.ErrNotFound
	}
	return service.getTransactionByExternalIDFn(ctx, providerID, externalID)
}

type verifierFunc func(context.Context, string) (auth.VerifiedToken, error)

func (verify verifierFunc) Verify(ctx context.Context, token string) (auth.VerifiedToken, error) {
	return verify(ctx, token)
}

type healthFunc func(context.Context) error

func (check healthFunc) Health(ctx context.Context) error { return check(ctx) }

type transactionMetricCall struct {
	kind   string
	status string
	code   string
}

type latencyMetricCall struct {
	kind   string
	status string
	value  time.Duration
}

type fakeMetrics struct {
	transactions []transactionMetricCall
	duplicates   []string
	conflicts    []string
	latencies    []latencyMetricCall
	divergences  int
}

func (metrics *fakeMetrics) RecordTransactionResult(kind, status, code string) {
	metrics.transactions = append(metrics.transactions, transactionMetricCall{kind: kind, status: status, code: code})
}

func (metrics *fakeMetrics) RecordDuplicate(kind string) {
	metrics.duplicates = append(metrics.duplicates, kind)
}

func (metrics *fakeMetrics) RecordConcurrencyConflict(kind string) {
	metrics.conflicts = append(metrics.conflicts, kind)
}

func (metrics *fakeMetrics) ObserveProcessingLatency(kind, status string, value time.Duration) {
	metrics.latencies = append(metrics.latencies, latencyMetricCall{kind: kind, status: status, value: value})
}

func (metrics *fakeMetrics) RecordReconciliationDivergence() {
	metrics.divergences++
}

func testHandler(t *testing.T, service Service, options Options, postgresHealth, sqsHealth HealthChecker) http.Handler {
	t.Helper()
	authenticator, err := auth.NewAuthenticator(verifierFunc(func(_ context.Context, token string) (auth.VerifiedToken, error) {
		claims := ""
		switch token {
		case "internal":
			claims = `{"realm_access":{"roles":["wallet:internal"]}}`
		case "provider-a":
			claims = `{"provider_id":"provider-a","scope":"wager:read wager:write"}`
		case "provider-b":
			claims = `{"provider_id":"provider-b","scope":"wager:read wager:write"}`
		case "provider-no-scope":
			claims = `{"provider_id":"provider-a"}`
		default:
			return auth.VerifiedToken{}, auth.ErrInvalidToken
		}
		return auth.VerifiedToken{Subject: "subject", Claims: json.RawMessage(claims)}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if postgresHealth == nil {
		postgresHealth = healthFunc(func(context.Context) error { return nil })
	}
	if sqsHealth == nil {
		sqsHealth = healthFunc(func(context.Context) error { return nil })
	}
	handler, err := NewHandler(service, authenticator, ReadinessChecks{PostgreSQL: postgresHealth, SQS: sqsHealth}, options)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func request(handler http.Handler, method, path, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func mustMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	money, err := domain.ParseMoneyValue(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return money
}

func mustWallet(t *testing.T) *domain.Wallet {
	t.Helper()
	wallet, err := domain.RehydrateWallet(walletID, "player-1", mustMoney(t, "100.00"), 1, testTime, testTime)
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

func transactionBody(providerID, kind string) string {
	reference := ""
	if kind == "REFUND" || kind == "ROLLBACK" {
		reference = `,"referenceExternalTransactionId":"reference-1"`
	}
	amount := "10.00"
	if kind == "LOSS" {
		amount = "0.00"
	}
	return `{"providerId":"` + providerID + `","externalTransactionId":"external-1","playerId":"player-1","walletId":"` + walletID + `","roundId":"round-1","gameId":"game-1","kind":"` + kind + `","money":{"amount":"` + amount + `","currency":"BRL"}` + reference + `}`
}

func mustTransaction(t *testing.T, state domain.WagerTransactionState, failure domain.FailureCode) *domain.WagerTransaction {
	t.Helper()
	tx, err := domain.NewWagerTransaction(transactionID, domain.WagerTransactionDetails{
		ProviderID: "provider-a", ExternalTransactionID: "external-1", PlayerID: "player-1",
		WalletID: walletID, RoundID: "round-1", GameID: "game-1",
		Kind: domain.TransactionKindBet, Money: mustMoney(t, "10.00"),
	}, testTime)
	if err != nil {
		t.Fatal(err)
	}
	switch state {
	case domain.TransactionStateProcessed:
		err = tx.MarkProcessed(mustMoney(t, "90.00"), testTime.Add(time.Second))
	case domain.TransactionStateRejected:
		err = tx.Reject(failure, testTime.Add(time.Second))
	case domain.TransactionStatePendingReference:
		tx, err = domain.NewWagerTransaction(transactionID, domain.WagerTransactionDetails{
			ProviderID: "provider-a", ExternalTransactionID: "external-1", PlayerID: "player-1",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1",
			Kind: domain.TransactionKindRefund, Money: mustMoney(t, "10.00"), ReferenceExternalTransactionID: "reference-1",
		}, testTime)
		if err == nil {
			err = tx.MarkPendingReference(testTime.Add(time.Second))
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestWalletContractsAndCorrelation(t *testing.T) {
	wallet := mustWallet(t)
	service := &fakeService{}
	service.openWalletFn = func(_ context.Context, input application.OpenWalletRequest) (*domain.Wallet, error) {
		if input.PlayerID != "player-1" || input.InitialBalance.String() != "100.00" || input.CorrelationID != "request-123" {
			t.Fatalf("OpenWallet input = %+v", input)
		}
		return wallet, nil
	}
	service.getWalletFn = func(_ context.Context, id string) (*domain.Wallet, error) {
		if id != walletID {
			t.Fatalf("wallet id = %q", id)
		}
		return wallet, nil
	}
	difference, _ := domain.NewMoney(0, wallet.Currency())
	service.reconcileFn = func(context.Context, string) (postgres.Reconciliation, error) {
		return postgres.Reconciliation{WalletBalance: wallet.Balance(), LedgerBalance: wallet.Balance(), Difference: difference, EntryCount: 1, Consistent: true}, nil
	}
	handler := testHandler(t, service, Options{}, nil, nil)

	created := request(handler, http.MethodPost, "/wallets", "internal", `{"playerId":"player-1","initialBalance":{"amount":"100.00","currency":"BRL"}}`, map[string]string{correlationHeader: "request-123"})
	if created.Code != http.StatusCreated || created.Header().Get(correlationHeader) != "request-123" {
		t.Fatalf("create status/header = %d/%q; body=%s", created.Code, created.Header().Get(correlationHeader), created.Body)
	}
	assertJSONContractFields(t, created.Body.Bytes(), []string{"id", "playerId", "balance", "version"}, []string{"walletId"})
	if !strings.Contains(created.Body.String(), `"id":"`+walletID+`"`) ||
		!strings.Contains(created.Body.String(), `"balance":{"amount":"100.00","currency":"BRL"}`) {
		t.Fatalf("create body does not contain exact money: %s", created.Body)
	}

	got := request(handler, http.MethodGet, "/wallets/"+walletID, "internal", "", nil)
	assertJSONContractFields(t, got.Body.Bytes(), []string{"id", "playerId", "balance", "version"}, []string{"walletId"})
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"id":"`+walletID+`"`) {
		t.Fatalf("get wallet response = %d %s", got.Code, got.Body)
	}
	reconciled := request(handler, http.MethodPost, "/wallets/"+walletID+"/reconciliation", "internal", "", nil)
	assertJSONContractFields(t, reconciled.Body.Bytes(), []string{"walletId", "storedBalance", "calculatedBalance", "difference", "consistent", "checkedEntries"}, []string{"walletBalance", "ledgerBalance", "entryCount"})
	if reconciled.Code != http.StatusOK || !strings.Contains(reconciled.Body.String(), `"walletId":"`+walletID+`"`) ||
		!strings.Contains(reconciled.Body.String(), `"storedBalance":{"amount":"100.00"`) ||
		!strings.Contains(reconciled.Body.String(), `"calculatedBalance":{"amount":"100.00"`) ||
		!strings.Contains(reconciled.Body.String(), `"difference":{"amount":"0.00"`) ||
		!strings.Contains(reconciled.Body.String(), `"consistent":true`) ||
		!strings.Contains(reconciled.Body.String(), `"checkedEntries":1`) {
		t.Fatalf("reconciliation response = %d %s", reconciled.Code, reconciled.Body)
	}
}

func TestAuthenticationAndInternalAuthorizationUseStableErrors(t *testing.T) {
	handler := testHandler(t, &fakeService{}, Options{NewCorrelationID: func() string { return "generated-id" }}, nil, nil)
	tests := []struct {
		name  string
		token string
		want  int
		code  string
	}{
		{name: "missing token", want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		{name: "provider on wallet", token: "provider-a", want: http.StatusForbidden, code: "FORBIDDEN"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := request(handler, http.MethodGet, "/wallets/"+walletID, test.token, "", nil)
			if response.Code != test.want || !strings.Contains(response.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			if response.Header().Get(correlationHeader) != "generated-id" {
				t.Fatalf("correlation header = %q", response.Header().Get(correlationHeader))
			}
		})
	}
}

func TestStrictJSONAndBodyLimit(t *testing.T) {
	var calls atomic.Int32
	service := &fakeService{openWalletFn: func(context.Context, application.OpenWalletRequest) (*domain.Wallet, error) {
		calls.Add(1)
		return mustWallet(t), nil
	}}
	handler := testHandler(t, service, Options{MaxBodyBytes: 100}, nil, nil)
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "malformed", body: `{`, want: http.StatusBadRequest},
		{name: "unknown", body: `{"playerId":"p","initialBalance":{"amount":"0.00","currency":"BRL"},"secret":"value"}`, want: http.StatusBadRequest},
		{name: "nested unknown", body: `{"playerId":"p","initialBalance":{"amount":"0.00","currency":"BRL","extra":1}}`, want: http.StatusBadRequest},
		{name: "trailing", body: `{"playerId":"p","initialBalance":{"amount":"0.00","currency":"BRL"}} {}`, want: http.StatusBadRequest},
		{name: "large", body: `{"playerId":"` + strings.Repeat("x", 200) + `","initialBalance":{"amount":"0.00","currency":"BRL"}}`, want: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := request(handler, http.MethodPost, "/wallets", "internal", test.body, nil)
			if response.Code != test.want || !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), strings.Repeat("x", 20)) {
				t.Fatalf("response leaked input: %s", response.Body)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("service calls = %d, want 0", calls.Load())
	}
}

func TestLedgerDefaultLimitAndOpaqueCursorRoundTrip(t *testing.T) {
	wallet := mustWallet(t)
	cursor := postgres.LedgerCursor{CreatedAt: testTime, ID: ledgerID}
	var calls atomic.Int32
	service := &fakeService{
		getWalletFn: func(context.Context, string) (*domain.Wallet, error) { return wallet, nil },
		listLedgerFn: func(_ context.Context, _ string, received *postgres.LedgerCursor, limit int) (postgres.LedgerPage, error) {
			call := calls.Add(1)
			if limit != DefaultLedgerLimit {
				t.Fatalf("limit = %d", limit)
			}
			if call == 1 {
				if received != nil {
					t.Fatalf("first cursor = %+v", received)
				}
				return postgres.LedgerPage{Entries: []domain.WalletLedgerEntry{}, NextCursor: &cursor}, nil
			}
			if received == nil || !received.CreatedAt.Equal(cursor.CreatedAt) || received.ID != cursor.ID {
				t.Fatalf("decoded cursor = %+v", received)
			}
			return postgres.LedgerPage{Entries: []domain.WalletLedgerEntry{}}, nil
		},
	}
	handler := testHandler(t, service, Options{}, nil, nil)
	first := request(handler, http.MethodGet, "/wallets/"+walletID+"/ledger", "internal", "", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("first response = %d %s", first.Code, first.Body)
	}
	var page ledgerResponse
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == "" || strings.Contains(page.NextCursor, ledgerID) {
		t.Fatalf("next cursor = %q, decode error = %v", page.NextCursor, err)
	}
	second := request(handler, http.MethodGet, "/wallets/"+walletID+"/ledger?cursor="+page.NextCursor, "internal", "", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("second response = %d %s", second.Code, second.Body)
	}
	invalid := request(handler, http.MethodGet, "/wallets/"+walletID+"/ledger?cursor=not-base64", "internal", "", nil)
	if invalid.Code != http.StatusBadRequest || calls.Load() != 2 {
		t.Fatalf("invalid cursor response/calls = %d/%d", invalid.Code, calls.Load())
	}
}

func TestTransactionStatusesReplayAndFailureCode(t *testing.T) {
	tests := []struct {
		name    string
		state   domain.WagerTransactionState
		failure domain.FailureCode
		replay  bool
		want    int
	}{
		{name: "processed", state: domain.TransactionStateProcessed, want: http.StatusOK},
		{name: "replay", state: domain.TransactionStateProcessed, replay: true, want: http.StatusOK},
		{name: "pending reference", state: domain.TransactionStatePendingReference, want: http.StatusAccepted},
		{name: "business rejection", state: domain.TransactionStateRejected, failure: domain.FailureInsufficientFunds, want: http.StatusConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeService{processTransactionFn: func(_ context.Context, input application.ProcessTransactionRequest) (application.TransactionResult, error) {
				if input.IdempotencyKey != "key-1" || input.Command.ProviderID() != "provider-a" {
					t.Fatalf("process input = %+v", input)
				}
				return application.TransactionResult{Transaction: mustTransaction(t, test.state, test.failure), IdempotentReplay: test.replay}, nil
			}}
			handler := testHandler(t, service, Options{}, nil, nil)
			kind := "BET"
			if test.state == domain.TransactionStatePendingReference {
				kind = "REFUND"
			}
			response := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", kind), map[string]string{"Idempotency-Key": "key-1"})
			if response.Code != test.want || !strings.Contains(response.Body.String(), `"idempotentReplay":`+strconvBool(test.replay)) {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			if test.failure != "" && !strings.Contains(response.Body.String(), `"failureCode":"`+string(test.failure)+`"`) {
				t.Fatalf("failure response = %s", response.Body)
			}
			if test.state == domain.TransactionStateProcessed {
				assertJSONContractFields(t, response.Body.Bytes(), []string{"transactionId", "balance", "idempotentReplay"}, []string{"resultBalance"})
			}
			if test.state == domain.TransactionStateProcessed && !strings.Contains(response.Body.String(), `"balance":{"amount":"90.00","currency":"BRL"}`) {
				t.Fatalf("result balance is not exact: %s", response.Body)
			}
		})
	}
}

func TestHTTPTransactionMetricsUseBoundedLabelsAndLogsSafeIdentifiers(t *testing.T) {
	metrics := observability.NewMetrics()
	var logs bytes.Buffer
	service := &fakeService{processTransactionFn: func(_ context.Context, _ application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{
			Transaction:      mustTransaction(t, domain.TransactionStateProcessed, ""),
			IdempotentReplay: true,
		}, nil
	}}
	handler := testHandler(t, service, Options{
		Metrics: metrics,
		Logger:  observability.NewJSONLogger(&logs, slog.LevelInfo),
	}, nil, nil)
	response := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", "BET"), map[string]string{
		"Idempotency-Key": idempotencySecret,
		correlationHeader: "correlation-1",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("response = %d %s", response.Code, response.Body)
	}

	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	wantFamilies := map[string]bool{
		"wager_transaction_results_total":  false,
		"wager_duplicate_deliveries_total": false,
		"wager_processing_latency_seconds": false,
	}
	allowedLabels := map[string]bool{"kind": true, "status": true, "code": true}
	for _, family := range families {
		if _, ok := wantFamilies[family.GetName()]; !ok {
			continue
		}
		wantFamilies[family.GetName()] = true
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if !allowedLabels[label.GetName()] {
					t.Errorf("HTTP metric %q has disallowed label %q", family.GetName(), label.GetName())
				}
				for _, identifier := range []string{walletID, transactionID, "provider-a", "correlation-1"} {
					if label.GetValue() == identifier {
						t.Errorf("HTTP metric %q contains identifier label value %q", family.GetName(), identifier)
					}
				}
			}
		}
	}
	for name, found := range wantFamilies {
		if !found {
			t.Errorf("HTTP path did not record %q", name)
		}
	}

	logOutput := logs.String()
	for _, field := range []string{
		`"correlationId":"correlation-1"`,
		`"transactionId":"` + transactionID + `"`,
		`"walletId":"` + walletID + `"`,
		`"providerId":"provider-a"`,
	} {
		if !strings.Contains(logOutput, field) {
			t.Errorf("logs do not contain safe field %s: %s", field, logOutput)
		}
	}
	for _, sensitive := range []string{idempotencySecret, `"amount":"10.00"`, "external-1", "round-1", "game-1"} {
		if strings.Contains(logOutput, sensitive) {
			t.Errorf("logs contain sensitive request value %q: %s", sensitive, logOutput)
		}
	}
}

const idempotencySecret = "idempotency-secret"

func TestHTTPMetricsRecordConflictsAndReconciliationDivergence(t *testing.T) {
	conflicts := []struct {
		name string
		err  error
		want string
	}{
		{name: "idempotency", err: postgres.ErrIdempotencyConflict, want: "idempotency"},
		{name: "external transaction", err: postgres.ErrExternalIDConflict, want: "external_transaction"},
		{name: "concurrent update", err: postgres.ErrConcurrentUpdate, want: "concurrent_update"},
		{name: "serialization", err: postgres.ErrSerializationFailure, want: "serialization"},
		{name: "deadlock", err: postgres.ErrDeadlock, want: "deadlock"},
		{name: "constraint", err: postgres.ErrConstraintViolation, want: "constraint"},
		{name: "database conflict", err: postgres.ErrConflict, want: "database"},
	}
	for _, test := range conflicts {
		t.Run(test.name, func(t *testing.T) {
			metrics := &fakeMetrics{}
			service := &fakeService{processTransactionFn: func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
				return application.TransactionResult{}, test.err
			}}
			handler := testHandler(t, service, Options{Metrics: metrics}, nil, nil)
			response := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", "BET"), map[string]string{"Idempotency-Key": "key"})
			if response.Code != http.StatusConflict && response.Code != http.StatusServiceUnavailable {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			if len(metrics.conflicts) != 1 || metrics.conflicts[0] != test.want {
				t.Fatalf("conflict metrics = %v, want [%s]", metrics.conflicts, test.want)
			}
			if len(metrics.latencies) != 1 || metrics.latencies[0].kind != "BET" || metrics.latencies[0].status != "ERROR" {
				t.Fatalf("latency metrics = %+v", metrics.latencies)
			}
		})
	}

	metrics := &fakeMetrics{}
	var logs bytes.Buffer
	wallet := mustWallet(t)
	difference := mustMoney(t, "1.00")
	service := &fakeService{reconcileFn: func(context.Context, string) (postgres.Reconciliation, error) {
		return postgres.Reconciliation{
			WalletBalance: wallet.Balance(), LedgerBalance: mustMoney(t, "99.00"),
			Difference: difference, Consistent: false, EntryCount: 1,
		}, nil
	}}
	handler := testHandler(t, service, Options{
		Metrics: metrics,
		Logger:  observability.NewJSONLogger(&logs, slog.LevelInfo),
	}, nil, nil)
	response := request(handler, http.MethodPost, "/wallets/"+walletID+"/reconciliation", "internal", "", nil)
	if response.Code != http.StatusOK || metrics.divergences != 1 {
		t.Fatalf("reconciliation response/divergences = %d/%d, body=%s", response.Code, metrics.divergences, response.Body)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `"level":"WARN"`) ||
		!strings.Contains(logOutput, `"msg":"HTTP reconciliation divergence detected"`) ||
		!strings.Contains(logOutput, `"walletId":"`+walletID+`"`) {
		t.Fatalf("reconciliation divergence log missing warning or safe identifier: %s", logOutput)
	}
	for _, financialValue := range []string{`"amount"`, "100.00", "99.00", "1.00"} {
		if strings.Contains(logOutput, financialValue) {
			t.Errorf("reconciliation divergence log contains financial value %q: %s", financialValue, logOutput)
		}
	}
}

func strconvBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestTransactionValidationIsolationAndErrorMapping(t *testing.T) {
	var calls atomic.Int32
	service := &fakeService{
		processTransactionFn: func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
			calls.Add(1)
			return application.TransactionResult{}, errors.New("database password=secret")
		},
		getTransactionByExternalIDFn: func(context.Context, string, string) (postgres.TransactionRecord, error) {
			calls.Add(1)
			return postgres.TransactionRecord{}, nil
		},
	}
	handler := testHandler(t, service, Options{RetryAfter: 1500 * time.Millisecond}, nil, nil)

	missingKey := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", "BET"), nil)
	if missingKey.Code != http.StatusBadRequest {
		t.Fatalf("missing key response = %d %s", missingKey.Code, missingKey.Body)
	}
	bodyMismatch := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-b", "BET"), map[string]string{"Idempotency-Key": "key"})
	if bodyMismatch.Code != http.StatusForbidden {
		t.Fatalf("body mismatch response = %d %s", bodyMismatch.Code, bodyMismatch.Body)
	}
	pathMismatch := request(handler, http.MethodGet, "/providers/provider-a/wagering/transactions/external-1", "provider-b", "", nil)
	if pathMismatch.Code != http.StatusForbidden {
		t.Fatalf("path mismatch response = %d %s", pathMismatch.Code, pathMismatch.Body)
	}
	if calls.Load() != 0 {
		t.Fatalf("provider isolation made %d service calls", calls.Load())
	}

	unavailable := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", "BET"), map[string]string{"Idempotency-Key": "key"})
	if unavailable.Code != http.StatusServiceUnavailable || unavailable.Header().Get("Retry-After") != "2" || strings.Contains(unavailable.Body.String(), "password") {
		t.Fatalf("unavailable response = %d, Retry-After=%q, body=%s", unavailable.Code, unavailable.Header().Get("Retry-After"), unavailable.Body)
	}

	service.processTransactionFn = func(context.Context, application.ProcessTransactionRequest) (application.TransactionResult, error) {
		return application.TransactionResult{}, postgres.ErrIdempotencyConflict
	}
	conflict := request(handler, http.MethodPost, "/wagering/transactions", "provider-a", transactionBody("provider-a", "BET"), map[string]string{"Idempotency-Key": "key"})
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "IDEMPOTENCY_CONFLICT") {
		t.Fatalf("conflict response = %d %s", conflict.Code, conflict.Body)
	}
}

func TestTransactionQueries(t *testing.T) {
	tx := mustTransaction(t, domain.TransactionStateProcessed, "")
	service := &fakeService{
		getTransactionFn: func(_ context.Context, id string) (postgres.TransactionRecord, error) {
			if id != transactionID {
				t.Fatalf("transaction id = %q", id)
			}
			return postgres.TransactionRecord{Transaction: tx}, nil
		},
		getTransactionByExternalIDFn: func(_ context.Context, providerID, externalID string) (postgres.TransactionRecord, error) {
			if providerID != "provider-a" || externalID != "external-1" {
				t.Fatalf("provider lookup = %q/%q", providerID, externalID)
			}
			return postgres.TransactionRecord{Transaction: tx}, nil
		},
	}
	handler := testHandler(t, service, Options{}, nil, nil)
	internal := request(handler, http.MethodGet, "/wagering/transactions/"+transactionID, "internal", "", nil)
	provider := request(handler, http.MethodGet, "/providers/provider-a/wagering/transactions/external-1", "provider-a", "", nil)
	if internal.Code != http.StatusOK || provider.Code != http.StatusOK {
		t.Fatalf("query statuses = %d/%d", internal.Code, provider.Code)
	}
	for _, response := range []*httptest.ResponseRecorder{internal, provider} {
		assertJSONContractFields(t, response.Body.Bytes(), []string{"transactionId", "balance"}, []string{"resultBalance", "idempotentReplay"})
		if !strings.Contains(response.Body.String(), `"balance":{"amount":"90.00","currency":"BRL"}`) {
			t.Fatalf("query transaction balance contract = %s", response.Body)
		}
	}
	missing := &fakeService{getTransactionFn: func(context.Context, string) (postgres.TransactionRecord, error) {
		return postgres.TransactionRecord{}, postgres.ErrNotFound
	}}
	missingHandler := testHandler(t, missing, Options{}, nil, nil)
	notFound := request(missingHandler, http.MethodGet, "/wagering/transactions/"+transactionID, "internal", "", nil)
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("not found response = %d %s", notFound.Code, notFound.Body)
	}
}

func TestPanicRecoveryDoesNotLeak(t *testing.T) {
	service := &fakeService{getWalletFn: func(context.Context, string) (*domain.Wallet, error) {
		panic("sensitive stack value")
	}}
	handler := testHandler(t, service, Options{}, nil, nil)
	response := request(handler, http.MethodGet, "/wallets/"+walletID, "internal", "", nil)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "INTERNAL_ERROR") || strings.Contains(response.Body.String(), "sensitive") {
		t.Fatalf("panic response = %d %s", response.Code, response.Body)
	}
}

func assertJSONContractFields(t *testing.T, body []byte, present, absent []string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode contract response: %v; body=%s", err, body)
	}
	for _, name := range present {
		if _, ok := fields[name]; !ok {
			t.Errorf("contract response is missing %q: %s", name, body)
		}
	}
	for _, name := range absent {
		if _, ok := fields[name]; ok {
			t.Errorf("contract response contains legacy field %q: %s", name, body)
		}
	}
}
