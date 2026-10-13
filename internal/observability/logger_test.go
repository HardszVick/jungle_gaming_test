package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

type unsafeContextKey string

func TestLogIncludesOnlyAllowlistedContextIdentifiers(t *testing.T) {
	var output bytes.Buffer
	logger := NewJSONLogger(&output, slog.LevelInfo)
	ctx := context.WithValue(context.Background(), unsafeContextKey("token"), "secret-token")
	ctx = context.WithValue(ctx, unsafeContextKey("financialPayload"), `{"amount":"999.00"}`)
	ctx = context.WithValue(ctx, unsafeContextKey("arbitrary"), "must-not-be-logged")
	ctx = WithIdentifiers(ctx, Identifiers{
		CorrelationID: "correlation-1",
		MessageID:     "message-1",
		TransactionID: "transaction-1",
		WalletID:      "wallet-1",
		ProviderID:    "provider-1",
	})

	Log(ctx, logger, slog.LevelInfo, "transaction processed")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode JSON log: %v\n%s", err, output.String())
	}
	want := map[string]string{
		"correlationId": "correlation-1",
		"messageId":     "message-1",
		"transactionId": "transaction-1",
		"walletId":      "wallet-1",
		"providerId":    "provider-1",
	}
	for key, value := range want {
		if record[key] != value {
			t.Errorf("log field %q = %v, want %q", key, record[key], value)
		}
	}
	for _, key := range []string{"token", "financialPayload", "arbitrary", "amount"} {
		if _, exists := record[key]; exists {
			t.Errorf("unsafe context field %q was logged", key)
		}
	}
	for _, value := range []string{"secret-token", "999.00", "must-not-be-logged"} {
		if bytes.Contains(output.Bytes(), []byte(value)) {
			t.Errorf("unsafe context value %q was logged", value)
		}
	}
}

func TestWithIdentifiersMergesOnlyNonEmptyValues(t *testing.T) {
	ctx := WithIdentifiers(context.Background(), Identifiers{
		CorrelationID: "correlation-1",
		MessageID:     "message-1",
	})
	ctx = WithIdentifiers(ctx, Identifiers{
		CorrelationID: "correlation-2",
		TransactionID: "transaction-1",
	})

	got := IdentifiersFromContext(ctx)
	want := Identifiers{
		CorrelationID: "correlation-2",
		MessageID:     "message-1",
		TransactionID: "transaction-1",
	}
	if got != want {
		t.Fatalf("IdentifiersFromContext() = %+v, want %+v", got, want)
	}
}
