package observability

import (
	"context"
	"io"
	"log/slog"
)

// Identifiers is the complete allowlist of values that may be copied from a
// context into a log record. It deliberately has no generic attributes map.
type Identifiers struct {
	CorrelationID string
	MessageID     string
	TransactionID string
	WalletID      string
	ProviderID    string
}

type identifiersContextKey struct{}

// NewJSONLogger creates a logger without changing slog's process-wide default.
func NewJSONLogger(writer io.Writer, level slog.Leveler) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: level}))
}

// WithIdentifiers returns a context containing the non-empty identifiers in
// ids. Existing identifiers are retained when the corresponding value is empty.
func WithIdentifiers(ctx context.Context, ids Identifiers) context.Context {
	current := IdentifiersFromContext(ctx)
	if ids.CorrelationID != "" {
		current.CorrelationID = ids.CorrelationID
	}
	if ids.MessageID != "" {
		current.MessageID = ids.MessageID
	}
	if ids.TransactionID != "" {
		current.TransactionID = ids.TransactionID
	}
	if ids.WalletID != "" {
		current.WalletID = ids.WalletID
	}
	if ids.ProviderID != "" {
		current.ProviderID = ids.ProviderID
	}
	return context.WithValue(ctx, identifiersContextKey{}, current)
}

// IdentifiersFromContext returns only identifiers previously stored through
// WithIdentifiers. Other context values are intentionally ignored.
func IdentifiersFromContext(ctx context.Context) Identifiers {
	ids, _ := ctx.Value(identifiersContextKey{}).(Identifiers)
	return ids
}

// EnrichLogger attaches the safe identifiers available in ctx to logger.
func EnrichLogger(ctx context.Context, logger *slog.Logger) *slog.Logger {
	ids := IdentifiersFromContext(ctx)
	attributes := make([]any, 0, 5)
	if ids.CorrelationID != "" {
		attributes = append(attributes, slog.String("correlationId", ids.CorrelationID))
	}
	if ids.MessageID != "" {
		attributes = append(attributes, slog.String("messageId", ids.MessageID))
	}
	if ids.TransactionID != "" {
		attributes = append(attributes, slog.String("transactionId", ids.TransactionID))
	}
	if ids.WalletID != "" {
		attributes = append(attributes, slog.String("walletId", ids.WalletID))
	}
	if ids.ProviderID != "" {
		attributes = append(attributes, slog.String("providerId", ids.ProviderID))
	}
	return logger.With(attributes...)
}

// Log emits a message with safe context identifiers and no caller-supplied
// attributes, preventing this helper from accepting tokens or raw payloads.
func Log(ctx context.Context, logger *slog.Logger, level slog.Level, message string) {
	EnrichLogger(ctx, logger).Log(ctx, level, message)
}
