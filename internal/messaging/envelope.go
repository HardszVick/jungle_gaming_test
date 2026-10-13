package messaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/application"
)

const WagerTransactionRequested = "WagerTransactionRequested"

var ErrInvalidEnvelope = errors.New("messaging: invalid WagerTransactionRequested envelope")

type ParsedEnvelope struct {
	MessageID      string
	OccurredAt     time.Time
	IdempotencyKey string
	Command        application.TransactionCommand
	PayloadHash    [sha256.Size]byte
}

type wagerTransactionRequestedEnvelope struct {
	MessageID  string                        `json:"messageId"`
	Type       string                        `json:"type"`
	OccurredAt string                        `json:"occurredAt"`
	Data       wagerTransactionRequestedData `json:"data"`
}

type wagerTransactionRequestedData struct {
	IdempotencyKey                 string `json:"idempotencyKey"`
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          money  `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ParseWagerTransactionRequested accepts only the spec envelope. In particular,
// the older README eventType/version shape is rejected as unknown fields.
func ParseWagerTransactionRequested(body []byte) (ParsedEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope wagerTransactionRequestedEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if envelope.MessageID == "" || strings.TrimSpace(envelope.MessageID) != envelope.MessageID {
		return ParsedEnvelope{}, fmt.Errorf("%w: messageId is required without surrounding whitespace", ErrInvalidEnvelope)
	}
	if envelope.Type != WagerTransactionRequested {
		return ParsedEnvelope{}, fmt.Errorf("%w: type must be %s", ErrInvalidEnvelope, WagerTransactionRequested)
	}
	if envelope.Data.IdempotencyKey == "" || strings.TrimSpace(envelope.Data.IdempotencyKey) != envelope.Data.IdempotencyKey {
		return ParsedEnvelope{}, fmt.Errorf("%w: data.idempotencyKey is required without surrounding whitespace", ErrInvalidEnvelope)
	}
	if uuid.Validate(envelope.Data.PlayerID) != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: data.playerId must be a UUID", ErrInvalidEnvelope)
	}
	if uuid.Validate(envelope.Data.WalletID) != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: data.walletId must be a UUID", ErrInvalidEnvelope)
	}
	if !strings.HasSuffix(envelope.OccurredAt, "Z") {
		return ParsedEnvelope{}, fmt.Errorf("%w: occurredAt must be RFC3339 UTC", ErrInvalidEnvelope)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt)
	if err != nil || occurredAt.IsZero() {
		return ParsedEnvelope{}, fmt.Errorf("%w: occurredAt must be RFC3339 UTC", ErrInvalidEnvelope)
	}
	command, err := application.NormalizeTransactionCommand(application.TransactionCommandInput{
		ProviderID: envelope.Data.ProviderID, ExternalTransactionID: envelope.Data.ExternalTransactionID,
		PlayerID: envelope.Data.PlayerID, WalletID: envelope.Data.WalletID, RoundID: envelope.Data.RoundID,
		GameID: envelope.Data.GameID, Kind: envelope.Data.Kind, Amount: envelope.Data.Money.Amount,
		Currency:                       envelope.Data.Money.Currency,
		ReferenceExternalTransactionID: envelope.Data.ReferenceExternalTransactionID,
	})
	if err != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	canonical, err := canonicalEnvelope(envelope.MessageID, occurredAt, envelope.Data.IdempotencyKey, command)
	if err != nil {
		return ParsedEnvelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	return ParsedEnvelope{
		MessageID: envelope.MessageID, OccurredAt: occurredAt.UTC(),
		IdempotencyKey: envelope.Data.IdempotencyKey, Command: command,
		PayloadHash: sha256.Sum256(canonical),
	}, nil
}

func canonicalEnvelope(messageID string, occurredAt time.Time, idempotencyKey string, command application.TransactionCommand) ([]byte, error) {
	reference, _ := command.ReferenceExternalTransactionID()
	return json.Marshal(wagerTransactionRequestedEnvelope{
		MessageID: messageID, Type: WagerTransactionRequested,
		OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano),
		Data: wagerTransactionRequestedData{
			IdempotencyKey: idempotencyKey, ProviderID: command.ProviderID(),
			ExternalTransactionID: command.ExternalTransactionID(), PlayerID: command.PlayerID(),
			WalletID: command.WalletID(), RoundID: command.RoundID(), GameID: command.GameID(),
			Kind:                           string(command.Kind()),
			Money:                          money{Amount: command.Money().String(), Currency: command.Money().Currency().String()},
			ReferenceExternalTransactionID: reference,
		},
	})
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
