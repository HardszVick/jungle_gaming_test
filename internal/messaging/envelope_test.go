package messaging

import (
	"errors"
	"strings"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/application"
)

const (
	testPlayerID = "11111111-1111-4111-8111-111111111111"
	testWalletID = "22222222-2222-4222-8222-222222222222"
)

func TestParseWagerTransactionRequested(t *testing.T) {
	parsed, err := ParseWagerTransactionRequested([]byte(validEnvelope("WIN")))
	if err != nil {
		t.Fatalf("parse valid envelope: %v", err)
	}
	if parsed.MessageID != "message-1" || parsed.IdempotencyKey != "idem-1" {
		t.Fatalf("unexpected envelope identity: %+v", parsed)
	}
	if parsed.Command.ProviderID() != "provider-a" || parsed.Command.Kind() != "WIN" || parsed.Command.Money().String() != "25.00" {
		t.Fatalf("unexpected normalized command")
	}

	httpCommand, err := application.NormalizeTransactionCommand(application.TransactionCommandInput{
		ProviderID: "provider-a", ExternalTransactionID: "external-1", PlayerID: testPlayerID,
		WalletID: testWalletID, RoundID: "round-1", GameID: "game-1", Kind: "WIN",
		Amount: "25.00", Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantHash, _ := httpCommand.SHA256()
	gotHash, _ := parsed.Command.SHA256()
	if gotHash != wantHash {
		t.Fatalf("SQS and HTTP commands have different canonical hashes")
	}
}

func TestParseWagerTransactionRequestedCanonicalPayloadHash(t *testing.T) {
	first, err := ParseWagerTransactionRequested([]byte(validEnvelope("win")))
	if err != nil {
		t.Fatal(err)
	}
	secondBody := `{
		"data":{"money":{"currency":"brl","amount":"25.00"},"kind":"WIN","gameId":"game-1","roundId":"round-1","walletId":"` + testWalletID + `","playerId":"` + testPlayerID + `","externalTransactionId":"external-1","providerId":"provider-a","idempotencyKey":"idem-1"},
		"occurredAt":"2026-10-10T12:00:00.000Z","type":"WagerTransactionRequested","messageId":"message-1"}`
	second, err := ParseWagerTransactionRequested([]byte(secondBody))
	if err != nil {
		t.Fatal(err)
	}
	if first.PayloadHash != second.PayloadHash {
		t.Fatalf("equivalent validated envelopes must have the same hash")
	}
}

func TestParseWagerTransactionRequestedRejectsInvalidInput(t *testing.T) {
	tests := map[string]string{
		"opening":        validEnvelope("OPENING"),
		"invalid player": strings.Replace(validEnvelope("WIN"), testPlayerID, "player-1", 1),
		"invalid wallet": strings.Replace(validEnvelope("WIN"), testWalletID, "wallet-1", 1),
		"unknown field":  `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-10-10T12:00:00Z","data":{},"extra":true}`,
		"old README":     `{"messageId":"m","eventType":"WagerTransactionRequested","occurredAt":"2026-10-10T12:00:00Z","version":1,"data":{}}`,
		"wrong type":     `{"messageId":"m","type":"Other","occurredAt":"2026-10-10T12:00:00Z","data":{}}`,
		"non UTC":        `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-10-10T09:00:00-03:00","data":{}}`,
		"trailing JSON":  validEnvelope("WIN") + `{}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseWagerTransactionRequested([]byte(body))
			if !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("expected ErrInvalidEnvelope, got %v", err)
			}
		})
	}
}

func validEnvelope(kind string) string {
	return `{
		"messageId":"message-1",
		"type":"WagerTransactionRequested",
		"occurredAt":"2026-10-10T12:00:00Z",
		"data":{
			"idempotencyKey":"idem-1",
			"providerId":"provider-a",
			"externalTransactionId":"external-1",
			"playerId":"` + testPlayerID + `",
			"walletId":"` + testWalletID + `",
			"roundId":"round-1",
			"gameId":"game-1",
			"kind":"` + kind + `",
			"money":{"amount":"25.00","currency":"BRL"}
		}
	}`
}
