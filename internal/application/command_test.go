package application

import (
	"errors"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

func baseCommandInput() TransactionCommandInput {
	return TransactionCommandInput{
		ProviderID: "provider-1", ExternalTransactionID: "external-1",
		PlayerID: "player-1", WalletID: "wallet-1", RoundID: "round-1",
		GameID: "game-1", Kind: "BET", Amount: "25.00", Currency: "BRL",
	}
}

func TestCanonicalTransactionCommandGolden(t *testing.T) {
	command, err := NormalizeTransactionCommand(baseCommandInput())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := command.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"externalTransactionId":"external-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"player-1","providerId":"provider-1","roundId":"round-1","walletId":"wallet-1"}`
	if string(canonical) != wantJSON {
		t.Fatalf("canonical JSON:\n got %s\nwant %s", canonical, wantJSON)
	}
	hash, err := command.SHA256Hex()
	if err != nil {
		t.Fatal(err)
	}
	if want := "0b0d83f0c7ad282cc057bade61932bc91a85ef7f732e1acd8e6bd67c3df96197"; hash != want {
		t.Fatalf("hash = %s, want %s", hash, want)
	}
}

func TestEquivalentHTTPAndSQSInputsHaveSameHash(t *testing.T) {
	httpInput := baseCommandInput()
	sqsInput := baseCommandInput()
	sqsInput.Kind = "bet"
	sqsInput.Currency = "brl"
	sqsInput.Amount = "025.00"

	httpHash, err := CanonicalTransactionCommandHash(httpInput)
	if err != nil {
		t.Fatal(err)
	}
	sqsHash, err := CanonicalTransactionCommandHash(sqsInput)
	if err != nil {
		t.Fatal(err)
	}
	if httpHash != sqsHash {
		t.Fatalf("equivalent hashes differ: %s != %s", httpHash, sqsHash)
	}
}

func TestCanonicalReferenceFieldAndDomainDetails(t *testing.T) {
	input := baseCommandInput()
	input.Kind = "rollback"
	input.ReferenceExternalTransactionID = "original-1"
	command, err := NormalizeTransactionCommand(input)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := command.CanonicalJSON()
	want := `{"externalTransactionId":"external-1","gameId":"game-1","kind":"ROLLBACK","money":{"amount":"25.00","currency":"BRL"},"playerId":"player-1","providerId":"provider-1","referenceExternalTransactionId":"original-1","roundId":"round-1","walletId":"wallet-1"}`
	if string(canonical) != want {
		t.Fatalf("canonical reversal = %s", canonical)
	}
	details := command.DomainDetails()
	if details.Kind != domain.TransactionKindRollback || details.ReferenceExternalTransactionID != "original-1" || details.Money.String() != "25.00" {
		t.Fatalf("unexpected domain details: %+v", details)
	}
}

func TestNormalizeTransactionCommandAcceptsOptionalWinReference(t *testing.T) {
	for _, reference := range []string{"", "bet-1"} {
		input := baseCommandInput()
		input.Kind = "WIN"
		input.ReferenceExternalTransactionID = reference
		command, err := NormalizeTransactionCommand(input)
		if err != nil {
			t.Fatalf("WIN reference %q: %v", reference, err)
		}
		got, present := command.ReferenceExternalTransactionID()
		if got != reference || present != (reference != "") {
			t.Fatalf("WIN reference = %q/%t", got, present)
		}
	}
}

func TestNormalizeTransactionCommandRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TransactionCommandInput)
	}{
		{"empty identifier", func(input *TransactionCommandInput) { input.WalletID = "" }},
		{"surrounding whitespace", func(input *TransactionCommandInput) { input.ProviderID = " provider-1" }},
		{"external opening", func(input *TransactionCommandInput) { input.Kind = "OPENING" }},
		{"scientific money", func(input *TransactionCommandInput) { input.Amount = "2.5e1" }},
		{"zero bet", func(input *TransactionCommandInput) { input.Amount = "0.00" }},
		{"reference on bet", func(input *TransactionCommandInput) { input.ReferenceExternalTransactionID = "other" }},
		{"reference on loss", func(input *TransactionCommandInput) {
			input.Kind = "LOSS"
			input.Amount = "0.00"
			input.ReferenceExternalTransactionID = "other"
		}},
		{"missing refund reference", func(input *TransactionCommandInput) { input.Kind = "REFUND" }},
		{"missing rollback reference", func(input *TransactionCommandInput) { input.Kind = "ROLLBACK" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := baseCommandInput()
			test.mutate(&input)
			if _, err := NormalizeTransactionCommand(input); !errors.Is(err, ErrInvalidTransactionCommand) {
				t.Fatalf("error = %v, want ErrInvalidTransactionCommand", err)
			}
		})
	}
}
