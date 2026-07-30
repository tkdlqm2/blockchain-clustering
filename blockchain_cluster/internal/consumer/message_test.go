package consumer

import (
	"math/big"
	"testing"
)

func TestParseMessage_BalanceDelta(t *testing.T) {
	raw := []byte(`{
		"type": "balance_delta",
		"chain_id": "ethereum",
		"txid": "0xabc",
		"delta_index": 1,
		"address": "0xdead",
		"amount": "-123456789012345678901234567890",
		"kind": "token",
		"block_height": 100,
		"block_hash": "0xblock1",
		"meta": {"token_contract": "0xusdc"}
	}`)

	delta, reorg, err := ParseMessage(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if reorg != nil {
		t.Fatalf("expected no reorg notice, got %+v", reorg)
	}
	if delta == nil {
		t.Fatalf("expected a delta")
	}
	want, _ := new(big.Int).SetString("-123456789012345678901234567890", 10)
	if delta.Amount.Cmp(want) != 0 {
		t.Fatalf("expected amount %s, got %s (precision must survive a 30-digit string)", want, delta.Amount)
	}
	if delta.ChainID != "ethereum" || delta.TxID != "0xabc" || delta.DeltaIndex != 1 {
		t.Fatalf("unexpected delta: %+v", delta)
	}
	if string(delta.Meta) != `{"token_contract": "0xusdc"}` {
		t.Fatalf("expected meta preserved opaquely, got %s", delta.Meta)
	}
}

func TestParseMessage_Reorg(t *testing.T) {
	raw := []byte(`{
		"type": "reorg",
		"chain_id": "bitcoin",
		"rolled_back_block_hashes": ["h1", "h2"]
	}`)

	delta, reorg, err := ParseMessage(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if delta != nil {
		t.Fatalf("expected no delta, got %+v", delta)
	}
	if reorg == nil || reorg.ChainID != "bitcoin" || len(reorg.RolledBackBlockHashes) != 2 {
		t.Fatalf("unexpected reorg notice: %+v", reorg)
	}
}

func TestParseMessage_RejectsNonStringAmount(t *testing.T) {
	// A bare JSON number here would already have lost precision before
	// reaching us for large values — the contract requires a string, and we
	// enforce it rather than silently accepting a number.
	raw := []byte(`{"type":"balance_delta","chain_id":"x","txid":"t","address":"a","amount":"not-a-number","kind":"native","block_height":1,"block_hash":"b"}`)
	_, _, err := ParseMessage(raw)
	if err == nil {
		t.Fatalf("expected an error for a non-integer amount string")
	}
}

func TestParseMessage_UnknownType(t *testing.T) {
	raw := []byte(`{"type":"something_else"}`)
	_, _, err := ParseMessage(raw)
	if err == nil {
		t.Fatalf("expected an error for an unknown message type")
	}
}

func TestParseMessage_MalformedJSON(t *testing.T) {
	_, _, err := ParseMessage([]byte(`not json at all`))
	if err == nil {
		t.Fatalf("expected an error for malformed JSON")
	}
}
