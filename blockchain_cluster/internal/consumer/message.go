// Package consumer implements the Kafka consumer loop that was the last
// missing piece connecting the indexer (a separate project — see
// docs/08-indexer-contract.md) to this system's pipeline. Everything it
// calls (Ingestor, Pipeline, ReorgHandler) already existed and was tested;
// this package is only the wiring between "a message arrived on
// balance-deltas" and "call the right Go function."
package consumer

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// ReorgNotice mirrors docs/08 §3.2's reorg event.
type ReorgNotice struct {
	ChainID               string
	RolledBackBlockHashes []string
}

type messageEnvelope struct {
	Type string `json:"type"`
}

// balanceDeltaPayload mirrors docs/08 §3.1 exactly (field names, and the
// contract's hard requirement that Amount is a JSON string — see §4 there —
// never a bare JSON number, which would already have lost uint256 precision
// by the time it reached us).
type balanceDeltaPayload struct {
	ChainID     string          `json:"chain_id"`
	TxID        string          `json:"txid"`
	DeltaIndex  int32           `json:"delta_index"`
	Address     string          `json:"address"`
	Amount      string          `json:"amount"`
	Kind        string          `json:"kind"`
	BlockHeight int64           `json:"block_height"`
	BlockHash   string          `json:"block_hash"`
	Meta        json.RawMessage `json:"meta,omitempty"`
}

type reorgPayload struct {
	ChainID               string   `json:"chain_id"`
	RolledBackBlockHashes []string `json:"rolled_back_block_hashes"`
}

// ParseMessage decodes one balance-deltas topic message. Exactly one of the
// two return values is non-nil on success. This is a pure function — no
// Kafka or DB dependency — so the envelope/payload shapes stay testable
// without a broker.
func ParseMessage(raw []byte) (*domain.BalanceDelta, *ReorgNotice, error) {
	var head messageEnvelope
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, nil, fmt.Errorf("consumer: decode envelope: %w", err)
	}

	switch head.Type {
	case "balance_delta":
		var p balanceDeltaPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, nil, fmt.Errorf("consumer: decode balance_delta: %w", err)
		}
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return nil, nil, fmt.Errorf("consumer: amount %q is not an integer string (docs/08 §4)", p.Amount)
		}
		return &domain.BalanceDelta{
			ChainID:     p.ChainID,
			TxID:        p.TxID,
			DeltaIndex:  p.DeltaIndex,
			Address:     p.Address,
			Amount:      amount,
			Kind:        p.Kind,
			BlockHeight: p.BlockHeight,
			BlockHash:   p.BlockHash,
			Meta:        p.Meta,
		}, nil, nil

	case "reorg":
		var p reorgPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, nil, fmt.Errorf("consumer: decode reorg: %w", err)
		}
		return nil, &ReorgNotice{ChainID: p.ChainID, RolledBackBlockHashes: p.RolledBackBlockHashes}, nil

	default:
		return nil, nil, fmt.Errorf("consumer: unknown message type %q", head.Type)
	}
}
