package heuristic

import (
	"context"
	"math/big"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type fakeHubs map[string]bool

func (f fakeHubs) IsHub(_ context.Context, _, address string) (bool, error) { return f[address], nil }

type fakeExclusions map[string]bool

func (f fakeExclusions) IsExcluded(_ context.Context, _, txid string) (bool, error) {
	return f[txid], nil
}

type fakeConfidence struct {
	confidence float64
	enabled    bool
}

func (f fakeConfidence) ConfidenceFor(_ context.Context, _, _ string) (float64, bool, error) {
	return f.confidence, f.enabled, nil
}

func d(txid, addr string, amount int64, blockHash string, height int64) domain.BalanceDelta {
	return domain.BalanceDelta{
		ChainID: "bitcoin", TxID: txid, Address: addr, Amount: big.NewInt(amount),
		BlockHash: blockHash, BlockHeight: height,
	}
}

func TestCommonInputEngine_ThreeInputsFormStarAroundAnchor(t *testing.T) {
	e := NewCommonInputEngine(fakeHubs{}, fakeExclusions{}, fakeConfidence{confidence: 0.95, enabled: true})

	deltas := []domain.BalanceDelta{
		d("tx1", "A", -10, "block1", 100),
		d("tx1", "B", -20, "block1", 100),
		d("tx1", "C", -30, "block1", 100),
		d("tx1", "D", 55, "block1", 100), // recipient, not a candidate
	}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 candidates (star around anchor A), got %d: %+v", len(got), got)
	}
	for _, c := range got {
		if c.AddressA != "A" {
			t.Fatalf("expected anchor A (first spender), got %+v", c)
		}
		if c.HeuristicKey != "common-input" {
			t.Fatalf("wrong heuristic key: %+v", c)
		}
		if c.Confidence != 0.95 {
			t.Fatalf("expected confidence from registry (0.95), got %v", c.Confidence)
		}
		if c.SourceBlockHash == nil || *c.SourceBlockHash != "block1" {
			t.Fatalf("expected source_block_hash to be preserved, got %+v", c)
		}
	}
}

func TestCommonInputEngine_SingleInputProducesNoCandidate(t *testing.T) {
	e := NewCommonInputEngine(fakeHubs{}, fakeExclusions{}, fakeConfidence{confidence: 0.95, enabled: true})
	deltas := []domain.BalanceDelta{d("tx1", "A", -10, "block1", 100)}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no candidates for a single-input tx, got %v", got)
	}
}

func TestCommonInputEngine_HubSpenderExcluded(t *testing.T) {
	// A, B, C spend together but B is a hub — B should be filtered out of
	// `ins` before the anchor is even picked (docs/03 §4).
	e := NewCommonInputEngine(fakeHubs{"B": true}, fakeExclusions{}, fakeConfidence{confidence: 0.95, enabled: true})
	deltas := []domain.BalanceDelta{
		d("tx1", "A", -10, "block1", 100),
		d("tx1", "B", -20, "block1", 100),
		d("tx1", "C", -30, "block1", 100),
	}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 candidate (A-C, hub B excluded), got %d: %+v", len(got), got)
	}
	if got[0].AddressA != "A" || got[0].AddressB != "C" {
		t.Fatalf("expected A-C, got %+v", got[0])
	}
}

func TestCommonInputEngine_ExcludedTxProducesNoCandidates(t *testing.T) {
	e := NewCommonInputEngine(fakeHubs{}, fakeExclusions{"tx1": true}, fakeConfidence{confidence: 0.95, enabled: true})
	deltas := []domain.BalanceDelta{
		d("tx1", "A", -10, "block1", 100),
		d("tx1", "B", -20, "block1", 100),
	}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected excluded tx to produce no candidates, got %v", got)
	}
}

func TestCommonInputEngine_DisabledForChainProducesNoCandidates(t *testing.T) {
	e := NewCommonInputEngine(fakeHubs{}, fakeExclusions{}, fakeConfidence{enabled: false})
	deltas := []domain.BalanceDelta{
		d("tx1", "A", -10, "block1", 100),
		d("tx1", "B", -20, "block1", 100),
	}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected disabled heuristic to produce no candidates, got %v", got)
	}
}

func TestCommonInputEngine_MultipleTxDeterministicOrder(t *testing.T) {
	e := NewCommonInputEngine(fakeHubs{}, fakeExclusions{}, fakeConfidence{confidence: 0.95, enabled: true})
	deltas := []domain.BalanceDelta{
		d("tx1", "A", -10, "block1", 100),
		d("tx1", "B", -20, "block1", 100),
		d("tx2", "C", -10, "block1", 100),
		d("tx2", "D", -20, "block1", 100),
	}

	first, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	second, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("expected 2 candidates each run, got %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i].AddressA != second[i].AddressA || first[i].AddressB != second[i].AddressB {
			t.Fatalf("candidate order not stable across runs: %+v vs %+v", first, second)
		}
	}
}
