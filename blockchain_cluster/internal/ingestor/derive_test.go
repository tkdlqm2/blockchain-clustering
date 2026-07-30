package ingestor

import (
	"math/big"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

func delta(txid, addr string, amount int64) domain.BalanceDelta {
	return domain.BalanceDelta{
		ChainID: "bitcoin",
		TxID:    txid,
		Address: addr,
		Amount:  big.NewInt(amount),
	}
}

func TestGroupByTx(t *testing.T) {
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", -10),
		delta("tx2", "D", -5),
		delta("tx1", "B", -20),
		delta("tx1", "C", 30),
	}

	groups := GroupByTx(deltas)
	if len(groups) != 2 {
		t.Fatalf("expected 2 tx groups, got %d", len(groups))
	}
	// First-occurrence order: tx1 appeared before tx2 in the input.
	if groups[0].Key != (TxKey{ChainID: "bitcoin", TxID: "tx1"}) {
		t.Fatalf("expected tx1 first (first-occurrence order), got %+v", groups[0].Key)
	}
	if len(groups[0].Deltas) != 3 {
		t.Fatalf("expected 3 deltas in tx1, got %d", len(groups[0].Deltas))
	}
	if groups[1].Key != (TxKey{ChainID: "bitcoin", TxID: "tx2"}) {
		t.Fatalf("expected tx2 second, got %+v", groups[1].Key)
	}
}

func TestGroupByTx_DeterministicAcrossRuns(t *testing.T) {
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", -10),
		delta("tx2", "D", -5),
		delta("tx3", "E", -1),
	}
	first := GroupByTx(deltas)
	second := GroupByTx(deltas)
	for i := range first {
		if first[i].Key != second[i].Key {
			t.Fatalf("GroupByTx order not stable across calls: %v vs %v", first, second)
		}
	}
}

func TestSpentAddresses_OnlyNegativeAndDeduped(t *testing.T) {
	txDeltas := []domain.BalanceDelta{
		delta("tx1", "A", -10),
		delta("tx1", "B", -20),
		delta("tx1", "A", -5), // same address spent twice in one tx
		delta("tx1", "C", 30), // received, not spent
	}

	spent := SpentAddresses(txDeltas)
	if len(spent) != 2 {
		t.Fatalf("expected 2 distinct spent addresses, got %v", spent)
	}
	if spent[0] != "A" || spent[1] != "B" {
		t.Fatalf("expected first-occurrence order [A B], got %v", spent)
	}
}

func TestReceivedEntries_OnlyPositive(t *testing.T) {
	txDeltas := []domain.BalanceDelta{
		delta("tx1", "A", -10),
		delta("tx1", "C", 30),
		delta("tx1", "D", 40),
	}

	received := ReceivedEntries(txDeltas)
	if len(received) != 2 {
		t.Fatalf("expected 2 received entries, got %v", received)
	}
	if received[0].Address != "C" || received[0].Amount.Cmp(big.NewInt(30)) != 0 {
		t.Fatalf("unexpected first entry: %+v", received[0])
	}
}

func TestSpentAddresses_ZeroAmountExcluded(t *testing.T) {
	txDeltas := []domain.BalanceDelta{delta("tx1", "A", 0)}
	if got := SpentAddresses(txDeltas); len(got) != 0 {
		t.Fatalf("zero-amount delta should be neither spent nor received, got spent=%v", got)
	}
	if got := ReceivedEntries(txDeltas); len(got) != 0 {
		t.Fatalf("zero-amount delta should be neither spent nor received, got received=%v", got)
	}
}
