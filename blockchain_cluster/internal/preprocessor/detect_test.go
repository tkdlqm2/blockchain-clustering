package preprocessor

import (
	"math/big"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

func delta(txid, addr string, amount int64) domain.BalanceDelta {
	return domain.BalanceDelta{ChainID: "bitcoin", TxID: txid, Address: addr, Amount: big.NewInt(amount)}
}

func defaultParams() domain.PreprocessingParams {
	return domain.PreprocessingParams{
		DustThreshold:           big.NewInt(546),
		CoinjoinConfidence:      0.8,
		DustExclusionConfidence: 0.8,
		EqualOutputMin:          3,
		CollabInputMin:          3,
		CollabOutputMin:         3,
	}
}

func TestDetectCollaborativeTx_FlagsEqualOutputPattern(t *testing.T) {
	// 3 inputs, 4 outputs, 3 of them equal-value (a classic CoinJoin shape).
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", -1000), delta("tx1", "B", -1000), delta("tx1", "C", -1000),
		delta("tx1", "X", 300), delta("tx1", "Y", 300), delta("tx1", "Z", 300),
		delta("tx1", "change", 2050),
	}

	got := DetectCollaborativeTx(deltas, defaultParams())
	if len(got) != 1 {
		t.Fatalf("expected tx1 flagged as collaborative, got %d detections: %+v", len(got), got)
	}
	if got[0].TxID != "tx1" || got[0].Confidence != 0.8 {
		t.Fatalf("unexpected detection: %+v", got[0])
	}
}

func TestDetectCollaborativeTx_OrdinaryTxNotFlagged(t *testing.T) {
	// A simple 2-in-2-out payment with no equal-value output cluster.
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", -1000), delta("tx1", "B", -500),
		delta("tx1", "recipient", 1200), delta("tx1", "change", 250),
	}

	got := DetectCollaborativeTx(deltas, defaultParams())
	if len(got) != 0 {
		t.Fatalf("expected ordinary tx not flagged, got %+v", got)
	}
}

func TestDetectCollaborativeTx_BelowInputThresholdNotFlagged(t *testing.T) {
	// Equal-output pattern present, but only 2 inputs (< CollabInputMin=3).
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", -1000), delta("tx1", "B", -1000),
		delta("tx1", "X", 300), delta("tx1", "Y", 300), delta("tx1", "Z", 300),
	}

	got := DetectCollaborativeTx(deltas, defaultParams())
	if len(got) != 0 {
		t.Fatalf("expected tx below input threshold not flagged, got %+v", got)
	}
}

func TestDustInflows_OnlySmallPositiveDeltas(t *testing.T) {
	deltas := []domain.BalanceDelta{
		delta("tx1", "A", 100),  // dust (<=546)
		delta("tx1", "B", 546),  // dust (boundary, inclusive)
		delta("tx1", "C", 547),  // not dust
		delta("tx1", "D", -100), // spend, irrelevant
		delta("tx2", "A", 50),   // dust again, same address — deduped
	}

	got := DustInflows(deltas, big.NewInt(546))
	addrs := map[string]bool{}
	for _, r := range got {
		addrs[r.Address] = true
	}
	if len(got) != 2 || !addrs["A"] || !addrs["B"] {
		t.Fatalf("expected deduped dust addresses {A,B}, got %+v", got)
	}
}

func TestHubScoreFromDegree(t *testing.T) {
	cases := []struct {
		degree     int
		saturation float64
		want       float64
	}{
		{0, 50, 0},
		{25, 50, 0.5},
		{50, 50, 1},
		{100, 50, 1}, // saturates, does not exceed 1
	}
	for _, c := range cases {
		got := hubScoreFromDegree(c.degree, c.saturation)
		if got != c.want {
			t.Fatalf("hubScoreFromDegree(%d, %v) = %v, want %v", c.degree, c.saturation, got, c.want)
		}
	}
}

func TestHubTypeFromLabels(t *testing.T) {
	labels := []domain.Label{
		{Category: "scam", Status: "active"},
		{Category: "exchange", Status: "retired"}, // not active, ignored
		{Category: "mixer", Status: "active"},
	}
	hubType, ok := hubTypeFromLabels(labels)
	if !ok || hubType != "mixer" {
		t.Fatalf("expected active mixer label to win, got hubType=%q ok=%v", hubType, ok)
	}
}

func TestHubTypeFromLabels_NoHubCategoryPresent(t *testing.T) {
	labels := []domain.Label{{Category: "scam", Status: "active"}}
	_, ok := hubTypeFromLabels(labels)
	if ok {
		t.Fatalf("expected no hub type for a non-hub-category label")
	}
}
