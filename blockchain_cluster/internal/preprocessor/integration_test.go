//go:build integration

package preprocessor

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/preprocessor/...
//
// These are the M3 DoD (docs/05 §1): a batch mixing hub/CoinJoin/dust
// traffic must not let those transactions feed commonInputHeuristic.

func testParams(saturation float64) domain.PreprocessingParams {
	return domain.PreprocessingParams{
		HubThreshold:            0.5,
		HubDegreeSaturation:     saturation,
		DustThreshold:           big.NewInt(546),
		CoinjoinConfidence:      0.8,
		DustExclusionConfidence: 0.8,
		EqualOutputMin:          3,
		CollabInputMin:          3,
		CollabOutputMin:         3,
	}
}

func TestMarkHubs_BehavioralDegreeBlocksCommonInputMerge(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	excludedStore := NewStore(pool)
	labelStore := label.NewStore(pool)
	hubDetector := NewHubDetector(pool, labelStore, addrStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	h := run + "-H"
	var payoutDeltas []domain.BalanceDelta
	for i := 0; i < 4; i++ {
		peer := fmt.Sprintf("%s-P%d", run, i)
		payoutDeltas = append(payoutDeltas,
			domain.BalanceDelta{ChainID: "bitcoin", TxID: fmt.Sprintf("%s-payout%d", run, i), DeltaIndex: 0, Address: h, Amount: big.NewInt(-100), BlockHeight: 1, BlockHash: "b1"},
			domain.BalanceDelta{ChainID: "bitcoin", TxID: fmt.Sprintf("%s-payout%d", run, i), DeltaIndex: 1, Address: peer, Amount: big.NewInt(100), BlockHeight: 1, BlockHash: "b1"},
		)
	}
	if _, err := ingestorStore.Ingest(ctx, payoutDeltas); err != nil {
		t.Fatalf("ingest payouts: %v", err)
	}

	// saturation=3: H's degree of 4 distinct counterparties saturates the score to 1.0.
	params := testParams(3)
	if err := hubDetector.MarkHubs(ctx, "bitcoin", []string{h}, params); err != nil {
		t.Fatalf("mark_hubs: %v", err)
	}
	isHub, err := addrStore.IsHub(ctx, "bitcoin", h)
	if err != nil {
		t.Fatalf("is_hub: %v", err)
	}
	if !isHub {
		t.Fatalf("expected %s to be flagged as a hub after 4 distinct counterparties (saturation=3)", h)
	}

	// Now H co-spends with two genuine users in one tx — without the hub
	// filter, commonInputHeuristic would merge H, Ua, and Ub together.
	ua, ub := run+"-Ua", run+"-Ub"
	spendTx := run + "-spend"
	spendDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 0, Address: h, Amount: big.NewInt(-500), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 1, Address: ua, Amount: big.NewInt(-300), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 2, Address: ub, Amount: big.NewInt(-200), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 3, Address: run + "-recipient", Amount: big.NewInt(1000), BlockHeight: 2, BlockHash: "b2"},
	}
	if _, err := ingestorStore.Ingest(ctx, spendDeltas); err != nil {
		t.Fatalf("ingest spend: %v", err)
	}
	got, err := ingestorStore.GetDeltasByTx(ctx, "bitcoin", spendTx)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}

	engine := heuristic.NewCommonInputEngine(addrStore, excludedStore, constantConfidence{0.95})
	candidates, err := engine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 candidate (Ua-Ub, hub H excluded), got %d: %+v", len(candidates), candidates)
	}
	if candidates[0].AddressA != ua || candidates[0].AddressB != ub {
		t.Fatalf("expected Ua-Ub, got %+v", candidates[0])
	}
}

func TestMarkCollaborativeTx_BlocksCommonInputMerge(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	excludedStore := NewStore(pool)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	txid := run + "-coinjoin"
	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 0, Address: run + "-A", Amount: big.NewInt(-1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 1, Address: run + "-B", Amount: big.NewInt(-1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 2, Address: run + "-C", Amount: big.NewInt(-1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 3, Address: run + "-X", Amount: big.NewInt(300), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 4, Address: run + "-Y", Amount: big.NewInt(300), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 5, Address: run + "-Z", Amount: big.NewInt(300), BlockHeight: 1, BlockHash: "b1"},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if err := excludedStore.MarkCollaborativeTx(ctx, deltas, testParams(50)); err != nil {
		t.Fatalf("mark_collaborative_tx: %v", err)
	}
	excluded, err := excludedStore.IsExcluded(ctx, "bitcoin", txid)
	if err != nil {
		t.Fatalf("is_excluded: %v", err)
	}
	if !excluded {
		t.Fatalf("expected %s to be excluded as coinjoin", txid)
	}

	got, err := ingestorStore.GetDeltasByTx(ctx, "bitcoin", txid)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}
	engine := heuristic.NewCommonInputEngine(addrStore, excludedStore, constantConfidence{0.95})
	candidates, err := engine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates for an excluded coinjoin tx, got %+v", candidates)
	}
}

func TestMarkDust_BlocksCommonInputWhenAllInputsTainted(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	excludedStore := NewStore(pool)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	p1, p2 := run+"-P1", run+"-P2"
	params := testParams(50)

	dustDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-dust1", DeltaIndex: 0, Address: p1, Amount: big.NewInt(100), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: run + "-dust2", DeltaIndex: 0, Address: p2, Amount: big.NewInt(200), BlockHeight: 1, BlockHash: "b1"},
	}
	if _, err := ingestorStore.Ingest(ctx, dustDeltas); err != nil {
		t.Fatalf("ingest dust: %v", err)
	}
	if err := excludedStore.MarkDust(ctx, dustDeltas, params, addrStore); err != nil {
		t.Fatalf("mark_dust (phase 1): %v", err)
	}
	for _, addr := range []string{p1, p2} {
		a, found, err := addrStore.Get(ctx, "bitcoin", addr)
		if err != nil || !found || !a.DustFlag {
			t.Fatalf("expected %s to be dust-flagged: found=%v err=%v a=%+v", addr, found, err, a)
		}
	}

	spendTx := run + "-spend"
	spendDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 0, Address: p1, Amount: big.NewInt(-50), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 1, Address: p2, Amount: big.NewInt(-150), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: spendTx, DeltaIndex: 2, Address: run + "-recipient", Amount: big.NewInt(180), BlockHeight: 2, BlockHash: "b2"},
	}
	if _, err := ingestorStore.Ingest(ctx, spendDeltas); err != nil {
		t.Fatalf("ingest spend: %v", err)
	}
	if err := excludedStore.MarkDust(ctx, spendDeltas, params, addrStore); err != nil {
		t.Fatalf("mark_dust (phase 2): %v", err)
	}

	excluded, err := excludedStore.IsExcluded(ctx, "bitcoin", spendTx)
	if err != nil {
		t.Fatalf("is_excluded: %v", err)
	}
	if !excluded {
		t.Fatalf("expected %s to be excluded as dust-only (both spenders dust-tainted)", spendTx)
	}

	got, err := ingestorStore.GetDeltasByTx(ctx, "bitcoin", spendTx)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}
	engine := heuristic.NewCommonInputEngine(addrStore, excludedStore, constantConfidence{0.95})
	candidates, err := engine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates for a dust-only excluded tx, got %+v", candidates)
	}
}

type constantConfidence struct{ confidence float64 }

func (c constantConfidence) ConfidenceFor(_ context.Context, _, _ string) (float64, bool, error) {
	return c.confidence, true, nil
}
