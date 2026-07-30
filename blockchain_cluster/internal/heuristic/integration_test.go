//go:build integration

package heuristic

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
	"github.com/tkdlqm2/blockchain-cluster/internal/sweeptarget"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/heuristic/...
//
// TestSweepEngine_DoD is the M4 DoD verbatim (docs/05 §1 M4): deposit
// addresses converging on a known-deposit sweep target merge into that
// entity, but the outside addresses that funded those deposits do not.
func TestSweepEngine_DoD_DepositAddressesMergeButOriginatorsDoNot(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	targetStore := sweeptarget.NewStore(pool)
	registryStore := registry.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	mergeEngine := merge.NewEngine(evidenceStore, addrStore)
	sweepEngine := NewSweepEngine(pool, targetStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	target := run + "-exchange-hotwallet"
	d1, d2 := run+"-deposit1", run+"-deposit2"
	o1, o2 := run+"-originator1", run+"-originator2"

	entityHint := "TestExchange"
	if err := targetStore.Add(ctx, domain.SweepTarget{
		ChainID: "bitcoin", Address: target, EntityHint: &entityHint,
		Source: "known-deposit", Confidence: 0.95,
	}); err != nil {
		t.Fatalf("register sweep target: %v", err)
	}

	// O1 funds D1; O2 funds D2 (two separate deposit transactions).
	depositDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-dep1", DeltaIndex: 0, Address: o1, Amount: big.NewInt(-1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: run + "-dep1", DeltaIndex: 1, Address: d1, Amount: big.NewInt(1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: run + "-dep2", DeltaIndex: 0, Address: o2, Amount: big.NewInt(-500), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: run + "-dep2", DeltaIndex: 1, Address: d2, Amount: big.NewInt(500), BlockHeight: 1, BlockHash: "b1"},
	}
	if _, err := ingestorStore.Ingest(ctx, depositDeltas); err != nil {
		t.Fatalf("ingest deposits: %v", err)
	}

	// D1 and D2 each sweep (nearly) everything they received into target.
	sweepDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-sweep1", DeltaIndex: 0, Address: d1, Amount: big.NewInt(-1000), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: run + "-sweep1", DeltaIndex: 1, Address: target, Amount: big.NewInt(1000), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: run + "-sweep2", DeltaIndex: 0, Address: d2, Amount: big.NewInt(-500), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: run + "-sweep2", DeltaIndex: 1, Address: target, Amount: big.NewInt(500), BlockHeight: 2, BlockHash: "b2"},
	}
	if _, err := ingestorStore.Ingest(ctx, sweepDeltas); err != nil {
		t.Fatalf("ingest sweeps: %v", err)
	}

	candidates, err := sweepEngine.Generate(ctx, sweepDeltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 sweep candidates (target-d1, target-d2), got %d: %+v", len(candidates), candidates)
	}
	for _, c := range candidates {
		if c.AddressA != target {
			t.Fatalf("expected target to anchor every sweep candidate, got %+v", c)
		}
		if c.AddressB == o1 || c.AddressB == o2 {
			t.Fatalf("originator address leaked into a sweep candidate: %+v", c)
		}
	}

	if _, err := mergeEngine.RecordAndMergeBatch(ctx, candidates); err != nil {
		t.Fatalf("record_and_merge_batch: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	sameD1, _, err := clusterStore.SameCluster(ctx, "bitcoin", d1, target, 0)
	if err != nil {
		t.Fatalf("same_cluster(d1,target): %v", err)
	}
	if !sameD1 {
		t.Fatalf("expected deposit address %s to be clustered with target %s", d1, target)
	}
	sameD2, _, err := clusterStore.SameCluster(ctx, "bitcoin", d2, target, 0)
	if err != nil {
		t.Fatalf("same_cluster(d2,target): %v", err)
	}
	if !sameD2 {
		t.Fatalf("expected deposit address %s to be clustered with target %s", d2, target)
	}

	// The DoD's other half: originators must NOT be pulled in.
	_, _, o1Clustered, err := clusterStore.ClusterOf(ctx, "bitcoin", o1, 0)
	if err != nil {
		t.Fatalf("cluster_of(o1): %v", err)
	}
	if o1Clustered {
		t.Fatalf("originator %s should not be in any cluster (it only funded a deposit address, docs/03 §5 경계선)", o1)
	}
	_, _, o2Clustered, err := clusterStore.ClusterOf(ctx, "bitcoin", o2, 0)
	if err != nil {
		t.Fatalf("cluster_of(o2): %v", err)
	}
	if o2Clustered {
		t.Fatalf("originator %s should not be in any cluster", o2)
	}
}

func TestSweepEngine_IncompleteSweepNotDetected(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	targetStore := sweeptarget.NewStore(pool)
	registryStore := registry.NewStore(pool)
	sweepEngine := NewSweepEngine(pool, targetStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	target := run + "-target"
	src := run + "-src"

	entityHint := "Test"
	if err := targetStore.Add(ctx, domain.SweepTarget{
		ChainID: "bitcoin", Address: target, EntityHint: &entityHint, Source: "observed", Confidence: 0.6,
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	// src received 1000 but only forwards 100 (10%) to target — an ordinary
	// payment, not a sweep (completeness far below the 0.9 default).
	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-fund", DeltaIndex: 0, Address: run + "-funder", Amount: big.NewInt(-1000), BlockHeight: 1, BlockHash: "b1"},
		{ChainID: "bitcoin", TxID: run + "-fund", DeltaIndex: 1, Address: src, Amount: big.NewInt(1000), BlockHeight: 1, BlockHash: "b1"},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest fund: %v", err)
	}
	payDeltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-pay", DeltaIndex: 0, Address: src, Amount: big.NewInt(-100), BlockHeight: 2, BlockHash: "b2"},
		{ChainID: "bitcoin", TxID: run + "-pay", DeltaIndex: 1, Address: target, Amount: big.NewInt(100), BlockHeight: 2, BlockHash: "b2"},
	}
	if _, err := ingestorStore.Ingest(ctx, payDeltas); err != nil {
		t.Fatalf("ingest pay: %v", err)
	}

	candidates, err := sweepEngine.Generate(ctx, payDeltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected no sweep candidate for a 10%% partial payment, got %+v", candidates)
	}
}

func TestChangeEngine_PicksNewOddRespentOutputAsChange(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	excludedStore := preprocessor.NewStore(pool)
	registryStore := registry.NewStore(pool)
	changeEngine := NewChangeEngine(pool, addrStore, excludedStore, addrStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	in1 := run + "-in1"
	recipient := run + "-recipient" // round amount, paid target
	changeAddr := run + "-change"   // new, odd amount, respent later

	txid := run + "-pay"
	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 0, Address: in1, Amount: big.NewInt(-9321), BlockHeight: 10, BlockHash: "b10"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 1, Address: recipient, Amount: big.NewInt(5000), BlockHeight: 10, BlockHash: "b10"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 2, Address: changeAddr, Amount: big.NewInt(4321), BlockHeight: 10, BlockHash: "b10"},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// changeAddr gets respent later — the third weak clue.
	respend := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: run + "-respend", DeltaIndex: 0, Address: changeAddr, Amount: big.NewInt(-4321), BlockHeight: 11, BlockHash: "b11"},
	}
	if _, err := ingestorStore.Ingest(ctx, respend); err != nil {
		t.Fatalf("ingest respend: %v", err)
	}

	candidates, err := changeEngine.Generate(ctx, deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 change candidate, got %d: %+v", len(candidates), candidates)
	}
	if candidates[0].AddressA != in1 || candidates[0].AddressB != changeAddr {
		t.Fatalf("expected %s -> %s, got %+v", in1, changeAddr, candidates[0])
	}
	if candidates[0].Confidence <= 0 || candidates[0].Confidence > 0.4 {
		t.Fatalf("expected change confidence capped at the registry ceiling (0.4), got %v", candidates[0].Confidence)
	}
}
