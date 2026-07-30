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
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
)

// Requires: `docker compose up -d` and the "ethereum" chain registered
// (model_type='account', which auto-enables funding/deployer/behavioral —
// see README §1 for the add_chain call).
// Run with: go test -tags=integration ./internal/heuristic/...
//
// This is the M7 DoD (docs/05 §1 M7): funding/deployer merges form on
// account-chain data, and a popular contract does not turn into a
// supernode.
func TestFundingEngine_DoD_NewEOAMergesWithFunder(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	registryStore := registry.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	mergeEngine := merge.NewEngine(evidenceStore, addrStore)
	fundingEngine := NewFundingEngine(addrStore, addrStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	funder, newEOA := run+"-funder", run+"-neweoa"
	txid := "tx-" + run

	deltas := []domain.BalanceDelta{
		{ChainID: "ethereum", TxID: txid, DeltaIndex: 0, Address: funder, Amount: big.NewInt(-1e9), Kind: "native", BlockHeight: 100, BlockHash: "b1"},
		{ChainID: "ethereum", TxID: txid, DeltaIndex: 1, Address: newEOA, Amount: big.NewInt(1e9), Kind: "native", BlockHeight: 100, BlockHash: "b1"},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := ingestorStore.GetDeltasByTx(ctx, "ethereum", txid)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}
	candidates, err := fundingEngine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected 1 funding candidate, got %d: %+v", len(candidates), candidates)
	}
	if candidates[0].AddressA != funder || candidates[0].AddressB != newEOA {
		t.Fatalf("expected %s -> %s, got %+v", funder, newEOA, candidates[0])
	}

	if _, err := mergeEngine.RecordAndMergeBatch(ctx, candidates); err != nil {
		t.Fatalf("record_and_merge_batch: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "ethereum"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	same, _, err := clusterStore.SameCluster(ctx, "ethereum", funder, newEOA, 0)
	if err != nil || !same {
		t.Fatalf("expected funder and new EOA clustered: same=%v err=%v", same, err)
	}
}

func TestDeployerEngine_DoD_ContractMergesWithDeployer(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	registryStore := registry.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	mergeEngine := merge.NewEngine(evidenceStore, addrStore)
	deployerEngine := NewDeployerEngine(addrStore, addrStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	deployer, contract := run+"-deployer", run+"-contract"
	txid := "tx-" + run

	deltas := []domain.BalanceDelta{
		{ChainID: "ethereum", TxID: txid, DeltaIndex: 0, Address: deployer, Amount: big.NewInt(-5e8), Kind: "native", BlockHeight: 200, BlockHash: "b2"},
		{ChainID: "ethereum", TxID: txid, DeltaIndex: 1, Address: contract, Amount: big.NewInt(5e8), Kind: "native", BlockHeight: 200, BlockHash: "b2", Meta: []byte(`{"contract_creation":true}`)},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := ingestorStore.GetDeltasByTx(ctx, "ethereum", txid)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}
	candidates, err := deployerEngine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected 1 deployer candidate, got %d: %+v", len(candidates), candidates)
	}

	if _, err := mergeEngine.RecordAndMergeBatch(ctx, candidates); err != nil {
		t.Fatalf("record_and_merge_batch: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "ethereum"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	same, _, err := clusterStore.SameCluster(ctx, "ethereum", deployer, contract, 0)
	if err != nil || !same {
		t.Fatalf("expected deployer and contract clustered: same=%v err=%v", same, err)
	}
}

// TestAccountHeuristics_PopularContractDoesNotFormSupernode is the M7 DoD's
// second half: a popular contract that "funds" (or is otherwise repeatedly
// touched by) many distinct addresses must not pull them all into one
// cluster — markHubs (M3, chain-model-agnostic) catches it by counterparty
// degree, and funding/behavioral both re-check IsHub before emitting a
// candidate.
func TestAccountHeuristics_PopularContractDoesNotFormSupernode(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	registryStore := registry.NewStore(pool)
	labelStore := label.NewStore(pool)
	hubDetector := preprocessor.NewHubDetector(pool, labelStore, addrStore)
	fundingEngine := NewFundingEngine(addrStore, addrStore, registryStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	popularContract := run + "-popular-dapp"

	var allDeltas []domain.BalanceDelta
	for i := 0; i < 10; i++ {
		user := fmt.Sprintf("%s-user%d", run, i)
		txid := fmt.Sprintf("%s-tx%d", run, i)
		allDeltas = append(allDeltas,
			domain.BalanceDelta{ChainID: "ethereum", TxID: txid, DeltaIndex: 0, Address: popularContract, Amount: big.NewInt(-1000), Kind: "native", BlockHeight: 300, BlockHash: "b3"},
			domain.BalanceDelta{ChainID: "ethereum", TxID: txid, DeltaIndex: 1, Address: user, Amount: big.NewInt(1000), Kind: "native", BlockHeight: 300, BlockHash: "b3"},
		)
	}
	if _, err := ingestorStore.Ingest(ctx, allDeltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// saturation=5: 10 distinct counterparties saturates the hub score to 1.0.
	params := domain.PreprocessingParams{HubThreshold: 0.5, HubDegreeSaturation: 5}
	if err := hubDetector.MarkHubs(ctx, "ethereum", []string{popularContract}, params); err != nil {
		t.Fatalf("mark_hubs: %v", err)
	}
	isHub, err := addrStore.IsHub(ctx, "ethereum", popularContract)
	if err != nil || !isHub {
		t.Fatalf("expected popular contract flagged as hub: is_hub=%v err=%v", isHub, err)
	}

	candidates, err := fundingEngine.Generate(ctx, allDeltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected 0 funding candidates once the funder is a hub (no supernode), got %d: %+v", len(candidates), candidates)
	}
}
