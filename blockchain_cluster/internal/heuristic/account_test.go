package heuristic

import (
	"context"
	"math/big"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type fakeConfig struct {
	cfg domain.HeuristicConfig
}

func (f fakeConfig) ConfigFor(_ context.Context, _, _ string) (domain.HeuristicConfig, bool, error) {
	return f.cfg, true, nil // found=true; cfg.Enabled itself controls enable/disable
}

type fakeAddresses map[string]domain.Address

func (f fakeAddresses) Get(_ context.Context, _, address string) (domain.Address, bool, error) {
	a, ok := f[address]
	return a, ok, nil
}

func heightPtr(h int64) *int64 { return &h }

func acctDelta(txid, addr string, amount int64, height int64, meta string) domain.BalanceDelta {
	d := domain.BalanceDelta{
		ChainID: "ethereum", TxID: txid, Address: addr, Amount: big.NewInt(amount), BlockHeight: height,
	}
	if meta != "" {
		d.Meta = []byte(meta)
	}
	return d
}

func TestFundingEngine_NewAddressFundedByExisting(t *testing.T) {
	addrs := fakeAddresses{"NewEOA": {FirstSeenHeight: heightPtr(100)}}
	e := NewFundingEngine(fakeHubs{}, addrs, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.6, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "Funder", -50, 100, ""),
		acctDelta("tx1", "NewEOA", 50, 100, ""),
	}

	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 funding candidate, got %d: %+v", len(got), got)
	}
	if got[0].AddressA != "Funder" || got[0].AddressB != "NewEOA" || got[0].HeuristicKey != "funding" {
		t.Fatalf("unexpected candidate: %+v", got[0])
	}
	if got[0].Confidence != 0.6 {
		t.Fatalf("expected registry confidence 0.6, got %v", got[0].Confidence)
	}
}

func TestFundingEngine_NotNewAddressSkipped(t *testing.T) {
	// FirstSeenHeight is an earlier block — this address existed before this tx.
	addrs := fakeAddresses{"OldEOA": {FirstSeenHeight: heightPtr(50)}}
	e := NewFundingEngine(fakeHubs{}, addrs, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.6, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "Funder", -50, 100, ""),
		acctDelta("tx1", "OldEOA", 50, 100, ""),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no candidates for a pre-existing address, got %+v", got)
	}
}

func TestFundingEngine_HubFunderExcluded(t *testing.T) {
	addrs := fakeAddresses{"NewEOA": {FirstSeenHeight: heightPtr(100)}}
	e := NewFundingEngine(fakeHubs{"BigExchange": true}, addrs, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.6, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "BigExchange", -50, 100, ""),
		acctDelta("tx1", "NewEOA", 50, 100, ""),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no candidate when the funder is a hub, got %+v", got)
	}
}

func TestFundingEngine_ContractCreationDeltaSkipped(t *testing.T) {
	addrs := fakeAddresses{"NewContract": {FirstSeenHeight: heightPtr(100)}}
	e := NewFundingEngine(fakeHubs{}, addrs, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.6, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "Deployer", -50, 100, ""),
		acctDelta("tx1", "NewContract", 50, 100, `{"contract_creation":true}`),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected FundingEngine to skip contract-creation deltas (DeployerEngine's job), got %+v", got)
	}
}

func TestDeployerEngine_OnlyFiresOnContractCreationFlag(t *testing.T) {
	addrs := fakeAddresses{}
	e := NewDeployerEngine(fakeHubs{}, addrs, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.85, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "Deployer", -50, 100, ""),
		acctDelta("tx1", "NewContract", 50, 100, `{"contract_creation":true}`),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 deployer candidate, got %d: %+v", len(got), got)
	}
	if got[0].AddressA != "Deployer" || got[0].AddressB != "NewContract" || got[0].HeuristicKey != "deployer" {
		t.Fatalf("unexpected candidate: %+v", got[0])
	}
	if got[0].Confidence != 0.85 {
		t.Fatalf("expected registry confidence 0.85, got %v", got[0].Confidence)
	}
}

func TestDeployerEngine_OrdinaryTransferIgnored(t *testing.T) {
	e := NewDeployerEngine(fakeHubs{}, fakeAddresses{}, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.85, Enabled: true}})
	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "A", -50, 100, ""),
		acctDelta("tx1", "B", 50, 100, ""), // no contract_creation flag
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no deployer candidates without the contract_creation flag, got %+v", got)
	}
}

type fakeInteractions map[accountPairKey]int

func (f fakeInteractions) SharedTransactionCount(_ context.Context, _, a, b string) (int, error) {
	return f[normalizeAccountPair(a, b)], nil
}

func TestBehavioralEngine_RepeatedInteractionMerges(t *testing.T) {
	interactions := fakeInteractions{normalizeAccountPair("X", "Y"): 5}
	e := NewBehavioralEngine(fakeHubs{}, interactions, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.3, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "X", -10, 100, ""),
		acctDelta("tx1", "Y", 10, 100, ""),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 behavioral candidate, got %d: %+v", len(got), got)
	}
	if got[0].Confidence != 0.3 {
		t.Fatalf("expected low registry confidence 0.3, got %v", got[0].Confidence)
	}
}

func TestBehavioralEngine_BelowMinInteractionsSkipped(t *testing.T) {
	interactions := fakeInteractions{normalizeAccountPair("X", "Y"): 1}
	e := NewBehavioralEngine(fakeHubs{}, interactions, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.3, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "X", -10, 100, ""),
		acctDelta("tx1", "Y", 10, 100, ""),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no candidates below min_interactions (default 3), got %+v", got)
	}
}

func TestBehavioralEngine_HubParticipantExcluded(t *testing.T) {
	interactions := fakeInteractions{normalizeAccountPair("PopularDApp", "Y"): 100}
	e := NewBehavioralEngine(fakeHubs{"PopularDApp": true}, interactions, fakeConfig{cfg: domain.HeuristicConfig{Confidence: 0.3, Enabled: true}})

	deltas := []domain.BalanceDelta{
		acctDelta("tx1", "Y", -10, 100, ""),
		acctDelta("tx1", "PopularDApp", 10, 100, ""),
	}
	got, err := e.Generate(context.Background(), deltas)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected a popular contract's high interaction count to still be excluded as a hub, got %+v", got)
	}
}
