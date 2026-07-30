// Account-model heuristics (docs/03-clustering-algorithms.md §5c):
// funding, deployer, behavioral — for chains without a "common input"
// concept (Ethereum and similar). These never mix with UTXO heuristics
// (common-input, change): docs/06's model_type registry only enables them
// for chains registered with model_type='account'.
//
// docs/03 §5c's hub caution — "인기 dApp/컨트랙트는 허브다. 같은 컨트랙트와
// 상호작용했다는 것만으로 묶지 말 것" — doesn't need new machinery: markHubs
// (docs/03 §1, M3) already flags any address by counterparty degree
// regardless of chain model, so a popular contract is already caught and
// excluded the same way a UTXO hub is. BehavioralEngine below also never
// merges on "both touched contract C" — only on direct interaction between
// the pair themselves, which is a structurally different (and safer) claim.
package heuristic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
)

// accountPairKey identifies an unordered address pair — normalized so
// (a,b) and (b,a) collide (mirrors cluster.pairKey, which is unexported in
// its own package and so can't be reused directly here).
type accountPairKey struct{ a, b string }

func normalizeAccountPair(a, b string) accountPairKey {
	if a <= b {
		return accountPairKey{a, b}
	}
	return accountPairKey{b, a}
}

const (
	HeuristicKeyFunding    = "funding"
	HeuristicKeyDeployer   = "deployer"
	HeuristicKeyBehavioral = "behavioral"
)

// contractCreationMeta is a provisional convention for BalanceDelta.Meta:
// the indexer marks a receiving delta this way when the address is a
// freshly deployed contract, not an EOA. There's no real indexer contract
// yet (docs/02 §1's BalanceDelta envelope is still self-designed, see
// CLAUDE.md) — this key is a placeholder that DeployerEngine consumes if
// present; absent it, every new address is treated as ordinary funding.
type contractCreationMeta struct {
	ContractCreation bool `json:"contract_creation"`
}

func isContractCreationDelta(d domain.BalanceDelta) bool {
	if len(d.Meta) == 0 {
		return false
	}
	var m contractCreationMeta
	if err := json.Unmarshal(d.Meta, &m); err != nil {
		return false
	}
	return m.ContractCreation
}

func isNewAddress(ctx context.Context, addresses AddressFirstSeenChecker, chainID, address string, blockHeight int64) (bool, error) {
	a, found, err := addresses.Get(ctx, chainID, address)
	if err != nil {
		return false, err
	}
	if !found || a.FirstSeenHeight == nil {
		return false, nil
	}
	return *a.FirstSeenHeight == blockHeight, nil
}

// FundingEngine implements the funding half of accountModelHeuristics
// (docs/03 §5c(1)): a brand new address's first funder is a moderate
// merge signal — unless that first receive is flagged as a contract
// creation, which DeployerEngine owns instead.
type FundingEngine struct {
	hubs      HubChecker
	addresses AddressFirstSeenChecker
	config    ConfigProvider
}

func NewFundingEngine(hubs HubChecker, addresses AddressFirstSeenChecker, config ConfigProvider) *FundingEngine {
	return &FundingEngine{hubs: hubs, addresses: addresses, config: config}
}

func (e *FundingEngine) Name() string { return HeuristicKeyFunding }

func (e *FundingEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	cfg, found, err := e.config.ConfigFor(ctx, chainID, HeuristicKeyFunding)
	if err != nil {
		return nil, fmt.Errorf("funding: config lookup: %w", err)
	}
	if !found || !cfg.Enabled {
		return nil, nil
	}

	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		spenders := ingestor.SpentAddresses(tx.Deltas)
		if len(spenders) == 0 {
			continue
		}
		funder := spenders[0]

		for _, delta := range tx.Deltas {
			if delta.Amount == nil || delta.Amount.Sign() <= 0 {
				continue
			}
			recipient := delta.Address
			if recipient == funder {
				continue
			}
			if isContractCreationDelta(delta) {
				continue // DeployerEngine's territory
			}

			isNew, err := isNewAddress(ctx, e.addresses, tx.Key.ChainID, recipient, tx.Deltas[0].BlockHeight)
			if err != nil {
				return nil, fmt.Errorf("funding: is_new_address(%s): %w", recipient, err)
			}
			if !isNew {
				continue
			}

			isHub, err := e.hubs.IsHub(ctx, tx.Key.ChainID, funder)
			if err != nil {
				return nil, fmt.Errorf("funding: is_hub(%s): %w", funder, err)
			}
			if isHub {
				continue
			}

			txid := tx.Key.TxID
			blockHash := tx.Deltas[0].BlockHash
			blockHeight := tx.Deltas[0].BlockHeight
			candidates = append(candidates, domain.MergeCandidate{
				ChainID: tx.Key.ChainID, AddressA: funder, AddressB: recipient,
				HeuristicKey: HeuristicKeyFunding, SourceTxID: &txid,
				SourceBlockHash: &blockHash, SourceBlockHeight: &blockHeight,
				Confidence: cfg.Confidence,
			})
		}
	}
	return candidates, nil
}

// DeployerEngine implements docs/03 §5c(2): a contract's deployer. Mirrors
// FundingEngine exactly except it only fires on deltas flagged as contract
// creation (see contractCreationMeta) — everything else about "who funded
// this address's first appearance" is identical.
type DeployerEngine struct {
	hubs      HubChecker
	addresses AddressFirstSeenChecker
	config    ConfigProvider
}

func NewDeployerEngine(hubs HubChecker, addresses AddressFirstSeenChecker, config ConfigProvider) *DeployerEngine {
	return &DeployerEngine{hubs: hubs, addresses: addresses, config: config}
}

func (e *DeployerEngine) Name() string { return HeuristicKeyDeployer }

func (e *DeployerEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	cfg, found, err := e.config.ConfigFor(ctx, chainID, HeuristicKeyDeployer)
	if err != nil {
		return nil, fmt.Errorf("deployer: config lookup: %w", err)
	}
	if !found || !cfg.Enabled {
		return nil, nil
	}

	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		spenders := ingestor.SpentAddresses(tx.Deltas)
		if len(spenders) == 0 {
			continue
		}
		deployer := spenders[0]

		for _, delta := range tx.Deltas {
			if delta.Amount == nil || delta.Amount.Sign() <= 0 {
				continue
			}
			if !isContractCreationDelta(delta) {
				continue
			}
			contractAddr := delta.Address
			if contractAddr == deployer {
				continue
			}

			isHub, err := e.hubs.IsHub(ctx, tx.Key.ChainID, deployer)
			if err != nil {
				return nil, fmt.Errorf("deployer: is_hub(%s): %w", deployer, err)
			}
			if isHub {
				continue
			}

			txid := tx.Key.TxID
			blockHash := tx.Deltas[0].BlockHash
			blockHeight := tx.Deltas[0].BlockHeight
			candidates = append(candidates, domain.MergeCandidate{
				ChainID: tx.Key.ChainID, AddressA: deployer, AddressB: contractAddr,
				HeuristicKey: HeuristicKeyDeployer, SourceTxID: &txid,
				SourceBlockHash: &blockHash, SourceBlockHeight: &blockHeight,
				Confidence: cfg.Confidence,
			})
		}
	}
	return candidates, nil
}

type behavioralParams struct {
	// MinInteractions: distinct transactions two addresses must share
	// (docs/03 §5c(3) "반복 상호작용") before behavioral treats it as a
	// weak ownership signal, not a coincidence.
	MinInteractions int
}

func defaultBehavioralParams() behavioralParams {
	return behavioralParams{MinInteractions: 3}
}

func parseBehavioralParams(raw json.RawMessage) behavioralParams {
	params := defaultBehavioralParams()
	if len(raw) == 0 {
		return params
	}
	var overrides struct {
		MinInteractions *int `json:"min_interactions"`
	}
	if err := json.Unmarshal(raw, &overrides); err != nil {
		return params
	}
	if overrides.MinInteractions != nil {
		params.MinInteractions = *overrides.MinInteractions
	}
	return params
}

// InteractionCounter counts how many distinct transactions two addresses
// have both appeared in, chain-wide — not scoped to the current batch,
// since "repeated" behavior is a property of history, not one block.
type InteractionCounter interface {
	SharedTransactionCount(ctx context.Context, chainID, a, b string) (int, error)
}

// SQLInteractionCounter is the live InteractionCounter, backed directly by
// balance_delta (mirrors SweepEngine's own pool-held queries in sweep.go).
type SQLInteractionCounter struct {
	pool *pgxpool.Pool
}

func NewSQLInteractionCounter(pool *pgxpool.Pool) *SQLInteractionCounter {
	return &SQLInteractionCounter{pool: pool}
}

func (c *SQLInteractionCounter) SharedTransactionCount(ctx context.Context, chainID, a, b string) (int, error) {
	var count int
	err := c.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t1.txid)
		FROM clustering.balance_delta t1
		JOIN clustering.balance_delta t2 ON t2.chain_id = t1.chain_id AND t2.txid = t1.txid
		WHERE t1.chain_id = $1 AND t1.address = $2 AND t2.address = $3
	`, chainID, a, b).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("interaction_counter: shared_transaction_count: %w", err)
	}
	return count, nil
}

// BehavioralEngine implements docs/03 §5c(3): addresses that repeatedly
// transact with *each other* are weak evidence of common control — deliberately
// never "both touched contract C" (docs/03 §5c's hub caution), only direct
// pairwise interaction, which structurally can't produce the "everyone who
// used this dApp" supernode the caution warns about.
type BehavioralEngine struct {
	hubs         HubChecker
	interactions InteractionCounter
	config       ConfigProvider
}

func NewBehavioralEngine(hubs HubChecker, interactions InteractionCounter, config ConfigProvider) *BehavioralEngine {
	return &BehavioralEngine{hubs: hubs, interactions: interactions, config: config}
}

func (e *BehavioralEngine) Name() string { return HeuristicKeyBehavioral }

func (e *BehavioralEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	cfg, found, err := e.config.ConfigFor(ctx, chainID, HeuristicKeyBehavioral)
	if err != nil {
		return nil, fmt.Errorf("behavioral: config lookup: %w", err)
	}
	if !found || !cfg.Enabled {
		return nil, nil
	}
	params := parseBehavioralParams(cfg.Params)

	seen := make(map[accountPairKey]bool)
	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		addrs := txAddresses(tx.Deltas)
		for i := 0; i < len(addrs); i++ {
			for j := i + 1; j < len(addrs); j++ {
				key := normalizeAccountPair(addrs[i], addrs[j])
				if seen[key] {
					continue
				}
				seen[key] = true

				count, err := e.interactions.SharedTransactionCount(ctx, tx.Key.ChainID, key.a, key.b)
				if err != nil {
					return nil, fmt.Errorf("behavioral: shared_transaction_count(%s,%s): %w", key.a, key.b, err)
				}
				if count < params.MinInteractions {
					continue
				}

				hubA, err := e.hubs.IsHub(ctx, tx.Key.ChainID, key.a)
				if err != nil {
					return nil, fmt.Errorf("behavioral: is_hub(%s): %w", key.a, err)
				}
				hubB, err := e.hubs.IsHub(ctx, tx.Key.ChainID, key.b)
				if err != nil {
					return nil, fmt.Errorf("behavioral: is_hub(%s): %w", key.b, err)
				}
				if hubA || hubB {
					continue
				}

				txid := tx.Key.TxID
				blockHash := tx.Deltas[0].BlockHash
				blockHeight := tx.Deltas[0].BlockHeight
				candidates = append(candidates, domain.MergeCandidate{
					ChainID: tx.Key.ChainID, AddressA: key.a, AddressB: key.b,
					HeuristicKey: HeuristicKeyBehavioral, SourceTxID: &txid,
					SourceBlockHash: &blockHash, SourceBlockHeight: &blockHeight,
					Confidence: cfg.Confidence,
				})
			}
		}
	}
	return candidates, nil
}

func txAddresses(txDeltas []domain.BalanceDelta) []string {
	seen := make(map[string]bool, len(txDeltas))
	var out []string
	for _, d := range txDeltas {
		if !seen[d.Address] {
			seen[d.Address] = true
			out = append(out, d.Address)
		}
	}
	return out
}
