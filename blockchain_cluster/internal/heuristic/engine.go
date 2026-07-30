// Package heuristic implements HeuristicEngines (docs/04-architecture §2 [3]):
// pluggable engines that only emit MergeCandidates, never merge directly
// (FR-12). Each engine implements Engine; the core (MergeEngine,
// EvidenceStore, ClusterStore) never changes when a new one is added.
package heuristic

import (
	"context"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// Engine is the contract every heuristic implements (docs/04 §2 [3]).
type Engine interface {
	Name() string
	Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error)
}

// HubChecker is the read-path a heuristic needs from the address registry
// to enforce "block merges through a hub" (docs/03 §1, §4).
type HubChecker interface {
	IsHub(ctx context.Context, chainID, address string) (bool, error)
}

// ExclusionChecker is the read-path a heuristic needs from Preprocessor's
// excluded_tx table (docs/03 §2, §3, §4).
type ExclusionChecker interface {
	IsExcluded(ctx context.Context, chainID, txid string) (bool, error)
}

// ConfidenceProvider resolves a heuristic's configured confidence for a
// chain from the chain_heuristic/heuristic registry (docs/03 §10: no
// hardcoded parameters).
type ConfidenceProvider interface {
	ConfidenceFor(ctx context.Context, chainID, heuristicKey string) (confidence float64, enabled bool, err error)
}

// ConfigProvider is ConfidenceProvider's wider sibling — for engines (like
// SweepEngine) that also need chain_heuristic.params, not just confidence.
type ConfigProvider interface {
	ConfigFor(ctx context.Context, chainID, heuristicKey string) (domain.HeuristicConfig, bool, error)
}

// SweepTargetChecker is sweeptarget.Store's read path — sweepHeuristic's
// isKnownSweepTarget(dst) (docs/03 §5).
type SweepTargetChecker interface {
	Get(ctx context.Context, chainID, address string) (domain.SweepTarget, bool, error)
}
