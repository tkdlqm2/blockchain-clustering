// Package domain holds the chain-agnostic types shared across storage and
// clustering packages, mirroring docs/02-data-model.md.
package domain

import (
	"encoding/json"
	"math/big"
	"time"
)

const (
	EvidenceStatusActive      = "active"
	EvidenceStatusInvalidated = "invalidated"
)

// BalanceDelta is the external input this system consumes from the indexer
// (docs/02-data-model.md §1) — not defined by this system, only referenced.
// Amount is a signed arbitrary-precision integer (spend <0, receive >0) so
// that uint256 token amounts survive without loss, matching the NUMERIC(78,0)
// column (docs/06-multichain-extensibility.md §4).
type BalanceDelta struct {
	ChainID     string
	TxID        string
	DeltaIndex  int32
	Address     string
	Amount      *big.Int
	Kind        string // "native" | "token"
	BlockHeight int64
	BlockHash   string
	Meta        json.RawMessage
}

// MergeCandidate is what a HeuristicEngine emits — never a merge itself
// (FR-12). MergeEngine.RecordAndMerge turns it into MergeEvidence after
// re-checking hub exclusion (docs/04-architecture §2 [3]).
type MergeCandidate struct {
	ChainID           string
	AddressA          string
	AddressB          string
	HeuristicKey      string
	SourceTxID        *string
	SourceBlockHash   *string
	SourceBlockHeight *int64
	Confidence        float64
}

// MergeEvidence is a single append-only record from merge_evidence — the
// source of truth for cluster membership (docs/02-data-model.md §3).
type MergeEvidence struct {
	ChainID           string
	OpID              int64
	AddressA          string
	AddressB          string
	HeuristicKey      string
	SourceTxID        *string
	SourceBlockHash   *string
	SourceBlockHeight *int64
	Confidence        float64
	Status            string
	InvalidatedReason *string
	CreatedAt         time.Time
	CreatedBy         string
}

// Cluster is the derived-cache representative record for a set of addresses
// (docs/02-data-model.md §4).
type Cluster struct {
	ChainID                  string
	ClusterID                string
	Size                     int64
	EntityType               string
	RepresentativeConfidence float64
	UpdatedAt                time.Time
}

// ClusterMembership maps a single address to its cluster (docs/02-data-model.md §5).
type ClusterMembership struct {
	ChainID              string
	Address              string
	ClusterID            string
	MembershipConfidence float64
}

// Address is the per-address registry row used by preprocessing (hub/dust)
// and clustering (docs/02-data-model.md §2).
type Address struct {
	ChainID         string
	Address         string
	FirstSeenHeight *int64
	LastSeenHeight  *int64
	IsHub           bool
	HubType         *string
	HubConfidence   *float64
	DustFlag        bool
}

// PreprocessingParams are Preprocessor's tunable thresholds (docs/03
// §1-§3, §10: "모든 파라미터는 설정 가능해야 하며 하드코딩 금지"). They are
// read from chain.config JSONB per chain (registry.PreprocessingParamsFor),
// never hardcoded as Go constants.
type PreprocessingParams struct {
	// HubThreshold and HubDegreeSaturation drive markHubs' behavioral score
	// (docs/03 §1(b)): score = min(distinct-counterparty-degree /
	// HubDegreeSaturation, 1), flagged as hub when score >= HubThreshold.
	// txRate and sweepConvergence from the §1(b) pseudocode are deliberately
	// not implemented here — see internal/preprocessor package doc.
	HubThreshold        float64
	HubDegreeSaturation float64

	// DustThreshold: inflows at or below this are dust (docs/03 §3).
	DustThreshold *big.Int

	// CoinjoinConfidence/DustExclusionConfidence: the detector_confidence
	// recorded on excluded_tx rows this preprocessor produces.
	CoinjoinConfidence      float64
	DustExclusionConfidence float64

	// EQUAL_OUTPUT_MIN / COLLAB_INPUT_MIN / COLLAB_OUTPUT_MIN from
	// markCollaborativeTx (docs/03 §2).
	EqualOutputMin  int
	CollabInputMin  int
	CollabOutputMin int
}

// HeuristicConfig is what a HeuristicEngine needs to know before running for
// a given chain: whether it's enabled, its confidence, and any
// heuristic-specific tuning parameters (registry.Store.ConfigFor —
// docs/03 §10: no hardcoded parameters). Params is left as opaque JSON
// because each heuristic engine owns its own parameter shape — the registry
// doesn't need to know it (docs/04 §2 [3] plugin principle).
type HeuristicConfig struct {
	Enabled    bool
	Confidence float64
	Params     json.RawMessage
}

// SweepTarget is a known collection destination (docs/02-data-model.md
// §8.2) — the anchor sweepHeuristic merges deposit addresses onto.
type SweepTarget struct {
	ChainID    string
	Address    string
	EntityHint *string
	Source     string // "known-deposit" | "observed"
	Confidence float64
}

// Label attaches identity to a cluster or address (docs/02-data-model.md §7).
type Label struct {
	LabelID          int64
	TargetType       string // "cluster" | "address"
	ChainID          string
	TargetClusterID  *string
	TargetAddress    *string
	Label            string
	Category         string
	Source           string
	SourceConfidence float64
	CollectedAt      time.Time
	LastVerifiedAt   time.Time
	Status           string
}
