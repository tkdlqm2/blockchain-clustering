// Package pipeline implements runPipeline (docs/03-clustering-algorithms.md
// §0): the one place that wires Preprocessor, HeuristicEngines, MergeEngine,
// and ClusterStore together in the order the whole system's correctness
// depends on.
//
// This closes a real gap: through M0-M8, every component was built and
// tested individually, but nothing enforced that preprocessing actually
// runs before heuristics on a given batch — AC-3 (docs/05 §2: "전처리가
// 휴리스틱보다 먼저 실행됨이 코드/테스트로 보장된다") was only true "if the
// caller happens to call things in the right order." Pipeline.Run is that
// enforcement: it's the only supported entrypoint for processing a batch,
// and its body is the order guarantee, not just documentation of one.
//
// This is what a Kafka consumer (still TODO — see CLAUDE.md) would call
// once the BalanceDelta envelope is finalized.
package pipeline

import (
	"context"
	"fmt"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
)

// Ingester is Ingestor's write path (docs/04 §2 [1]). Run calls this first
// — every downstream step (preprocessing, heuristics) reads balance_delta
// from the database, not from the in-memory deltas slice, so if the batch
// hasn't actually been persisted yet, they'd silently see nothing. This was
// a real gap: earlier tests always called Ingest() manually before Run(),
// which masked that Run() itself never guaranteed it.
type Ingester interface {
	Ingest(ctx context.Context, deltas []domain.BalanceDelta) (ingestor.IngestResult, error)
}

// HubMarker is HubDetector's write path (docs/03 §1).
type HubMarker interface {
	MarkHubs(ctx context.Context, chainID string, addresses []string, params domain.PreprocessingParams) error
}

// CollaborativeMarker is the excluded_tx write path for CoinJoin detection (docs/03 §2).
type CollaborativeMarker interface {
	MarkCollaborativeTx(ctx context.Context, deltas []domain.BalanceDelta, params domain.PreprocessingParams) error
}

// DustMarker is the excluded_tx/dust_flag write path (docs/03 §3).
type DustMarker interface {
	MarkDust(ctx context.Context, deltas []domain.BalanceDelta, params domain.PreprocessingParams, addrs preprocessor.AddressDustStore) error
}

// ParamsProvider resolves a chain's preprocessing thresholds (docs/03 §10:
// no hardcoded parameters).
type ParamsProvider interface {
	PreprocessingParamsFor(ctx context.Context, chainID string) (domain.PreprocessingParams, error)
}

// MergeRecorder is MergeEngine's batch entrypoint.
type MergeRecorder interface {
	RecordAndMergeBatch(ctx context.Context, candidates []domain.MergeCandidate) (merge.BatchResult, error)
}

// ClusterRebuilder is ClusterStore's replay entrypoint.
type ClusterRebuilder interface {
	RebuildFromEvidence(ctx context.Context, chainID string) error
}

// Pipeline wires one batch's processing end to end. Every dependency is a
// narrow interface (mirroring the rest of this codebase's style) so the
// ordering guarantee itself is unit-testable without a database.
type Pipeline struct {
	ingest        Ingester
	hubs          HubMarker
	collaborative CollaborativeMarker
	dust          DustMarker
	dustAddresses preprocessor.AddressDustStore
	params        ParamsProvider
	engines       []heuristic.Engine
	merge         MergeRecorder
	cluster       ClusterRebuilder
}

func New(
	ingest Ingester,
	hubs HubMarker,
	collaborative CollaborativeMarker,
	dust DustMarker,
	dustAddresses preprocessor.AddressDustStore,
	params ParamsProvider,
	engines []heuristic.Engine,
	mergeEngine MergeRecorder,
	clusterStore ClusterRebuilder,
) *Pipeline {
	return &Pipeline{
		ingest: ingest, hubs: hubs, collaborative: collaborative, dust: dust, dustAddresses: dustAddresses,
		params: params, engines: engines, merge: mergeEngine, cluster: clusterStore,
	}
}

// Result tallies one Run.
type Result struct {
	CandidatesGenerated int
	Recorded            int
	Rejected            int
}

// Run implements docs/03 §0's runPipeline for one chain's batch:
//
//	[0] Ingestor persists the batch (idempotent) — everything after this
//	    point reads balance_delta from the database, not from deltas itself
//	[A] preprocessing (markHubs, markCollaborativeTx, markDust) — always first
//	[B] every registered heuristic engine generates candidates
//	[C] MergeEngine records them as evidence
//	[D] ClusterStore replays to materialize the derived cache
//
// [D] here corresponds to expandFromSeeds/rebuild in the pseudocode's spirit
// — making the batch's effect queryable — rather than the seed-registration
// side of expandFromSeeds, which is an operator action (FR-25), not part of
// per-batch processing.
func (p *Pipeline) Run(ctx context.Context, chainID string, deltas []domain.BalanceDelta) (Result, error) {
	if len(deltas) == 0 {
		return Result{}, nil
	}

	if _, err := p.ingest.Ingest(ctx, deltas); err != nil {
		return Result{}, fmt.Errorf("pipeline: ingest: %w", err)
	}

	params, err := p.params.PreprocessingParamsFor(ctx, chainID)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: preprocessing_params_for: %w", err)
	}

	// [A] Preprocessing — must complete before any heuristic runs (AC-3).
	if err := p.hubs.MarkHubs(ctx, chainID, addressesIn(deltas), params); err != nil {
		return Result{}, fmt.Errorf("pipeline: mark_hubs: %w", err)
	}
	if err := p.collaborative.MarkCollaborativeTx(ctx, deltas, params); err != nil {
		return Result{}, fmt.Errorf("pipeline: mark_collaborative_tx: %w", err)
	}
	if err := p.dust.MarkDust(ctx, deltas, params, p.dustAddresses); err != nil {
		return Result{}, fmt.Errorf("pipeline: mark_dust: %w", err)
	}

	// [B] Heuristics — plugin loop; a new engine added to the slice needs no
	// change here (docs/04 §2 [3] plugin principle).
	var candidates []domain.MergeCandidate
	for _, engine := range p.engines {
		generated, err := engine.Generate(ctx, deltas)
		if err != nil {
			return Result{}, fmt.Errorf("pipeline: %s: %w", engine.Name(), err)
		}
		candidates = append(candidates, generated...)
	}

	// [C] Merge — record evidence.
	batchResult, err := p.merge.RecordAndMergeBatch(ctx, candidates)
	if err != nil {
		return Result{}, fmt.Errorf("pipeline: record_and_merge_batch: %w", err)
	}

	// [D] Materialize — make this batch's effect queryable.
	if err := p.cluster.RebuildFromEvidence(ctx, chainID); err != nil {
		return Result{}, fmt.Errorf("pipeline: rebuild_from_evidence: %w", err)
	}

	return Result{
		CandidatesGenerated: len(candidates),
		Recorded:            batchResult.Recorded,
		Rejected:            batchResult.Rejected,
	}, nil
}

func addressesIn(deltas []domain.BalanceDelta) []string {
	seen := make(map[string]bool, len(deltas))
	var out []string
	for _, d := range deltas {
		if !seen[d.Address] {
			seen[d.Address] = true
			out = append(out, d.Address)
		}
	}
	return out
}
