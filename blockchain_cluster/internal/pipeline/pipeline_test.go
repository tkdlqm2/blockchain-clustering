package pipeline

import (
	"context"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
)

type fakeIngester struct{ log *orderLog }

func (f *fakeIngester) Ingest(_ context.Context, deltas []domain.BalanceDelta) (ingestor.IngestResult, error) {
	f.log.record("ingest")
	return ingestor.IngestResult{Inserted: len(deltas)}, nil
}

// orderLog is shared by every fake below so a single Run call produces one
// ordered trace of everything that happened — this is what lets the test
// assert AC-3 directly (preprocessing entries all precede heuristic
// entries, which precede merge, which precedes rebuild).
type orderLog struct{ events []string }

func (l *orderLog) record(event string) { l.events = append(l.events, event) }

type fakeHubMarker struct{ log *orderLog }

func (f *fakeHubMarker) MarkHubs(_ context.Context, _ string, _ []string, _ domain.PreprocessingParams) error {
	f.log.record("mark_hubs")
	return nil
}

type fakeCollaborativeMarker struct{ log *orderLog }

func (f *fakeCollaborativeMarker) MarkCollaborativeTx(_ context.Context, _ []domain.BalanceDelta, _ domain.PreprocessingParams) error {
	f.log.record("mark_collaborative_tx")
	return nil
}

type fakeDustMarker struct{ log *orderLog }

func (f *fakeDustMarker) MarkDust(_ context.Context, _ []domain.BalanceDelta, _ domain.PreprocessingParams, _ preprocessor.AddressDustStore) error {
	f.log.record("mark_dust")
	return nil
}

type fakeDustAddresses struct{}

func (fakeDustAddresses) SetDustFlag(_ context.Context, _, _ string) error { return nil }
func (fakeDustAddresses) Get(_ context.Context, _, _ string) (domain.Address, bool, error) {
	return domain.Address{}, false, nil
}

type fakeParamsProvider struct{}

func (fakeParamsProvider) PreprocessingParamsFor(_ context.Context, _ string) (domain.PreprocessingParams, error) {
	return domain.PreprocessingParams{}, nil
}

type fakeEngine struct {
	name string
	log  *orderLog
}

func (f *fakeEngine) Name() string { return f.name }
func (f *fakeEngine) Generate(_ context.Context, _ []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	f.log.record("generate:" + f.name)
	return []domain.MergeCandidate{{ChainID: "bitcoin", AddressA: "A", AddressB: "B", HeuristicKey: f.name, Confidence: 0.5}}, nil
}

type fakeMergeRecorder struct{ log *orderLog }

func (f *fakeMergeRecorder) RecordAndMergeBatch(_ context.Context, candidates []domain.MergeCandidate) (merge.BatchResult, error) {
	f.log.record("record_and_merge_batch")
	return merge.BatchResult{Recorded: len(candidates)}, nil
}

type fakeClusterRebuilder struct{ log *orderLog }

func (f *fakeClusterRebuilder) RebuildFromEvidence(_ context.Context, _ string) error {
	f.log.record("rebuild_from_evidence")
	return nil
}

func TestPipeline_EnforcesPreprocessingBeforeHeuristicsBeforeMergeBeforeRebuild(t *testing.T) {
	log := &orderLog{}
	p := New(
		&fakeIngester{log: log},
		&fakeHubMarker{log: log},
		&fakeCollaborativeMarker{log: log},
		&fakeDustMarker{log: log},
		fakeDustAddresses{},
		fakeParamsProvider{},
		[]heuristic.Engine{
			&fakeEngine{name: "common-input", log: log},
			&fakeEngine{name: "sweep-seed", log: log},
		},
		&fakeMergeRecorder{log: log},
		&fakeClusterRebuilder{log: log},
	)

	deltas := []domain.BalanceDelta{{ChainID: "bitcoin", TxID: "tx1", Address: "A"}}
	result, err := p.Run(context.Background(), "bitcoin", deltas)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.CandidatesGenerated != 2 || result.Recorded != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}

	want := []string{
		"ingest",
		"mark_hubs", "mark_collaborative_tx", "mark_dust",
		"generate:common-input", "generate:sweep-seed",
		"record_and_merge_batch",
		"rebuild_from_evidence",
	}
	if len(log.events) != len(want) {
		t.Fatalf("expected %d events, got %d: %v", len(want), len(log.events), log.events)
	}
	for i, event := range want {
		if log.events[i] != event {
			t.Fatalf("event order violated at position %d: got %v, want %v", i, log.events, want)
		}
	}
}

func TestPipeline_EmptyBatchIsNoop(t *testing.T) {
	log := &orderLog{}
	p := New(
		&fakeIngester{log: log},
		&fakeHubMarker{log: log}, &fakeCollaborativeMarker{log: log}, &fakeDustMarker{log: log},
		fakeDustAddresses{}, fakeParamsProvider{},
		[]heuristic.Engine{&fakeEngine{name: "common-input", log: log}},
		&fakeMergeRecorder{log: log}, &fakeClusterRebuilder{log: log},
	)

	result, err := p.Run(context.Background(), "bitcoin", nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.CandidatesGenerated != 0 {
		t.Fatalf("expected no-op result, got %+v", result)
	}
	if len(log.events) != 0 {
		t.Fatalf("expected no side effects for an empty batch, got %v", log.events)
	}
}
