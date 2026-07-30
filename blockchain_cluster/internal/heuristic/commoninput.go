package heuristic

import (
	"context"
	"fmt"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
)

const HeuristicKeyCommonInput = "common-input"

// CommonInputEngine implements commonInputHeuristic (docs/03-clustering-
// algorithms.md §4): addresses spent together in one transaction are
// merge candidates, anchored on the first (post-hub-filter) spender.
type CommonInputEngine struct {
	hubs       HubChecker
	excluded   ExclusionChecker
	confidence ConfidenceProvider
}

func NewCommonInputEngine(hubs HubChecker, excluded ExclusionChecker, confidence ConfidenceProvider) *CommonInputEngine {
	return &CommonInputEngine{hubs: hubs, excluded: excluded, confidence: confidence}
}

func (e *CommonInputEngine) Name() string { return HeuristicKeyCommonInput }

func (e *CommonInputEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	confidence, enabled, err := e.confidence.ConfidenceFor(ctx, chainID, HeuristicKeyCommonInput)
	if err != nil {
		return nil, fmt.Errorf("common-input: confidence lookup: %w", err)
	}
	if !enabled {
		// Not registered/enabled for this chain (docs/06 §2.2) — nothing to do,
		// not an error: e.g. an account-model chain simply has no row here.
		return nil, nil
	}

	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		excluded, err := e.excluded.IsExcluded(ctx, tx.Key.ChainID, tx.Key.TxID)
		if err != nil {
			return nil, fmt.Errorf("common-input: is_excluded(%s): %w", tx.Key.TxID, err)
		}
		if excluded {
			continue
		}

		spenders := ingestor.SpentAddresses(tx.Deltas)

		var ins []string
		for _, addr := range spenders {
			isHub, err := e.hubs.IsHub(ctx, tx.Key.ChainID, addr)
			if err != nil {
				return nil, fmt.Errorf("common-input: is_hub(%s): %w", addr, err)
			}
			if !isHub {
				ins = append(ins, addr)
			}
		}
		if len(ins) < 2 {
			continue
		}

		blockHash := tx.Deltas[0].BlockHash
		blockHeight := tx.Deltas[0].BlockHeight
		txid := tx.Key.TxID

		anchor := ins[0]
		for _, a := range ins[1:] {
			candidates = append(candidates, domain.MergeCandidate{
				ChainID:           tx.Key.ChainID,
				AddressA:          anchor,
				AddressB:          a,
				HeuristicKey:      HeuristicKeyCommonInput,
				SourceTxID:        &txid,
				SourceBlockHash:   &blockHash,
				SourceBlockHeight: &blockHeight,
				Confidence:        confidence,
			})
		}
	}
	return candidates, nil
}
