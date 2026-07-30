package heuristic

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
)

const HeuristicKeyChange = "change"

type changeParams struct {
	// ChangeMin: minimum combined weak-signal score to even bother emitting
	// a candidate (docs/03 §5b "confidenceOf(cand) >= CHANGE_MIN"). The
	// candidate's actual recorded confidence is separately capped at the
	// heuristic's own registry confidence (min(CONF_CHANGE, confidenceOf) —
	// docs/03 §5b), which is why this default is deliberately low: getting
	// past ChangeMin only means "worth recording as a weak signal", not
	// "trustworthy".
	ChangeMin float64
	// Weights for guessChangeOutput's clues (docs/03 §5b). sameScriptType is
	// not implemented — BalanceDelta carries no script-type field (only
	// native/token "kind"), so there's nothing to compare.
	NewAddressWeight     float64
	NotRoundAmountWeight float64
	RespentWeight        float64
}

func defaultChangeParams() changeParams {
	return changeParams{
		ChangeMin:            0.3,
		NewAddressWeight:     0.5,
		NotRoundAmountWeight: 0.2,
		RespentWeight:        0.3,
	}
}

func parseChangeParams(raw json.RawMessage) changeParams {
	params := defaultChangeParams()
	if len(raw) == 0 {
		return params
	}
	var overrides struct {
		ChangeMin            *float64 `json:"change_min"`
		NewAddressWeight     *float64 `json:"new_address_weight"`
		NotRoundAmountWeight *float64 `json:"not_round_amount_weight"`
		RespentWeight        *float64 `json:"respent_weight"`
	}
	if err := json.Unmarshal(raw, &overrides); err != nil {
		return params
	}
	if overrides.ChangeMin != nil {
		params.ChangeMin = *overrides.ChangeMin
	}
	if overrides.NewAddressWeight != nil {
		params.NewAddressWeight = *overrides.NewAddressWeight
	}
	if overrides.NotRoundAmountWeight != nil {
		params.NotRoundAmountWeight = *overrides.NotRoundAmountWeight
	}
	if overrides.RespentWeight != nil {
		params.RespentWeight = *overrides.RespentWeight
	}
	return params
}

// AddressFirstSeenChecker is address.Store's read path — used for
// guessChangeOutput's "new address" clue.
type AddressFirstSeenChecker interface {
	Get(ctx context.Context, chainID, address string) (domain.Address, bool, error)
}

// ChangeEngine implements changeHeuristic (docs/03-clustering-algorithms.md
// §5b): a deliberately conservative, low-confidence heuristic — a change
// misjudgment pulls an unrelated counterparty into the anchor's cluster,
// which is exactly the false-positive class this system is built to avoid
// (docs/00 §"핵심 설계 원칙" 4, docs/03 §5b). It never confirms on a single
// clue; every output candidate's clues are summed and must clear ChangeMin,
// and the final confidence is capped by the heuristic's own registry
// confidence (docs/03 §7's min(...) combination pattern, reused here).
type ChangeEngine struct {
	pool      *pgxpool.Pool
	hubs      HubChecker
	excluded  ExclusionChecker
	addresses AddressFirstSeenChecker
	config    ConfigProvider
}

func NewChangeEngine(pool *pgxpool.Pool, hubs HubChecker, excluded ExclusionChecker, addresses AddressFirstSeenChecker, config ConfigProvider) *ChangeEngine {
	return &ChangeEngine{pool: pool, hubs: hubs, excluded: excluded, addresses: addresses, config: config}
}

func (e *ChangeEngine) Name() string { return HeuristicKeyChange }

func (e *ChangeEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	cfg, found, err := e.config.ConfigFor(ctx, chainID, HeuristicKeyChange)
	if err != nil {
		return nil, fmt.Errorf("change: config lookup: %w", err)
	}
	if !found || !cfg.Enabled {
		return nil, nil
	}
	params := parseChangeParams(cfg.Params)

	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		excluded, err := e.excluded.IsExcluded(ctx, tx.Key.ChainID, tx.Key.TxID)
		if err != nil {
			return nil, fmt.Errorf("change: is_excluded(%s): %w", tx.Key.TxID, err)
		}
		if excluded {
			continue
		}

		ins := ingestor.SpentAddresses(tx.Deltas)
		outs := ingestor.ReceivedEntries(tx.Deltas)
		if len(ins) == 0 || len(outs) == 0 {
			continue
		}

		hubTouch, err := e.touchesHub(ctx, tx.Key.ChainID, ins, outs)
		if err != nil {
			return nil, fmt.Errorf("change: touches_hub(%s): %w", tx.Key.TxID, err)
		}
		if hubTouch {
			continue
		}

		blockHeight := tx.Deltas[0].BlockHeight
		var best ingestor.ReceivedEntry
		var bestScore float64
		var haveBest bool
		for _, out := range outs {
			score, err := e.scoreChangeCandidate(ctx, tx.Key.ChainID, blockHeight, out, params)
			if err != nil {
				return nil, fmt.Errorf("change: score(%s): %w", out.Address, err)
			}
			if !haveBest || score > bestScore {
				best, bestScore, haveBest = out, score, true
			}
		}
		if !haveBest || bestScore < params.ChangeMin {
			continue
		}

		confidence := bestScore
		if cfg.Confidence < confidence {
			confidence = cfg.Confidence // min(CONF_CHANGE, confidenceOf(cand)) — docs/03 §5b
		}

		txid := tx.Key.TxID
		blockHash := tx.Deltas[0].BlockHash
		candidates = append(candidates, domain.MergeCandidate{
			ChainID:           tx.Key.ChainID,
			AddressA:          ins[0],
			AddressB:          best.Address,
			HeuristicKey:      HeuristicKeyChange,
			SourceTxID:        &txid,
			SourceBlockHash:   &blockHash,
			SourceBlockHeight: &blockHeight,
			Confidence:        confidence,
		})
	}
	return candidates, nil
}

func (e *ChangeEngine) touchesHub(ctx context.Context, chainID string, ins []string, outs []ingestor.ReceivedEntry) (bool, error) {
	for _, a := range ins {
		isHub, err := e.hubs.IsHub(ctx, chainID, a)
		if err != nil {
			return false, err
		}
		if isHub {
			return true, nil
		}
	}
	for _, o := range outs {
		isHub, err := e.hubs.IsHub(ctx, chainID, o.Address)
		if err != nil {
			return false, err
		}
		if isHub {
			return true, nil
		}
	}
	return false, nil
}

func (e *ChangeEngine) scoreChangeCandidate(ctx context.Context, chainID string, txBlockHeight int64, out ingestor.ReceivedEntry, params changeParams) (float64, error) {
	var score float64

	addr, found, err := e.addresses.Get(ctx, chainID, out.Address)
	if err != nil {
		return 0, err
	}
	if found && addr.FirstSeenHeight != nil && *addr.FirstSeenHeight == txBlockHeight {
		score += params.NewAddressWeight
	}

	if !isRoundAmount(out.Amount) {
		score += params.NotRoundAmountWeight
	}

	respent, err := e.wasRespentAfter(ctx, chainID, out.Address, txBlockHeight)
	if err != nil {
		return 0, err
	}
	if respent {
		score += params.RespentWeight
	}

	return score, nil
}

func (e *ChangeEngine) wasRespentAfter(ctx context.Context, chainID, address string, blockHeight int64) (bool, error) {
	var exists bool
	err := e.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM clustering.balance_delta
			WHERE chain_id = $1 AND address = $2 AND amount < 0 AND block_height > $3
		)
	`, chainID, address, blockHeight).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("was_respent_after: %w", err)
	}
	return exists, nil
}

// isRoundAmount is guessChangeOutput's "반올림 안 된 금액" clue, inverted:
// an amount divisible by 1000 (in whatever unit BalanceDelta reports) looks
// like a deliberately chosen payment size rather than leftover change.
func isRoundAmount(amount *big.Int) bool {
	if amount == nil {
		return false
	}
	return new(big.Int).Mod(amount, big.NewInt(1000)).Sign() == 0
}
