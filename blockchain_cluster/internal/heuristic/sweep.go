package heuristic

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/pgnumeric"
)

const HeuristicKeySweepSeed = "sweep-seed"

// sweepParams are sweep-seed-specific tuning, read from
// chain_heuristic.params for heuristic_key='sweep-seed' — owned entirely by
// this engine, not the registry (docs/04 §2 [3] plugin principle: the
// registry stores opaque params, only the engine interprets them).
type sweepParams struct {
	// CompletenessMin: minimum fraction of an address's cumulative received
	// total it must spend in one tx toward a known target to look like a
	// sweep, not an ordinary payment (docs/03 §5 "residualBalanceNearZero").
	CompletenessMin float64
	// RecurrenceMin: how many distinct source addresses must have sent to a
	// target before it's treated as a genuine recurring collection point,
	// not a one-off coincidence (docs/03 §5 "recurringToSameTarget").
	RecurrenceMin int
}

func defaultSweepParams() sweepParams {
	return sweepParams{CompletenessMin: 0.9, RecurrenceMin: 2}
}

func parseSweepParams(raw json.RawMessage) sweepParams {
	params := defaultSweepParams()
	if len(raw) == 0 {
		return params
	}
	var overrides struct {
		CompletenessMin *float64 `json:"completeness_min"`
		RecurrenceMin   *int     `json:"recurrence_min"`
	}
	if err := json.Unmarshal(raw, &overrides); err != nil {
		return params // malformed params: fall back to defaults rather than fail the batch
	}
	if overrides.CompletenessMin != nil {
		params.CompletenessMin = *overrides.CompletenessMin
	}
	if overrides.RecurrenceMin != nil {
		params.RecurrenceMin = *overrides.RecurrenceMin
	}
	return params
}

// SweepEngine implements sweepHeuristic (docs/03-clustering-algorithms.md
// §5): deposit addresses that sweep completely and repeatedly into a known
// collection target are merged onto that target. It deliberately never
// merges the outside address that *funded* a deposit address — only the
// deposit address itself and the target (docs/03 §5 "경계선").
//
// docs/03 §5's expandFromSeeds mentions decaying confidence by hop distance
// from a seed. This engine only detects direct (one-hop) sweeps; a deposit
// address that's itself later swept elsewhere gets picked up as its own
// direct sweep when that transaction is processed, and transitive
// membership then follows from Union-Find/replay (docs/02 §6) rather than
// from a hop-decayed confidence value here. True hop-aware decay would need
// to track distance-from-seed through the merge graph, which is a
// confidence-combination concern deferred to M6 alongside noisy-OR
// (docs/03 §7), not reimplemented per-heuristic.
type SweepEngine struct {
	pool    *pgxpool.Pool
	targets SweepTargetChecker
	config  ConfigProvider
}

func NewSweepEngine(pool *pgxpool.Pool, targets SweepTargetChecker, config ConfigProvider) *SweepEngine {
	return &SweepEngine{pool: pool, targets: targets, config: config}
}

func (e *SweepEngine) Name() string { return HeuristicKeySweepSeed }

func (e *SweepEngine) Generate(ctx context.Context, deltas []domain.BalanceDelta) ([]domain.MergeCandidate, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	chainID := deltas[0].ChainID

	cfg, found, err := e.config.ConfigFor(ctx, chainID, HeuristicKeySweepSeed)
	if err != nil {
		return nil, fmt.Errorf("sweep: config lookup: %w", err)
	}
	if !found || !cfg.Enabled {
		return nil, nil
	}
	params := parseSweepParams(cfg.Params)

	var candidates []domain.MergeCandidate
	for _, tx := range ingestor.GroupByTx(deltas) {
		outs := ingestor.ReceivedEntries(tx.Deltas)
		ins := ingestor.SpentAddresses(tx.Deltas)
		if len(ins) == 0 || len(outs) == 0 {
			continue
		}

		for _, out := range outs {
			target, isTarget, err := e.targets.Get(ctx, tx.Key.ChainID, out.Address)
			if err != nil {
				return nil, fmt.Errorf("sweep: is_known_target(%s): %w", out.Address, err)
			}
			if !isTarget {
				continue
			}

			for _, src := range ins {
				if src == out.Address {
					continue
				}
				ok, err := e.looksLikeSweep(ctx, tx.Key.ChainID, tx.Deltas, src, out.Address, params)
				if err != nil {
					return nil, fmt.Errorf("sweep: looks_like_sweep(%s,%s): %w", src, out.Address, err)
				}
				if !ok {
					continue
				}

				confidence := cfg.Confidence
				if target.Confidence < confidence {
					confidence = target.Confidence // conservative: min of heuristic confidence and seed trust
				}

				txid := tx.Key.TxID
				blockHash := tx.Deltas[0].BlockHash
				blockHeight := tx.Deltas[0].BlockHeight
				candidates = append(candidates, domain.MergeCandidate{
					ChainID:           tx.Key.ChainID,
					AddressA:          out.Address, // target anchors the merge (docs/03 §5)
					AddressB:          src,
					HeuristicKey:      HeuristicKeySweepSeed,
					SourceTxID:        &txid,
					SourceBlockHash:   &blockHash,
					SourceBlockHeight: &blockHeight,
					Confidence:        confidence,
				})
			}
		}
	}
	return candidates, nil
}

// looksLikeSweep implements docs/03 §5's two conditions:
//   - residualBalanceNearZero: src spent, in this tx alone, at least
//     CompletenessMin of everything it has ever received (a one-off partial
//     payment won't clear this bar; a genuine "collect and forward" sweep will).
//   - recurringToSameTarget: dst has received from at least RecurrenceMin
//     distinct source addresses across all observed transactions — i.e. dst
//     is a real, repeated collection point, not a coincidental single payment.
func (e *SweepEngine) looksLikeSweep(ctx context.Context, chainID string, txDeltas []domain.BalanceDelta, src, dst string, params sweepParams) (bool, error) {
	spentInTx := big.NewInt(0)
	for _, d := range txDeltas {
		if d.Address == src && d.Amount != nil && d.Amount.Sign() < 0 {
			spentInTx.Sub(spentInTx, d.Amount) // amount is negative; subtract to accumulate a positive total
		}
	}
	if spentInTx.Sign() <= 0 {
		return false, nil
	}

	cumulativeReceived, err := e.cumulativeReceived(ctx, chainID, src)
	if err != nil {
		return false, err
	}
	if cumulativeReceived.Sign() <= 0 {
		return false, nil // no receive history to compare against — not enough evidence
	}

	completeness := new(big.Float).Quo(new(big.Float).SetInt(spentInTx), new(big.Float).SetInt(cumulativeReceived))
	completenessOK := completeness.Cmp(big.NewFloat(params.CompletenessMin)) >= 0
	if !completenessOK {
		return false, nil
	}

	recurrence, err := e.distinctSourcesToTarget(ctx, chainID, dst)
	if err != nil {
		return false, err
	}
	return recurrence >= params.RecurrenceMin, nil
}

func (e *SweepEngine) cumulativeReceived(ctx context.Context, chainID, address string) (*big.Int, error) {
	var sum pgtype.Numeric
	err := e.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0) FROM clustering.balance_delta
		WHERE chain_id = $1 AND address = $2 AND amount > 0
	`, chainID, address).Scan(&sum)
	if err != nil {
		return nil, fmt.Errorf("cumulative_received: %w", err)
	}
	v, err := pgnumeric.ToBigInt(sum)
	if err != nil {
		return nil, fmt.Errorf("cumulative_received: %w", err)
	}
	return v, nil
}

func (e *SweepEngine) distinctSourcesToTarget(ctx context.Context, chainID, target string) (int, error) {
	var count int
	err := e.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT spender.address)
		FROM clustering.balance_delta recv
		JOIN clustering.balance_delta spender
		  ON spender.chain_id = recv.chain_id AND spender.txid = recv.txid
		 AND spender.amount < 0 AND spender.address <> recv.address
		WHERE recv.chain_id = $1 AND recv.address = $2 AND recv.amount > 0
	`, chainID, target).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("distinct_sources_to_target: %w", err)
	}
	return count, nil
}
