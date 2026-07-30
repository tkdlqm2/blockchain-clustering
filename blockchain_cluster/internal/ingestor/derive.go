// Package ingestor implements Ingestor (docs/04-architecture §2 [1]):
// idempotent BalanceDelta collection plus the per-transaction derivations
// that later heuristics (M2+) build on (docs/02-data-model.md §1 "파생 규칙").
package ingestor

import (
	"math/big"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// TxKey identifies one transaction within one chain.
type TxKey struct {
	ChainID string
	TxID    string
}

// TxGroup is one transaction's deltas, in GroupByTx's output order.
type TxGroup struct {
	Key    TxKey
	Deltas []domain.BalanceDelta
}

// GroupByTx groups deltas by (chain, txid) — this is groupByTx in docs/03 §0.
// It returns an ordered slice, not a map: heuristic engines (M2+) append
// MergeEvidence in the order they iterate transactions, and that append
// order determines op_id order, which determines the canonical cluster
// anchor on replay (docs/02 §6). A map here would make anchor selection
// vary across runs of the identical batch — this preserves first-occurrence
// order from the input slice instead, which is deterministic whenever the
// input itself came from an ORDER BY query (as Ingestor's reads do).
func GroupByTx(deltas []domain.BalanceDelta) []TxGroup {
	index := make(map[TxKey]int, len(deltas))
	var groups []TxGroup
	for _, d := range deltas {
		key := TxKey{ChainID: d.ChainID, TxID: d.TxID}
		if i, ok := index[key]; ok {
			groups[i].Deltas = append(groups[i].Deltas, d)
			continue
		}
		index[key] = len(groups)
		groups = append(groups, TxGroup{Key: key, Deltas: []domain.BalanceDelta{d}})
	}
	return groups
}

// SpentAddresses returns the deduplicated set of addresses with a negative
// (spent) delta in a single transaction — the "공통 입력 집합" that
// commonInputHeuristic groups on (docs/03 §4). Order is first-occurrence,
// so callers that need a deterministic anchor (e.g. ins[0]) get one.
func SpentAddresses(txDeltas []domain.BalanceDelta) []string {
	seen := make(map[string]bool, len(txDeltas))
	out := make([]string, 0, len(txDeltas))
	for _, d := range txDeltas {
		if d.Amount == nil || d.Amount.Sign() >= 0 {
			continue
		}
		if !seen[d.Address] {
			seen[d.Address] = true
			out = append(out, d.Address)
		}
	}
	return out
}

// ReceivedEntry is one recipient address and the positive amount it
// received within a transaction.
type ReceivedEntry struct {
	Address string
	Amount  *big.Int
}

// ReceivedEntries returns the "수취 집합" — positive-amount deltas — used by
// the sweep and change heuristics (docs/03 §5, §5b).
func ReceivedEntries(txDeltas []domain.BalanceDelta) []ReceivedEntry {
	var out []ReceivedEntry
	for _, d := range txDeltas {
		if d.Amount == nil || d.Amount.Sign() <= 0 {
			continue
		}
		out = append(out, ReceivedEntry{Address: d.Address, Amount: d.Amount})
	}
	return out
}
