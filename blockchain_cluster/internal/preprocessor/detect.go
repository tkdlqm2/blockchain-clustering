// Package preprocessor implements Preprocessor (docs/04-architecture §2 [2]):
// markHubs, markCollaborativeTx, markDust — pollution blocked *before*
// heuristics run (docs/03 §0's "불변 규칙": [A] preprocessing always
// precedes [B] heuristics).
//
// markHubs' full pseudocode (docs/03 §1(b)) scores hubs on three signals:
// distinct-counterparty count, tx rate, and sweep convergence. This
// implementation only computes the first — txRate needs real timestamps
// that BalanceDelta doesn't carry (docs/02 §1 has only block_height), and
// sweepConvergence properly belongs with M4's sweep-detection heuristic
// (computing it well here would mean reimplementing looksLikeSweep early).
// Counterparty degree alone is a real, conservative, tunable starting
// signal — refining it with the other two signals is future work, not a
// silent gap: it's called out here and in CLAUDE.md.
package preprocessor

import (
	"math/big"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
)

// CollaborativeTxDetection is one transaction markCollaborativeTx decided to
// exclude.
type CollaborativeTxDetection struct {
	ChainID    string
	TxID       string
	Confidence float64
}

// DetectCollaborativeTx implements markCollaborativeTx's measurable core
// (docs/03 §2): many inputs, many outputs, and a large group of equal-value
// outputs (the anonymity-set signal). The pseudocode's optional
// structureMatchesKnownImpl (wallet fingerprinting against Wasabi/Whirlpool/
// JoinMarket) is not implemented — it needs a fingerprint database this
// system doesn't have, and pretending to check it would be worse than
// omitting it.
func DetectCollaborativeTx(deltas []domain.BalanceDelta, params domain.PreprocessingParams) []CollaborativeTxDetection {
	var out []CollaborativeTxDetection
	for _, tx := range ingestor.GroupByTx(deltas) {
		ins := ingestor.SpentAddresses(tx.Deltas)
		outs := ingestor.ReceivedEntries(tx.Deltas)

		if maxEqualAmountGroup(outs) >= params.EqualOutputMin &&
			len(ins) >= params.CollabInputMin &&
			len(outs) >= params.CollabOutputMin {
			out = append(out, CollaborativeTxDetection{
				ChainID:    tx.Key.ChainID,
				TxID:       tx.Key.TxID,
				Confidence: params.CoinjoinConfidence,
			})
		}
	}
	return out
}

func maxEqualAmountGroup(outs []ingestor.ReceivedEntry) int {
	counts := make(map[string]int, len(outs))
	max := 0
	for _, o := range outs {
		key := o.Amount.String()
		counts[key]++
		if counts[key] > max {
			max = counts[key]
		}
	}
	return max
}

// AddressRef identifies one address on one chain.
type AddressRef struct {
	ChainID string
	Address string
}

// DustInflows implements markDust's first phase (docs/03 §3): the
// deduplicated set of addresses that received a dust-sized inflow in this
// batch. Only positive (received) deltas at or below dustThreshold count.
func DustInflows(deltas []domain.BalanceDelta, dustThreshold *big.Int) []AddressRef {
	seen := make(map[AddressRef]bool, len(deltas))
	var out []AddressRef
	for _, d := range deltas {
		if d.Amount == nil || d.Amount.Sign() <= 0 {
			continue
		}
		if d.Amount.Cmp(dustThreshold) > 0 {
			continue
		}
		ref := AddressRef{d.ChainID, d.Address}
		if !seen[ref] {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	return out
}
