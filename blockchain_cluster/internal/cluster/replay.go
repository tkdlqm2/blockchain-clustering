// Package cluster implements ClusterStore — the derived cache materialized
// by replaying merge_evidence (docs/02-data-model.md §4-5, §6; docs/04 §2 [6]).
package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/unionfind"
)

// Member is one address's derived membership in a replayed cluster.
type Member struct {
	Address    string
	Confidence float64
}

// Result is one materialized cluster produced by Replay.
type Result struct {
	ClusterID                string
	AnchorAddress            string
	Size                     int64
	RepresentativeConfidence float64
	Members                  []Member
}

// pairKey identifies an unordered address pair — normalized so (a,b) and
// (b,a) collide, since merge_evidence can record either order.
type pairKey struct{ a, b string }

func normalizePair(a, b string) pairKey {
	if a <= b {
		return pairKey{a, b}
	}
	return pairKey{b, a}
}

// Replay is the pure, deterministic core of rebuildMembershipFromEvidence
// (docs/03-clustering-algorithms.md §6, §9, §7). Given the active
// merge_evidence for a chain — MUST be ordered by op_id ascending, which is
// what makes replay reproducible — it returns the resulting clusters.
//
// Confidence combination happens in two layers (docs/03 §7):
//  1. Per address pair: multiple *independent* pieces of evidence (different
//     source transactions) combine via noisy-OR (combined = 1 - Π(1-conf_i)),
//     raising confidence. Multiple evidence rows sharing the same
//     source_txid are the same observation, not independent, so they take
//     the max instead of compounding (docs/03 §7: "근거들이 사실상 같은
//     원천이면 독립이 아님 → 곱하지 말 것").
//  2. Per member, along the path back to the cluster's anchor: the
//     conservative minimum edge weight (docs/03 §7 "결합값(보수적 선택
//     권장)"), following the spanning tree formed by the merges that
//     actually connected new addresses.
//
// Threshold views (re-deriving clusters from only evidence at or above a
// confidence floor) are not this function's concern — Replay always uses
// everything it's given; ClusterStore filters the input before calling it
// (docs/03 §7's "그 임계치 이상 근거로만 재구성한 클러스터").
func Replay(evidence []domain.MergeEvidence) []Result {
	firstSeenOrder := make(map[string]int)
	nextOrder := 0
	touch := func(addr string) {
		if _, ok := firstSeenOrder[addr]; !ok {
			firstSeenOrder[addr] = nextOrder
			nextOrder++
		}
	}

	type pairEvidence struct {
		firstOrder int
		bySource   map[string]float64 // source key -> max confidence seen for that source
	}
	pairs := make(map[pairKey]*pairEvidence)

	streamIndex := 0
	for _, e := range evidence {
		// Defensive: callers (EvidenceStore.ScanActive) already filter to
		// active-only, but Replay stays correct even if given a mixed set.
		if e.Status != domain.EvidenceStatusActive {
			continue
		}
		// Self-referential evidence should never be appended (recordAndMerge
		// guards against it), but skip defensively rather than form a
		// meaningless singleton "cluster".
		if e.AddressA == e.AddressB {
			continue
		}

		touch(e.AddressA)
		touch(e.AddressB)

		key := normalizePair(e.AddressA, e.AddressB)
		pe, ok := pairs[key]
		if !ok {
			pe = &pairEvidence{firstOrder: streamIndex, bySource: make(map[string]float64)}
			pairs[key] = pe
		}
		source := sourceKey(e)
		if cur, ok := pe.bySource[source]; !ok || e.Confidence > cur {
			pe.bySource[source] = e.Confidence
		}
		streamIndex++
	}

	type combinedEdge struct {
		key        pairKey
		confidence float64
		firstOrder int
	}
	combinedEdges := make([]combinedEdge, 0, len(pairs))
	for key, pe := range pairs {
		combinedEdges = append(combinedEdges, combinedEdge{
			key:        key,
			confidence: noisyOR(pe.bySource),
			firstOrder: pe.firstOrder,
		})
	}
	// Union in first-appearance order so anchor selection (below) reflects
	// actual chronology regardless of Go's random map iteration order.
	sort.Slice(combinedEdges, func(i, j int) bool { return combinedEdges[i].firstOrder < combinedEdges[j].firstOrder })

	dsu := unionfind.New()
	type edge struct {
		to         string
		confidence float64
	}
	adjacency := make(map[string][]edge)
	for _, ce := range combinedEdges {
		if dsu.Union(ce.key.a, ce.key.b) {
			adjacency[ce.key.a] = append(adjacency[ce.key.a], edge{ce.key.b, ce.confidence})
			adjacency[ce.key.b] = append(adjacency[ce.key.b], edge{ce.key.a, ce.confidence})
		}
	}

	componentMembers := make(map[string][]string)
	for addr := range firstSeenOrder {
		root := dsu.Find(addr)
		componentMembers[root] = append(componentMembers[root], addr)
	}

	results := make([]Result, 0, len(componentMembers))
	for _, addrs := range componentMembers {
		anchor := addrs[0]
		for _, a := range addrs[1:] {
			if firstSeenOrder[a] < firstSeenOrder[anchor] {
				anchor = a
			}
		}

		confidence := map[string]float64{anchor: 1.0}
		visited := map[string]bool{anchor: true}
		queue := []string{anchor}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, e := range adjacency[cur] {
				if visited[e.to] {
					continue
				}
				visited[e.to] = true
				c := confidence[cur]
				if e.confidence < c {
					c = e.confidence
				}
				confidence[e.to] = c
				queue = append(queue, e.to)
			}
		}

		members := make([]Member, 0, len(addrs))
		var confidenceSum float64
		for _, a := range addrs {
			members = append(members, Member{Address: a, Confidence: confidence[a]})
			confidenceSum += confidence[a]
		}

		results = append(results, Result{
			ClusterID:                deriveClusterID(anchor),
			AnchorAddress:            anchor,
			Size:                     int64(len(addrs)),
			RepresentativeConfidence: confidenceSum / float64(len(addrs)),
			Members:                  members,
		})
	}
	return results
}

// deriveClusterID implements the canonical-anchor stability rule
// (docs/02-data-model.md §6): a deterministic function of the anchor
// address, so replaying the same evidence always yields the same id.
func deriveClusterID(anchor string) string {
	sum := sha256.Sum256([]byte(anchor))
	return hex.EncodeToString(sum[:16])
}

// sourceKey identifies where one piece of evidence came from, for grouping
// "independent" vs "same observation" (docs/03 §7). On-chain evidence is
// keyed by its transaction — two rows from the same tx are the same
// observation. Off-chain evidence (manual/seed, no source_txid) has no
// natural shared key, so each op_id is treated as its own independent
// source — that's correct for "manual", which represents a distinct
// operator assertion each time.
func sourceKey(e domain.MergeEvidence) string {
	if e.SourceTxID != nil {
		return *e.SourceTxID
	}
	return fmt.Sprintf("op:%d", e.OpID)
}

// noisyOR combines independent per-source confidences (docs/03 §7):
// combined = 1 - Π(1 - conf_i). With one source, this is just that
// source's confidence, unchanged.
func noisyOR(bySource map[string]float64) float64 {
	product := 1.0
	for _, c := range bySource {
		product *= 1 - c
	}
	return 1 - product
}
