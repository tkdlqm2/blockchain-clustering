// Package unionfind implements a path-compressed, union-by-rank disjoint-set
// over string keys (addresses), as specified in docs/03-clustering-algorithms.md §6.
//
// It is a pure in-memory structure with no knowledge of merge_evidence or
// persistence — callers are responsible for feeding it unions in a
// deterministic order (op_id ascending) so that replay is reproducible.
package unionfind

type DSU struct {
	parent map[string]string
	rank   map[string]int
}

func New() *DSU {
	return &DSU{
		parent: make(map[string]string),
		rank:   make(map[string]int),
	}
}

// Find returns the root of x's set, registering x as its own singleton set
// on first sight, and compressing the path it walks.
func (d *DSU) Find(x string) string {
	if _, ok := d.parent[x]; !ok {
		d.parent[x] = x
		d.rank[x] = 0
		return x
	}
	if d.parent[x] != x {
		d.parent[x] = d.Find(d.parent[x])
	}
	return d.parent[x]
}

// Union merges the sets containing a and b. It reports whether a merge
// actually happened (false if a and b were already in the same set) — the
// caller uses this to know whether the edge (a,b) belongs to the spanning
// tree of the resulting component.
func (d *DSU) Union(a, b string) bool {
	ra, rb := d.Find(a), d.Find(b)
	if ra == rb {
		return false
	}
	switch {
	case d.rank[ra] < d.rank[rb]:
		ra, rb = rb, ra
	case d.rank[ra] == d.rank[rb]:
		d.rank[ra]++
	}
	d.parent[rb] = ra
	return true
}
