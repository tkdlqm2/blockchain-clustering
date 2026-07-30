package unionfind

import "testing"

func TestUnionFind_Basic(t *testing.T) {
	d := New()

	if merged := d.Union("A", "B"); !merged {
		t.Fatalf("expected first union of A,B to report merged=true")
	}
	if merged := d.Union("A", "C"); !merged {
		t.Fatalf("expected union of A,C to report merged=true")
	}
	if merged := d.Union("B", "C"); merged {
		t.Fatalf("expected redundant union of B,C (already same set) to report merged=false")
	}

	if d.Find("A") != d.Find("B") || d.Find("B") != d.Find("C") {
		t.Fatalf("A, B, C should all share a root after unioning")
	}

	if d.Find("D") == d.Find("A") {
		t.Fatalf("unrelated address D should not share a root with A")
	}
}

func TestUnionFind_FindRegistersSingleton(t *testing.T) {
	d := New()
	if d.Find("X") != "X" {
		t.Fatalf("an untouched address should be its own root")
	}
}
