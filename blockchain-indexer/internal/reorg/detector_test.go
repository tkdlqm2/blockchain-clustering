package reorg

import (
	"context"
	"testing"
)

// fakeStore는 인덱서가 저장한(과거) 체인 상태를 흉내낸다: height -> hash.
type fakeStore struct {
	byHeight map[int64]string
}

func (f *fakeStore) GetBlockHash(_ context.Context, _ string, height int64) (string, bool, error) {
	h, ok := f.byHeight[height]
	return h, ok, nil
}

// fakeNode는 노드가 지금 보고하는(실제 canonical) 체인 상태를 흉내낸다.
type fakeNode struct {
	byHeight map[int64]string
}

func (f *fakeNode) BlockHash(_ context.Context, height int64) (string, error) {
	return f.byHeight[height], nil
}

// T-13/14 대응: depth-1 reorg — height 10만 갈라졌고 9는 그대로.
func TestFindRollback_ShallowReorg(t *testing.T) {
	store := &fakeStore{byHeight: map[int64]string{
		8: "h8", 9: "h9", 10: "h10-old",
	}}
	node := &fakeNode{byHeight: map[int64]string{
		9: "h9", 10: "h10-new",
	}}
	d := New()

	rolledBack, ancestor, err := d.FindRollback(context.Background(), store, node, "ethereum", 11)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ancestor != 9 {
		t.Fatalf("공통 조상 = %d, want 9", ancestor)
	}
	if len(rolledBack) != 1 || rolledBack[0] != "h10-old" {
		t.Fatalf("rolledBack = %v, want [h10-old]", rolledBack)
	}
}

// depth-3 reorg — 8,9,10 전부 갈라짐, 7이 공통 조상.
func TestFindRollback_DeepReorg(t *testing.T) {
	store := &fakeStore{byHeight: map[int64]string{
		7: "h7", 8: "h8-old", 9: "h9-old", 10: "h10-old",
	}}
	node := &fakeNode{byHeight: map[int64]string{
		7: "h7", 8: "h8-new", 9: "h9-new", 10: "h10-new",
	}}
	d := New()

	rolledBack, ancestor, err := d.FindRollback(context.Background(), store, node, "ethereum", 11)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ancestor != 7 {
		t.Fatalf("공통 조상 = %d, want 7", ancestor)
	}
	want := []string{"h10-old", "h9-old", "h8-old"}
	if len(rolledBack) != len(want) {
		t.Fatalf("rolledBack 길이 = %d, want %d (%v)", len(rolledBack), len(want), rolledBack)
	}
	for i := range want {
		if rolledBack[i] != want[i] {
			t.Fatalf("rolledBack[%d] = %s, want %s", i, rolledBack[i], want[i])
		}
	}
}

// 저장된 블록이 아예 없는 초기 동기화 구간 — rollback 없이 height-1이 조상.
func TestFindRollback_NoStoredHistory(t *testing.T) {
	store := &fakeStore{byHeight: map[int64]string{}}
	node := &fakeNode{byHeight: map[int64]string{}}
	d := New()

	rolledBack, ancestor, err := d.FindRollback(context.Background(), store, node, "ethereum", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ancestor != 4 {
		t.Fatalf("공통 조상 = %d, want 4", ancestor)
	}
	if len(rolledBack) != 0 {
		t.Fatalf("rolledBack = %v, want empty", rolledBack)
	}
}

func TestIsContinuous(t *testing.T) {
	store := &fakeStore{byHeight: map[int64]string{9: "h9"}}
	d := New()

	ok, err := d.IsContinuous(context.Background(), store, "ethereum", 10, "h9")
	if err != nil || !ok {
		t.Fatalf("연속인데 불연속 판정됨: ok=%v err=%v", ok, err)
	}

	ok, err = d.IsContinuous(context.Background(), store, "ethereum", 10, "h9-wrong")
	if err != nil || ok {
		t.Fatalf("불연속인데 연속 판정됨: ok=%v err=%v", ok, err)
	}

	// 저장된 블록 없음 → 초기 동기화 구간으로 간주, 연속 취급.
	ok, err = d.IsContinuous(context.Background(), store, "ethereum", 100, "아무거나")
	if err != nil || !ok {
		t.Fatalf("저장된 블록 없는 경우 연속으로 처리되지 않음: ok=%v err=%v", ok, err)
	}
}
