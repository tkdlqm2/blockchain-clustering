package consumer

import (
	"math/big"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

func testDelta(chainID, blockHash, addr string) domain.BalanceDelta {
	return domain.BalanceDelta{ChainID: chainID, BlockHash: blockHash, Address: addr, Amount: big.NewInt(1)}
}

func msg(offset int64) kafka.Message {
	return kafka.Message{Offset: offset}
}

func TestBatcher_SameBlockAccumulatesWithoutFlushing(t *testing.T) {
	b := NewBatcher()

	_, _, _, ready := b.Add(testDelta("eth", "block1", "A"), msg(0))
	if ready {
		t.Fatalf("expected no flush on the first delta")
	}
	_, _, _, ready = b.Add(testDelta("eth", "block1", "B"), msg(1))
	if ready {
		t.Fatalf("expected no flush while still in the same block")
	}

	deltas, messages, chainID := b.Flush()
	if len(deltas) != 2 || len(messages) != 2 || chainID != "eth" {
		t.Fatalf("expected 2 accumulated deltas on explicit flush, got %d/%d chain=%s", len(deltas), len(messages), chainID)
	}
}

func TestBatcher_NewBlockTriggersFlushOfPrevious(t *testing.T) {
	b := NewBatcher()
	b.Add(testDelta("eth", "block1", "A"), msg(0))
	b.Add(testDelta("eth", "block1", "B"), msg(1))

	flushDeltas, flushMessages, flushChainID, ready := b.Add(testDelta("eth", "block2", "C"), msg(2))
	if !ready {
		t.Fatalf("expected a flush when block_hash changes")
	}
	if len(flushDeltas) != 2 || flushChainID != "eth" {
		t.Fatalf("expected block1's 2 deltas flushed, got %d chain=%s", len(flushDeltas), flushChainID)
	}
	if len(flushMessages) != 2 || flushMessages[0].Offset != 0 || flushMessages[1].Offset != 1 {
		t.Fatalf("expected block1's messages (offsets 0,1) flushed, got %+v", flushMessages)
	}

	// block2's delta (C) must now be pending, not lost.
	deltas, _, _ := b.Flush()
	if len(deltas) != 1 || deltas[0].Address != "C" {
		t.Fatalf("expected block2's delta C still pending after the flush, got %+v", deltas)
	}
}

func TestBatcher_DifferentChainTriggersFlush(t *testing.T) {
	b := NewBatcher()
	b.Add(testDelta("eth", "block1", "A"), msg(0))

	_, _, flushChainID, ready := b.Add(testDelta("bitcoin", "blockX", "B"), msg(1))
	if !ready || flushChainID != "eth" {
		t.Fatalf("expected a flush of the eth batch when a different chain's delta arrives, got ready=%v chain=%s", ready, flushChainID)
	}
}

func TestBatcher_FlushOnEmptyIsSafe(t *testing.T) {
	b := NewBatcher()
	deltas, messages, chainID := b.Flush()
	if deltas != nil || messages != nil || chainID != "" {
		t.Fatalf("expected a no-op flush on an empty batcher, got %+v %+v %q", deltas, messages, chainID)
	}
}
