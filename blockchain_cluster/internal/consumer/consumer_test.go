package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/pipeline"
)

type fakeReader struct {
	committed []kafka.Message
}

func (f *fakeReader) FetchMessage(_ context.Context) (kafka.Message, error) {
	return kafka.Message{}, errors.New("fakeReader.FetchMessage: not used by these tests (handle() is called directly)")
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

type pipelineCall struct {
	chainID string
	deltas  []domain.BalanceDelta
}

type fakePipeline struct {
	calls []pipelineCall
	err   error
}

func (f *fakePipeline) Run(_ context.Context, chainID string, deltas []domain.BalanceDelta) (pipeline.Result, error) {
	if f.err != nil {
		return pipeline.Result{}, f.err
	}
	f.calls = append(f.calls, pipelineCall{chainID, deltas})
	return pipeline.Result{Recorded: len(deltas)}, nil
}

type reorgCall struct {
	chainID string
	hashes  []string
}

type fakeReorg struct {
	calls []reorgCall
}

func (f *fakeReorg) OnReorg(_ context.Context, chainID string, hashes []string) (int, error) {
	f.calls = append(f.calls, reorgCall{chainID, hashes})
	return len(hashes), nil
}

func balanceDeltaMsg(offset int64, chainID, txid, address, amount, blockHash string) kafka.Message {
	body := `{"type":"balance_delta","chain_id":"` + chainID + `","txid":"` + txid + `","address":"` + address +
		`","amount":"` + amount + `","kind":"native","block_height":1,"block_hash":"` + blockHash + `"}`
	return kafka.Message{Offset: offset, Value: []byte(body)}
}

func reorgMsg(offset int64, chainID string, hashes ...string) kafka.Message {
	quoted := ""
	for i, h := range hashes {
		if i > 0 {
			quoted += ","
		}
		quoted += `"` + h + `"`
	}
	body := `{"type":"reorg","chain_id":"` + chainID + `","rolled_back_block_hashes":[` + quoted + `]}`
	return kafka.Message{Offset: offset, Value: []byte(body)}
}

func newTestConsumer(reader *fakeReader, pl *fakePipeline, rg *fakeReorg) *Consumer {
	return New(reader, pl, rg, time.Hour) // long interval: tests drive flushing explicitly
}

func TestConsumer_AccumulatesUntilNewBlockThenRunsAndCommits(t *testing.T) {
	reader := &fakeReader{}
	pl := &fakePipeline{}
	c := newTestConsumer(reader, pl, &fakeReorg{})
	ctx := context.Background()

	if err := c.handle(ctx, balanceDeltaMsg(0, "eth", "tx1", "A", "-100", "block1")); err != nil {
		t.Fatalf("handle msg0: %v", err)
	}
	if err := c.handle(ctx, balanceDeltaMsg(1, "eth", "tx1", "B", "100", "block1")); err != nil {
		t.Fatalf("handle msg1: %v", err)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("expected no pipeline call yet (still same block), got %d", len(pl.calls))
	}
	if len(reader.committed) != 0 {
		t.Fatalf("expected no commits yet, got %d", len(reader.committed))
	}

	// New block triggers a flush of block1's 2 deltas.
	if err := c.handle(ctx, balanceDeltaMsg(2, "eth", "tx2", "C", "-50", "block2")); err != nil {
		t.Fatalf("handle msg2: %v", err)
	}
	if len(pl.calls) != 1 || len(pl.calls[0].deltas) != 2 {
		t.Fatalf("expected 1 pipeline call with 2 deltas, got %+v", pl.calls)
	}
	if len(reader.committed) != 2 || reader.committed[0].Offset != 0 || reader.committed[1].Offset != 1 {
		t.Fatalf("expected offsets 0,1 committed, got %+v", reader.committed)
	}
}

func TestConsumer_ReorgFlushesPendingFirstThenCallsOnReorg(t *testing.T) {
	reader := &fakeReader{}
	pl := &fakePipeline{}
	rg := &fakeReorg{}
	c := newTestConsumer(reader, pl, rg)
	ctx := context.Background()

	if err := c.handle(ctx, balanceDeltaMsg(0, "eth", "tx1", "A", "-100", "block1")); err != nil {
		t.Fatalf("handle delta: %v", err)
	}
	if err := c.handle(ctx, reorgMsg(1, "eth", "block1")); err != nil {
		t.Fatalf("handle reorg: %v", err)
	}

	if len(pl.calls) != 1 {
		t.Fatalf("expected the pending delta flushed before the reorg call, got %d pipeline calls", len(pl.calls))
	}
	if len(rg.calls) != 1 || rg.calls[0].chainID != "eth" || rg.calls[0].hashes[0] != "block1" {
		t.Fatalf("expected OnReorg called with chain=eth hashes=[block1], got %+v", rg.calls)
	}
	if len(reader.committed) != 2 { // the flushed delta message + the reorg message itself
		t.Fatalf("expected 2 commits (delta + reorg), got %d", len(reader.committed))
	}
}

func TestConsumer_PoisonMessageSkippedAndCommitted(t *testing.T) {
	reader := &fakeReader{}
	pl := &fakePipeline{}
	c := newTestConsumer(reader, pl, &fakeReorg{})
	ctx := context.Background()

	bad := kafka.Message{Offset: 5, Value: []byte("not json")}
	if err := c.handle(ctx, bad); err != nil {
		t.Fatalf("expected poison message to be skipped, not returned as an error: %v", err)
	}
	if len(reader.committed) != 1 || reader.committed[0].Offset != 5 {
		t.Fatalf("expected the poison message committed past (so it isn't retried forever), got %+v", reader.committed)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("expected no pipeline call for a poison message")
	}
}

func TestConsumer_PipelineErrorPreventsCommit(t *testing.T) {
	reader := &fakeReader{}
	pl := &fakePipeline{err: errors.New("db is down")}
	c := newTestConsumer(reader, pl, &fakeReorg{})
	ctx := context.Background()

	c.handle(ctx, balanceDeltaMsg(0, "eth", "tx1", "A", "-100", "block1")) //nolint:errcheck
	err := c.handle(ctx, balanceDeltaMsg(1, "eth", "tx2", "B", "-50", "block2"))
	if err == nil {
		t.Fatalf("expected an error when the pipeline fails")
	}
	if len(reader.committed) != 0 {
		t.Fatalf("expected NO commits when pipeline.Run fails — committing here would lose block1's data forever, got %+v", reader.committed)
	}
}

func TestConsumer_FlushBatchIsNoopWhenNothingPending(t *testing.T) {
	reader := &fakeReader{}
	pl := &fakePipeline{}
	c := newTestConsumer(reader, pl, &fakeReorg{})

	if err := c.flushBatch(context.Background()); err != nil {
		t.Fatalf("expected flushing an empty batcher to be a no-op, got %v", err)
	}
	if len(pl.calls) != 0 || len(reader.committed) != 0 {
		t.Fatalf("expected no pipeline calls or commits from an empty flush")
	}
}
