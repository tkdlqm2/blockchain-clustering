package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/pipeline"
)

// KafkaReader is the slice of *kafka.Reader's API this package needs,
// narrowed to an interface so the dispatch/batching logic is testable
// without a live broker.
type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// PipelineRunner is pipeline.Pipeline's batch entrypoint.
type PipelineRunner interface {
	Run(ctx context.Context, chainID string, deltas []domain.BalanceDelta) (pipeline.Result, error)
}

// ReorgNotifier is reorg.Handler's rollback entrypoint.
type ReorgNotifier interface {
	OnReorg(ctx context.Context, chainID string, rolledBackBlockHashes []string) (int, error)
}

// Consumer reads the balance-deltas topic (docs/08-indexer-contract.md) and
// drives Pipeline/ReorgHandler from it. It is the only place a Kafka
// offset gets committed, and only after the data it covers has been
// durably processed (docs comment on Batcher) — never before.
type Consumer struct {
	reader        KafkaReader
	pipeline      PipelineRunner
	reorg         ReorgNotifier
	batcher       *Batcher
	flushInterval time.Duration
}

func New(reader KafkaReader, pipelineRunner PipelineRunner, reorgNotifier ReorgNotifier, flushInterval time.Duration) *Consumer {
	return &Consumer{
		reader:        reader,
		pipeline:      pipelineRunner,
		reorg:         reorgNotifier,
		batcher:       NewBatcher(),
		flushInterval: flushInterval,
	}
}

// Run consumes until ctx is cancelled, at which point it flushes whatever's
// pending (using a fresh background context, since ctx is already done) and
// returns nil. Any other error is returned to the caller, which should
// treat it as fatal — kafka-go's Reader will redeliver uncommitted messages
// on the next run, so crashing and restarting is a safe response.
func (c *Consumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	type fetched struct {
		msg kafka.Message
		err error
	}
	msgCh := make(chan fetched)
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()

	go func() {
		for {
			m, err := c.reader.FetchMessage(fetchCtx)
			select {
			case msgCh <- fetched{m, err}:
			case <-fetchCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return c.flushBatch(context.Background())

		case <-ticker.C:
			// Safety net: if no new block has arrived for a while, don't
			// leave the last one sitting unprocessed indefinitely.
			if err := c.flushBatch(ctx); err != nil {
				slog.Error("consumer: periodic flush failed", "error", err)
			}

		case f := <-msgCh:
			if f.err != nil {
				if ctx.Err() != nil {
					return c.flushBatch(context.Background())
				}
				return fmt.Errorf("consumer: fetch message: %w", f.err)
			}
			if err := c.handle(ctx, f.msg); err != nil {
				return fmt.Errorf("consumer: handle message (partition=%d offset=%d): %w", f.msg.Partition, f.msg.Offset, err)
			}
		}
	}
}

func (c *Consumer) handle(ctx context.Context, m kafka.Message) error {
	delta, reorgNotice, err := ParseMessage(m.Value)
	if err != nil {
		// A message we can't even parse would block this partition forever
		// if we don't commit past it — log loudly for manual follow-up and
		// move on, rather than wedge the whole consumer on one bad message.
		slog.Error("consumer: unparseable message, skipping", "partition", m.Partition, "offset", m.Offset, "error", err)
		return c.reader.CommitMessages(ctx, m)
	}

	if reorgNotice != nil {
		// docs/08 §3.2: the indexer publishes reorg *before* republishing the
		// new chain, so anything already batched must be durable first —
		// flush before acting on the rollback, not after.
		if err := c.flushBatch(ctx); err != nil {
			return err
		}
		if _, err := c.reorg.OnReorg(ctx, reorgNotice.ChainID, reorgNotice.RolledBackBlockHashes); err != nil {
			return fmt.Errorf("consumer: on_reorg: %w", err)
		}
		return c.reader.CommitMessages(ctx, m)
	}

	flushDeltas, flushMessages, flushChainID, ready := c.batcher.Add(*delta, m)
	if ready {
		if err := c.runAndCommit(ctx, flushChainID, flushDeltas, flushMessages); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) flushBatch(ctx context.Context) error {
	deltas, messages, chainID := c.batcher.Flush()
	return c.runAndCommit(ctx, chainID, deltas, messages)
}

func (c *Consumer) runAndCommit(ctx context.Context, chainID string, deltas []domain.BalanceDelta, messages []kafka.Message) error {
	if len(deltas) == 0 {
		return nil
	}
	if _, err := c.pipeline.Run(ctx, chainID, deltas); err != nil {
		return fmt.Errorf("consumer: pipeline run (chain=%s): %w", chainID, err)
	}
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		return fmt.Errorf("consumer: commit offsets: %w", err)
	}
	return nil
}
