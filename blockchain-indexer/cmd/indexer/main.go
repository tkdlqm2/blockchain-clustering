// cmd/indexer는 워커 기동 골격이다. 메인 루프 의사코드는 docs/03 §0 참고.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/powerwaves5307/blockchain-indexer/internal/adapter/ethereum"
	"github.com/powerwaves5307/blockchain-indexer/internal/config"
	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
	"github.com/powerwaves5307/blockchain-indexer/internal/kafka"
	"github.com/powerwaves5307/blockchain-indexer/internal/metrics"
	"github.com/powerwaves5307/blockchain-indexer/internal/reorg"
	"github.com/powerwaves5307/blockchain-indexer/internal/state"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("config 로드 실패: %v", err)
	}

	pool, err := state.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("postgres 연결 실패: %v", err)
	}
	defer pool.Close()
	queries := state.New(pool)

	publisher := kafka.NewPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer publisher.Close()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		log.Printf("metrics listening on %s", cfg.Metrics.ListenAddr)
		if err := http.ListenAndServe(cfg.Metrics.ListenAddr, mux); err != nil {
			log.Printf("metrics server 종료: %v", err)
		}
	}()

	chains, err := queries.ListEnabledChainConfigs(ctx)
	if err != nil {
		log.Fatalf("chain_config 조회 실패: %v", err)
	}
	if len(chains) == 0 {
		log.Println("enabled=true인 체인이 없습니다 — chain_config를 확인하세요")
	}

	detector := reorg.New()

	var wg sync.WaitGroup
	for _, cc := range chains {
		cc := cc
		switch cc.ModelType {
		case "account":
			node, err := ethereum.New(ctx, cc.NodeEndpoint)
			if err != nil {
				log.Printf("[%s] ethereum 어댑터 생성 실패: %v", cc.ChainID, err)
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				runEthereumLoop(ctx, cc.ChainID, cc.StartHeight, node, pool, queries, publisher, detector)
			}()
		case "utxo":
			log.Printf("[%s] UTXO 어댑터는 아직 미구현(M3) — 건너뜀", cc.ChainID)
		default:
			log.Printf("[%s] 알 수 없는 model_type: %s", cc.ChainID, cc.ModelType)
		}
	}

	<-ctx.Done()
	log.Println("shutting down")
	wg.Wait()
}

// queryBlockStore는 reorg.BlockStore를 state.Queries로 구현한다.
type queryBlockStore struct {
	q *state.Queries
}

func (s queryBlockStore) GetBlockHash(ctx context.Context, chainID string, height int64) (string, bool, error) {
	b, err := s.q.GetBlockAtHeight(ctx, state.GetBlockAtHeightParams{ChainID: chainID, Height: height})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return b.Hash, true, nil
}

// runEthereumLoop은 docs/03 §0 indexLoop 의사코드의 계정 체인 구현이다.
// 노드/발행 오류는 지수 백오프로 재시도한다(FR-21). reorg는 감지 시 통지를 먼저 발행한 뒤
// 내부 상태를 공통 조상까지 롤백한다(docs/03 §3, docs/06 §5).
func runEthereumLoop(ctx context.Context, chainID string, startHeight int64, node *ethereum.Adapter, pool *pgxpool.Pool, q *state.Queries, pub *kafka.Publisher, detector *reorg.Detector) {
	store := queryBlockStore{q: q}
	bo := newBackoff()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		latest, err := node.LatestHeight(ctx)
		if err != nil {
			log.Printf("[%s] 최신 높이 조회 실패: %v", chainID, err)
			sleep(ctx, bo.next())
			continue
		}

		cursor, err := q.GetCursor(ctx, chainID)
		var height int64
		switch {
		case err == nil:
			height = cursor.Height
		case errors.Is(err, pgx.ErrNoRows):
			// 커서가 아직 없는 최초 기동. start_height가 설정돼 있으면 거기서부터 백필하고,
			// 0(미설정)이면 방금 조회한 latest 근처(현재 tip)부터 실시간 팔로우로 시작한다
			// — genesis(0)부터 전체를 순회하는 걸 기본값으로 두지 않기 위함.
			if startHeight > 0 {
				height = startHeight - 1
			} else {
				height = latest - 1
			}
		default:
			log.Printf("[%s] 커서 조회 실패: %v", chainID, err)
			sleep(ctx, bo.next())
			continue
		}

		metrics.IndexingLagBlocks.WithLabelValues(chainID).Set(float64(latest - height))

		if height >= latest {
			bo.reset()
			sleep(ctx, 10*time.Second)
			continue
		}
		next := height + 1

		block, err := node.FetchBlock(ctx, next)
		if err != nil {
			log.Printf("[%s] 블록 조회 실패(height=%d): %v", chainID, next, err)
			sleep(ctx, bo.next())
			continue
		}
		rawBlock := node.RawBlock(block)

		continuous, err := detector.IsContinuous(ctx, store, chainID, next, rawBlock.ParentHash)
		if err != nil {
			log.Printf("[%s] 연속성 검증 실패: %v", chainID, err)
			sleep(ctx, bo.next())
			continue
		}
		if !continuous {
			if err := handleReorg(ctx, chainID, next, node, q, pub, detector, store); err != nil {
				log.Printf("[%s] reorg 처리 실패: %v — 재시도", chainID, err)
				sleep(ctx, bo.next())
			}
			continue // 성공/실패 무관하게 다음 루프에서 (롤백된) 커서 기준으로 다시 진행
		}

		var deltas []domain.BalanceDelta
		for _, tx := range block.Transactions() {
			txDeltas, err := node.Extract(ctx, chainID, tx, rawBlock)
			if err != nil {
				log.Printf("[%s] tx 추출 실패(tx=%s): %v", chainID, tx.Hash().Hex(), err)
				continue
			}
			deltas = append(deltas, txDeltas...)
		}

		if err := pub.PublishDeltas(ctx, deltas); err != nil {
			metrics.PublishFailuresTotal.WithLabelValues(chainID).Inc()
			log.Printf("[%s] 발행 실패(height=%d): %v — 커서 미전진, 재시도", chainID, next, err)
			sleep(ctx, bo.next())
			continue
		}

		if err := persistBlockAndCursor(ctx, pool, chainID, next, rawBlock.Hash, rawBlock.ParentHash, block.Time()); err != nil {
			log.Printf("[%s] 상태 저장 실패(height=%d): %v", chainID, next, err)
			sleep(ctx, bo.next())
			continue
		}

		bo.reset()
		log.Printf("[%s] height=%d 처리 완료, delta %d건 발행", chainID, next, len(deltas))
	}
}

// persistBlockAndCursor는 block 저장과 cursor 전진을 하나의 DB 트랜잭션으로 묶는다.
// 이 둘 사이에 크래시가 나서 block만 저장되고 cursor가 그대로인 채로 재시작되더라도,
// InsertBlock이 ON CONFLICT DO NOTHING이라 재시도가 안전하다 (FR-16).
func persistBlockAndCursor(ctx context.Context, pool *pgxpool.Pool, chainID string, height int64, hash, parentHash string, blockTime uint64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx 시작 실패: %w", err)
	}
	defer tx.Rollback(ctx)

	q := state.New(tx)
	if err := q.InsertBlock(ctx, state.InsertBlockParams{
		ChainID:    chainID,
		Height:     height,
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  pgtype.Timestamptz{Time: time.Unix(int64(blockTime), 0), Valid: true},
	}); err != nil {
		return fmt.Errorf("block 저장 실패: %w", err)
	}
	// 발행 완료 후에만 커서 전진 (FR-16, docs/03 §0 writeCursor).
	if err := q.UpsertCursor(ctx, state.UpsertCursorParams{ChainID: chainID, Height: height}); err != nil {
		return fmt.Errorf("커서 갱신 실패: %w", err)
	}
	return tx.Commit(ctx)
}

// handleReorg는 docs/03 §3 handleReorg를 구현한다: 공통 조상 탐색 → reorg 이벤트 발행(먼저) →
// orphan 블록 삭제 → 커서를 공통 조상으로 되돌림. 다음 루프에서 공통 조상+1부터 재추출된다.
func handleReorg(ctx context.Context, chainID string, newHeight int64, node *ethereum.Adapter, q *state.Queries, pub *kafka.Publisher, detector *reorg.Detector, store queryBlockStore) error {
	rolledBack, ancestor, err := detector.FindRollback(ctx, store, node, chainID, newHeight)
	if err != nil {
		return fmt.Errorf("공통 조상 탐색 실패: %w", err)
	}

	metrics.ReorgTotal.WithLabelValues(chainID).Inc()
	metrics.ReorgDepthBlocks.WithLabelValues(chainID).Observe(float64(len(rolledBack)))
	log.Printf("[%s] reorg 감지: %d개 블록 롤백, 공통 조상 height=%d", chainID, len(rolledBack), ancestor)

	// 통지 먼저, 그 다음 재발행 (docs/03 §3, docs/06 §5) — 소비자가 무효화를 마친 뒤 새 데이터를 받게 함.
	if err := pub.PublishReorg(ctx, domain.ReorgEvent{ChainID: chainID, RolledBackBlockHashes: rolledBack}); err != nil {
		return fmt.Errorf("reorg 이벤트 발행 실패: %w", err)
	}

	if err := q.DeleteBlocksFromHeight(ctx, state.DeleteBlocksFromHeightParams{ChainID: chainID, Height: ancestor + 1}); err != nil {
		return fmt.Errorf("orphan 블록 삭제 실패: %w", err)
	}
	if err := q.UpsertCursor(ctx, state.UpsertCursorParams{ChainID: chainID, Height: ancestor}); err != nil {
		return fmt.Errorf("커서 롤백 실패: %w", err)
	}
	return nil
}

// backoff는 노드/네트워크 오류 재시도 간격을 지수적으로 늘린다(FR-21). "새 블록 대기" 같은
// 정상 폴링에는 쓰지 않는다 — 실제 오류 상황에서만 next()를 호출하고, 정상 처리 시 reset().
type backoff struct {
	cur, max time.Duration
}

func newBackoff() *backoff {
	return &backoff{cur: time.Second, max: 60 * time.Second}
}

func (b *backoff) next() time.Duration {
	d := b.cur
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return d
}

func (b *backoff) reset() {
	b.cur = time.Second
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
