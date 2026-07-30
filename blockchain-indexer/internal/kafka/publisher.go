// Package kafka는 계약이 고정한 발행 규칙을 강제한다 (docs/02 §A, docs/06 §4):
//   - topic은 하나(balance-deltas), BalanceDelta/ReorgEvent를 type 필드로 구분
//   - 파티션 키는 항상 chain_id (체인 내 순서 보존)
//   - amount는 domain.BalanceDelta에서 이미 string 타입이므로 여기서 별도 강제 불필요
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
)

type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.Hash{}, // 파티션 키(chain_id) 기반 해시 분배 (docs/06 §4)
			AllowAutoTopicCreation: false,
		},
	}
}

// PublishDeltas는 한 블록에서 추출된 모든 BalanceDelta를 원자적으로 발행한다.
// 이 호출이 성공한 뒤에만 호출자가 커서를 전진시켜야 한다 (docs/03 §0 beginPublish/commitPublish, FR-16).
func (p *Publisher) PublishDeltas(ctx context.Context, deltas []domain.BalanceDelta) error {
	msgs := make([]kafkago.Message, 0, len(deltas))
	for _, d := range deltas {
		d.Type = "balance_delta"
		payload, err := json.Marshal(d)
		if err != nil {
			return fmt.Errorf("delta 직렬화 실패: %w", err)
		}
		msgs = append(msgs, kafkago.Message{Key: []byte(d.ChainID), Value: payload})
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

// PublishReorg는 재발행보다 반드시 먼저 호출되어야 한다 (docs/02 §A.2, docs/06 §5).
func (p *Publisher) PublishReorg(ctx context.Context, event domain.ReorgEvent) error {
	event.Type = "reorg"
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("reorg 이벤트 직렬화 실패: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(event.ChainID), Value: payload})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
