# cluster

블록체인 인덱싱 파이프라인 모노레포. 두 개의 독립 Go 서비스로 구성되며, Kafka로 연결된다.

```
blockchain-indexer (producer) --topic: balance-deltas--> blockchain_cluster (consumer)
```

## 서비스

| 서비스 | 역할 | 상세 |
|---|---|---|
| [`blockchain-indexer/`](./blockchain-indexer) | 블록체인 노드에서 블록을 순차 인덱싱해 BalanceDelta를 추출하고, reorg를 감지해 Kafka(`balance-deltas`)로 발행하는 생산자 | [README](./blockchain-indexer/README.md) |
| [`blockchain_cluster/`](./blockchain_cluster) | BalanceDelta를 소비해 공통 입력/sweep/잔돈/계정 휴리스틱으로 병합 근거를 쌓고, 온체인 주소를 동일 통제 주체로 클러스터링하는 시스템 | [README](./blockchain_cluster/README.md) |

각 서비스의 상세 아키텍처·도메인 스펙·불변식은 서비스별 `docs/`, `CLAUDE.md`를 참고.

## 공유 인프라

두 서비스가 Kafka 호스트 포트(9092)를 공유하기 때문에 인프라는 이 디렉토리의 `docker-compose.yml` 하나로 통합 관리한다. Postgres는 서비스별 스키마가 달라 컨테이너를 분리 유지한다(`cluster-postgres`, `indexer-postgres`), Kafka/Kafka UI만 공유.

```bash
cp .env.example .env     # 값 채우기 (DB 계정, 포트 등)
docker compose up -d
docker compose ps        # cluster-postgres, indexer-postgres, kafka, kafka-ui 모두 healthy 확인
```

- Kafka UI: http://localhost:8080
- cluster-postgres: `localhost:${DB_PORT:-55432}`
- indexer-postgres: `localhost:${POSTGRES_PORT:-5433}`

## 서비스 실행

인프라 기동 후 각 서비스 디렉토리에서 개별적으로 실행한다. 각 서비스는 자체 `.env`(앱 설정용, 인프라 자격증명과 별개)가 필요하다.

```bash
# indexer
cd blockchain-indexer
cp .env.example .env     # RPC 엔드포인트 등 채우기
go run ./cmd/indexer

# cluster (별도 터미널)
cd blockchain_cluster
cp .env.example .env
go run ./cmd/server       # REST API
go run ./cmd/consumer     # Kafka consumer
```

세부 절차(체인 등록, sqlc 코드 생성, 마이그레이션 등)는 각 서비스의 README를 따른다.
