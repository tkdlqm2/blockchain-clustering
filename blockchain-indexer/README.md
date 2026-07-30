# blockchain-indexer

블록체인 노드에서 블록을 순차 인덱싱하여 BalanceDelta를 추출하고, reorg를 감지하며, Kafka(topic `balance-deltas`)로 발행하는 생산자. 소비자는 클러스터링 시스템(별도 프로젝트, 이미 완성됨).

설계 스펙 전체는 [`docs/`](./docs) 참고. 서비스 개요·도메인·계약 준수 규칙은 [`CLAUDE.md`](./CLAUDE.md) 참고.

## 기술스택

- Go / PostgreSQL(sqlc) / Kafka / Prometheus

## 로컬 실행

### 1. 환경변수 준비

`.env`가 이미 로컬 개발용 시크릿과 함께 생성되어 있다(git에 커밋되지 않음). 이 값은 앱 설정용이며, 인프라(compose) 자격증명은 아래 §2의 부모 디렉토리 `.env`가 별도로 담당한다.

```bash
cp .env.example .env
# ETH_MAINNET_RPC_URL, BTC_TESTNET_RPC_URL 등 서드파티 RPC 제공자 키를 채운다.
```

### 2. 인프라 기동 (Kafka + PostgreSQL) — 클러스터링 프로젝트와 공유

이 프로젝트만의 `docker-compose.yml`은 더 이상 없다 — 클러스터링 시스템(`../blockchain_cluster`)과 Kafka 호스트 포트(9092)가 겹쳐서 동시에 못 띄우는 문제가 있었고, 그래서 인프라(Postgres ×2 + Kafka + Kafka UI)를 부모 디렉토리(`../docker-compose.yml`, 즉 `/Users/dustin/Desktop/test/cluster/`)에서 하나로 통합했다. Postgres는 프로젝트별로 분리 유지(스키마가 다름), Kafka/Kafka UI만 공유.

```bash
cd ..                    # /Users/dustin/Desktop/test/cluster
docker compose up -d
docker compose ps        # cluster-postgres, indexer-postgres, kafka, kafka-ui 모두 healthy 확인
```

- Kafka UI: http://localhost:8080
- PostgreSQL(이 프로젝트용): `localhost:${POSTGRES_PORT}` (기본 5433 — 클러스터링 프로젝트의 Postgres가 이미 5432대를 쓰고 있어서 분리)

인프라 자격증명(`.env`)도 부모 디렉토리에 있다. 통합 과정에서 원래 이 프로젝트의 compose가 Kafka 볼륨을 마운트만 해두고 실제 `KAFKA_LOG_DIRS`는 지정하지 않아 데이터가 컨테이너 재생성 시 전부 유실되는 버그를 발견해 고쳤다(부모 compose 파일 주석 참고).

최초 기동 시 `migrations/0001_init.sql`(스키마), `migrations/0002_create_app_user.sh`(애플리케이션 전용 유저 생성), `migrations/0003_seed_chain_config.sql`(chain_config 시드)이 순서대로 자동 실행된다. chain_id·node_endpoint는 아직 placeholder이므로 실제 값 확정 후 `UPDATE chain_config ...`로 채워야 한다(`CLAUDE.md` 미해결 TODO 참고).

### 3. 쿼리 코드 생성 (sqlc)

```bash
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

### 4. 의존성 설치 & 실행

```bash
go mod tidy
CONFIG_PATH=configs/config.yaml go run ./cmd/indexer
```

`/metrics`는 기본 `:9100`에 노출된다(`METRICS_LISTEN_ADDR`).

### 5. 테스트

```bash
go test ./...
```

`internal/adapter/ethereum`, `internal/reorg`는 네트워크·DB 없이 합성 픽스처로 도는 단위 테스트다.

## 상태

Ethereum 경로는 실제 mainnet 블록으로 라이브 검증 완료(native/ERC-20 추출, reorg 롤백, 지수 백오프 재시도). **Bitcoin(UTXO) 어댑터는 의도적으로 미구현**이다 — 현재 스코프가 Ethereum 우선이라 `internal/adapter/bitcoin`은 스텁 상태로 남겨뒀다(`chain_config`의 `bitcoin-testnet` 행도 `enabled=false`).

| 항목 | 상태 |
|---|---|
| Ethereum: NodeClient·메인 루프(M1) | ✅ |
| Ethereum: native + ERC-20 추출(M2) | ✅ 단위 테스트 + mainnet 라이브 검증 |
| Bitcoin(UTXO) 어댑터(M3) | ⏸️ 의도적으로 보류 (Ethereum 우선) |
| reorg 감지·롤백·재발행(M4) | ✅ 단위 테스트 완료, 실제 reorg로는 미검증(발생 대기 중) |
| meta 컨벤션(M5) | ✅ (Ethereum 한정) |
| 백필(start_height)·지수 백오프(M6) | ✅ 기본 구현. Prometheus 지표는 배선됐으나 대시보드·알림은 없음 |
| 자동화 테스트(`docs/05` 테스트 매트릭스) | 부분 — Ethereum 관련 단위 테스트만(`internal/adapter/ethereum`, `internal/reorg`). UTXO 계열(T-4,5,6)과 실 reorg 시나리오(T-13,14 라이브)는 없음 |

구현 순서는 `docs/05-implementation-plan-and-tests.md`의 마일스톤(M0~M7)을 따른다.

## 알려진 한계

- 블록당 트랜잭션마다 `eth_getTransactionReceipt`를 개별 호출 — 트랜잭션이 많은 블록/대량 백필에서 느릴 수 있다(배치 조회로 최적화 여지 있음).
- Kafka 발행(`PublishDeltas`)과 DB 상태 저장(`persistBlockAndCursor`)은 각각은 원자적이지만 둘 사이의 전역 원자성은 없다 — 대신 재처리가 멱등하도록 설계되어 있다(`InsertBlock`은 `ON CONFLICT DO NOTHING`).

## 미해결 TODO

- `docs/04-architecture-and-interfaces.md` 미수령
- 클러스터링 팀 연동 계약 원본(문서 08) 미수령 — `chain_id` 값(`ethereum`) 확정 필요
- Bitcoin(UTXO) 어댑터(M3) — 착수 안 함
- 운영 배포 대상·시크릿 관리 방식 미정

자세한 내용은 [`CLAUDE.md`](./CLAUDE.md)의 "미해결 TODO" 참고.
