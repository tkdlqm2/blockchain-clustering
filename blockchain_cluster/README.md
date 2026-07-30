# blockchain_cluster

온체인 주소를 같은 통제 주체(엔티티)로 되돌릴 수 있게 클러스터링하는 백엔드 시스템. 인덱서가 생산한 BalanceDelta를 소비해 공통 입력/sweep/잔돈/계정 휴리스틱으로 병합 근거를 쌓고, 그 근거를 재생해 클러스터 멤버십을 산출한다. reorg·오탐 정정은 근거 무효화 + 재생으로 되돌린다.

전체 스펙은 [`docs/`](./docs)에 있다. 설계 원칙·도메인 배경·구현 시 지켜야 할 불변식은 [`CLAUDE.md`](./CLAUDE.md) 참고.

## 기술 스택

- Go + [chi](https://github.com/go-chi/chi) (REST) + [pgx/v5](https://github.com/jackc/pgx) + [sqlc](https://sqlc.dev/)
- PostgreSQL 16 (스키마 `clustering`, `chain_id` 파티셔닝)
- Kafka (KRaft 모드, BalanceDelta + reorg 이벤트)

## 로컬 실행

### 0. 인프라는 상위 디렉토리에서 공유한다

이 프로젝트만의 `docker-compose.yml`은 더 이상 없다 — `blockchain-indexer`(별도 프로젝트, 인덱서)와 Kafka를 공유해야 해서, 인프라(Postgres ×2 + Kafka + Kafka UI)는 부모 디렉토리(`../docker-compose.yml`, 즉 `/Users/dustin/Desktop/test/cluster/`)에서 하나로 관리한다.

```bash
cd ..                    # /Users/dustin/Desktop/test/cluster
docker compose up -d
docker compose ps        # cluster-postgres, indexer-postgres, kafka, kafka-ui 모두 healthy 확인
```

`.env`도 그 디렉토리에 있다(이 프로젝트의 `.env`는 앱 설정용으로 그대로 남아있고, 인프라 자격증명은 부모의 `.env`가 담당).

최초 기동 시 `migrations/0001_init.sql`(스키마)과 `migrations/0002_create_app_user.sh`(최소권한 앱 유저 생성)이 자동 실행된다.

체인을 최소 하나 등록해야 파티션이 생기고 데이터를 넣을 수 있다. `model_type`에 따라 어떤 휴리스틱이 자동으로 켜지는지가 갈린다(`utxo` → common-input/change, `account` → funding/deployer/behavioral, `both`인 sweep-seed/manual은 둘 다):

```bash
docker exec cluster-cluster-postgres-1 psql -U postgres -d clustering_db \
  -c "SELECT clustering.add_chain('bitcoin', 'Bitcoin', 'utxo', 'BTC', 8, 6, 'bitcoin');"

docker exec cluster-cluster-postgres-1 psql -U postgres -d clustering_db \
  -c "SELECT clustering.add_chain('ethereum', 'Ethereum', 'account', 'ETH', 18, 12, 'evm-lowercase');"
```

### 1. 앱 실행

```bash
go run ./cmd/server      # API + /metrics + 라벨 유지보수 스케줄러
go run ./cmd/consumer    # Kafka 컨슈머 (별도 프로세스)
curl localhost:8080/healthz
```

`.env`는 이미 로컬 개발용 실제 시크릿으로 채워져 있다(git에는 커밋되지 않음, `.env.example` 참고).

`cmd/consumer`는 `KAFKA_TOPIC_BALANCE_DELTA` topic을 구독해서 `balance_delta`/`reorg` 이벤트를 받아 `internal/pipeline.Pipeline.Run()` / `reorg.Handler.OnReorg()`를 호출한다(`docs/08-indexer-contract.md` 계약). Kafka는 이제 인덱서와 완전히 공유(포트 충돌 없음, 위 §0 참고).

### API 사용 예시 (QueryService)

인증은 아직 없다(내부 네트워크 신뢰 전제, `CLAUDE.md` TODO 참고). 모든 응답은 confidence를 동반한다.

```bash
curl "localhost:8080/v1/chains/bitcoin/addresses/<address>/cluster?min_confidence=0.5"
curl "localhost:8080/v1/chains/bitcoin/clusters/<cluster_id>/members?limit=50"
curl "localhost:8080/v1/chains/bitcoin/same-cluster?a=<addr1>&b=<addr2>"
curl "localhost:8080/v1/chains/bitcoin/evidence?address_a=<addr1>&address_b=<addr2>"
curl "localhost:8080/v1/chains/bitcoin/clusters/<cluster_id>/labels"
curl "localhost:8080/v1/chains/bitcoin/addresses/<address>/hub-status"
```

### 관측성

```bash
curl localhost:8080/metrics | grep ^clustering_
```

`clustering_cluster_max_size`(supernode 감시), `clustering_merge_evidence_active`(병합률), `clustering_merge_evidence_invalidated`(reorg/수동정정 빈도), `clustering_label_status_total`(라벨 신선도)을 체인별로 노출한다(NFR-6). 라벨 유지보수(신선도 감쇠·충돌 탐지)는 `.env`의 `LABEL_MAINTENANCE_*` 주기로 백그라운드에서 자동 실행된다.

### 2. 종료

```bash
cd ..
docker compose down        # 볼륨 유지 (인덱서도 같이 내려감 — 공유 인프라)
docker compose down -v     # 볼륨까지 삭제(데이터 초기화, 양쪽 프로젝트 전부)
```

PostgreSQL은 호스트 포트 `55432`로 노출된다(기본 `5432`가 아님 — 로컬에 이미 다른 Postgres가 떠 있는 경우와 충돌을 피하려고 옮겨뒀다. 필요하면 부모 디렉토리 `.env`의 `DB_PORT`만 바꾸면 된다).

## 테스트

```bash
go test ./...                                   # 순수 로직 단위 테스트 (DB 불필요)
(cd .. && docker compose up -d)                 # 통합 테스트는 라이브 DB/Kafka가 필요 (공유 인프라)
go test -tags=integration ./...                 # EvidenceStore/ClusterStore/Ingestor/Consumer 등 연동 테스트
```

## 디렉토리 구조

```
cmd/server/               API 서버 (QueryService + /metrics + 라벨 유지보수)
cmd/consumer/             Kafka 컨슈머 (인덱서로부터 수신 → 파이프라인 실행)
internal/config/          YAML+환경변수 config 로더
internal/domain/          공용 도메인 타입 (docs/02 매핑)
internal/unionfind/       경로압축+rank Union-Find
internal/evidence/        EvidenceStore (merge_evidence, append-only)
internal/cluster/         Replay(순수 재생 함수) + ClusterStore
internal/ingestor/        Ingestor (BalanceDelta 멱등 적재 + 파생 함수)
internal/heuristic/       HeuristicEngines 플러그인 인터페이스 + CommonInput/Sweep/Change/Funding/Deployer/Behavioral 엔진
internal/merge/           MergeEngine (근거 append + 허브 재확인)
internal/registry/        chain_heuristic/heuristic 신뢰도 + chain.config 전처리 파라미터 조회 (하드코딩 금지)
internal/preprocessor/    markHubs/markCollaborativeTx/markDust + excluded_tx 저장소
internal/sweeptarget/     sweep_target 저장소 (집금 목적지 앵커)
internal/reorg/           ReorgHandler (근거 무효화 + 재생, 되돌림)
internal/audit/           audit_log 기록 (감사 추적, FR-26)
internal/queryservice/    QueryService REST API (/v1/chains/{chain}/...)
internal/pipeline/        전체 파이프라인 단일 진입점 (적재→전처리→휴리스틱→병합→재생 순서 보장)
internal/consumer/        Kafka 컨슈머 (블록 단위 배칭, 처리 성공 후에만 오프셋 커밋)
internal/metrics/         Prometheus /metrics 컬렉터 (NFR-6)
internal/address/         주소 레지스트리 스켈레톤
internal/label/           LabelStore (CRUD + Maintain: 신선도 감쇠, 충돌 탐지)
internal/pgnumeric/       NUMERIC(78,0) ↔ *big.Int 변환 (uint256 무손실)
internal/integrationtest/ `integration` 태그 테스트용 라이브 DB 커넥션 헬퍼
internal/store/           sqlc 생성 코드 자리 (쿼리는 queries/에 추가, 아직 비어있음)
migrations/                Postgres 물리 스키마 (docs/07의 실행 사본)
configs/                   config.yaml
docs/                      스펙 패키지 (00~07) + 인덱서 연동 계약 (08)
```

## 진행 상황

`docs/05-implementation-plan-and-tests.md`의 마일스톤 기준. 자세한 내용과 라이브 검증 중 발견한 이슈는 [`CLAUDE.md`](./CLAUDE.md) 참고.

- [x] M0 — EvidenceStore, ClusterStore, Union-Find, 재생 로직
- [x] M1 — Ingestor (멱등 적재, tx/block 조회, 파생 함수)
- [x] M2 — MergeEngine + common-input 휴리스틱
- [x] M3 — Preprocessor (markHubs/markCollaborativeTx/markDust)
- [x] M4 — sweep + 잔돈 휴리스틱
- [x] M5 — reorg·정정 (되돌림)
- [x] M6 — 신뢰도 결합, threshold 뷰, 라벨 유지보수, QueryService (REST API 최초 등장)
- [x] M7 — 계정 체인 트랙 (funding/deployer/behavioral)
- [x] M8 — 관측성·운영 (지표, 라벨 유지보수 스케줄러, 파라미터 외부화)

`docs/05` §7 "구현 완료 선언 조건" 최종 점검 결과: AC-1~AC-8·T-1~T-14 전부 충족, 파라미터 외부화·지표·감사 로그 전부 동작. 다만 §4의 precision 기준선 확보는 실제 ground-truth 데이터가 있어야 하는 운영 단계 작업이라 아직 미충족 — 자세한 내용은 [`CLAUDE.md`](./CLAUDE.md) 참고.

## 남은 작업 (TODO)

- [x] Kafka 컨슈머 (`internal/consumer`, `cmd/consumer`) — 실제 공유 Kafka로 라이브 검증 완료
- [ ] 인덱서 `docs/06-contract-decisions.md`의 협의 항목 최종 확정 — `meta.token_contract`(우리 쪽에서 아직 안 씀), `unparsed:` 주소 필터링(Bitcoin 재개 시), Protobuf 전환 시점, 운영 Kafka 인증
- [ ] `finality_depth` 값 정합 — 우리 `chain` 테이블의 `ethereum` 행을 인덱서 값(64)에 맞출지 검토
- [ ] QueryService 인증(API Key/mTLS) 적용
- [ ] CI 파이프라인
- [ ] 배포 대상 확정 후 시크릿 관리 방식 결정(Vault/SSM/K8s Secret) — 운영 Kafka는 SASL/TLS 전환 필요(로컬은 평문)
- [ ] 평가 방법론(§4) precision 기준선 — 실제 ground-truth 데이터 확보 후 측정
