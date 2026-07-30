# blockchain_cluster — 온체인 주소 클러스터링 시스템

## 서비스 설명

인덱서가 생산한 BalanceDelta(자산 이동 원자 기록)를 입력으로, 여러 주소를 같은 통제 주체(엔티티)로 되돌릴 수 있게(reversible) 묶고, 각 병합에 근거와 신뢰도를 부여하며, 그 위에 라벨·조회를 제공하는 백엔드 시스템.

- **입력**: 상류 인덱서가 Kafka로 발행하는 BalanceDelta 스트림/배치. 이 시스템은 블록 인덱싱 자체를 하지 않는다 — 소비만 한다.
- **출력**: 엔티티(클러스터), 주소→엔티티 매핑, 병합 근거, 라벨. 리스크 스코어링/추적/수사 등 상위 응용은 이 시스템의 **소비자**이며 범위 밖이다.
- **비범위**: KYC/실명 매핑, 가격·회계 계산, 프라이버시 공격 도구화(개별 사용자 표적 해제 조장 금지).

## 전체 스펙은 `docs/`가 원본이다

이 프로젝트는 처음부터 상세 스펙 패키지(`docs/00`~`07`)를 갖고 시작했다. **구현 중 애매하면 코드가 아니라 이 문서들을 먼저 참조할 것.**

| 문서 | 내용 |
|---|---|
| `docs/00-README.md` | 패키지 개요, 핵심 설계 원칙 5가지, 용어집 |
| `docs/01-functional-spec.md` | FR/NFR, 서비스 표면(조회 API), 윤리·규제 제약 |
| `docs/02-data-model.md` | 논리 데이터 모델. **핵심 불변식**: merge_evidence가 진실의 원천, cluster/membership은 파생 캐시 |
| `docs/03-clustering-algorithms.md` | 휴리스틱별 의사코드 — 정확성이 결정되는 가장 중요한 문서 |
| `docs/04-architecture-and-interfaces.md` | 컴포넌트 9개(Ingestor~QueryService) 분해와 인터페이스 계약 |
| `docs/05-implementation-plan-and-tests.md` | M0~M8 마일스톤, AC-1~8 인수기준, T-1~15 테스트 매트릭스 — **구현 완료의 정의** |
| `docs/06-multichain-extensibility.md` | 새 체인 추가를 레지스트리+파티션으로 흡수하는 설계 |
| `docs/07-postgres-schema.sql` | 물리 스키마 원본. `migrations/0001_init.sql`은 이 파일의 사본 — 스키마를 고치려면 **둘 다** 갱신 |

## 절대 어기면 안 되는 것

1. **merge_evidence는 append-only.** 물리 UPDATE/DELETE 금지, `status` 전이(`active`→`invalidated`)만 허용. DB 권한 자체가 이를 강제한다 — 앱 유저(`clustering_app`)에게 DELETE 권한을 주지 않는다(`migrations/0002_create_app_user.sh`).
2. **전처리(Hub/CoinJoin/Dust 표시)는 휴리스틱 엔진보다 먼저 실행되어야 한다** (FR-7, AC-3). 순서 위반은 결함.
3. **모호하면 병합하지 않거나 낮은 confidence.** 오탐 1건이 정탐 누락 1건보다 비싸다 — precision을 recall보다 우선.
4. **모든 조회 응답은 confidence를 동반해야 한다** (단정 금지 — 윤리·규제 제약, `docs/01` §6).
5. **cluster/cluster_membership은 언제나 merge_evidence(active)로부터 재생 가능해야 한다.** 두 저장소가 불일치하면 재생값이 정답.
6. 새 체인 추가는 코어 스키마/코드 변경 없이 `SELECT add_chain(...)` + `chain_heuristic` 매핑 + 파티션 생성으로 끝나야 한다(`docs/06` §6 체크리스트).

## 기술 스택

- **언어**: Go (표준 `net/http` + `chi`). ORM 대신 `pgx/v5` + `sqlc`(설정: `sqlc.yaml`, 쿼리 파일은 `internal/store/queries/`에 추가하며 M0부터 채워나간다).
- **DB**: PostgreSQL 16 단일 저장소. 스키마 `clustering`, `chain_id` 기준 LIST 파티셔닝(`balance_delta`/`address`/`merge_evidence`/`cluster`/`cluster_membership`).
- **Union-Find**: 인메모리(경로압축+rank), 스캐폴딩 범위 밖 — M0에서 구현. 수억 원소 규모가 되면 외부화/샤딩 전략 재검토(`docs/05` 리스크 §6).
- **메시징**: Kafka(KRaft, Zookeeper 없음). BalanceDelta 수집과 reorg 통지를 **같은 topic**의 이벤트 타입으로 구분해서 소비한다(`KAFKA_TOPIC_BALANCE_DELTA`).
- **조회 API**: REST(JSON). `docs/01` §5의 논리 조회 능력(`cluster_of`/`members_of`/`same_cluster`/`evidence_of`/`labels_of`/`entity_flow`/`hub_status`)을 그대로 엔드포인트로 노출.

## 인프라: 부모 디렉토리에서 인덱서와 공유

이 프로젝트 자체의 `docker-compose.yml`은 없다 — Kafka를 인덱서 프로젝트(`../blockchain-indexer`)와 공유해야 해서, Postgres ×2(cluster/indexer 각자 스키마 분리) + Kafka + Kafka UI를 `../docker-compose.yml`(`/Users/dustin/Desktop/test/cluster/`) 하나로 통합했다. 인프라 자격증명도 그 디렉토리의 `.env`에 있다(이 프로젝트의 `.env`는 앱 설정용으로만 그대로 쓰인다). 서비스명이 바뀌었다: `postgres` → `cluster-postgres`(컨테이너명 `cluster-cluster-postgres-1`), Kafka는 `kafka` 그대로.

**통합하면서 실제 버그 하나를 발견해서 고쳤다**: 인덱서의 원래 `docker-compose.yml`이 Kafka 볼륨을 `/var/lib/kafka/data`에 마운트해뒀는데, `apache/kafka` 이미지는 `KAFKA_LOG_DIRS`를 명시하지 않으면 실제로는 `/tmp/kafka-logs`에 쓴다 — 즉 그 볼륨은 처음부터 한 번도 실제 데이터를 받은 적이 없었고, 컨테이너를 내리는 순간(`docker compose down`, `-v` 없이도) 토픽이 통째로 날아가는 상태였다. 통합 compose에 `KAFKA_LOG_DIRS: /var/lib/kafka/data`를 명시해서 고쳤고, 컨테이너 재시작으로 토픽이 실제로 살아남는 것까지 확인했다.

## 외부 통신

| 방향 | 대상 | 프로토콜 | 명세 상태 |
|---|---|---|---|
| Inbound | 상류 인덱서 | Kafka (`KAFKA_TOPIC_BALANCE_DELTA`), `internal/consumer` + `cmd/consumer` | **컨슈머 구현 완료.** 인덱서는 별개 프로젝트(`docs/01` §1.2, 실제로 `blockchain-indexer/`에 존재). 계약은 **`docs/08-indexer-contract.md`**, 인덱서 쪽 `docs/06-contract-decisions.md`와 상호 확인 완료(amount 문자열 인코딩, `chain_id` 일치, `meta.token_contract`/`meta.contract_creation` 컨벤션). 실제 공유 Kafka로 라이브 검증됨(`internal/consumer/integration_test.go`). |
| Inbound | 상류 인덱서 (reorg) | 위와 동일 topic, 이벤트 타입으로 구분 | `docs/08-indexer-contract.md` §3.2. 자체 감지 폴백(통지 실패 시)은 여전히 미구현 — 인덱서가 권위 있는 감지자라는 전제(인덱서 `docs/06-contract-decisions.md` §5) |
| Outbound | 소비 서비스(리스크/추적/인텔리전스) | REST | 인증 미구현(내부 네트워크 신뢰 전제) — 실제 배포 전 API Key 또는 mTLS 적용 필요 |

## 인증/배포/관측성 — 아직 정해지지 않음 (TODO)

- **API 인증**: 없음. 로컬/프로토타입 단계 전제. 외부 노출 전 API Key 등 도입 필요.
- **배포 대상**: 미정. AWS/온프레미스 결정되면 config·시크릿 관리(현재 `.env` → Vault/SSM/K8s Secret 등)를 그에 맞게 교체.
- **관측성**: 구조화 로깅(`slog`, JSON)만 적용. `docs/01` NFR-6이 요구하는 지표(supernode 발생, 병합률, 정정율, 라벨 신선도)는 M8에서 Prometheus 연동과 함께 추가.
- **CI**: 아직 없음.

## 구현 순서

새 기능을 추가하기 전에 `docs/05-implementation-plan-and-tests.md`의 M0~M8 순서를 따를 것. 패키지 경계는 `docs/04-architecture-and-interfaces.md` §1의 컴포넌트 9개 분해를 그대로 따른다.

### 진행 상황

- **M0 완료**: `internal/domain`(공용 타입), `internal/unionfind`(경로압축+rank DSU), `internal/cluster`(`Replay` 순수 재생 함수 + Postgres `Store`), `internal/evidence`(EvidenceStore), `internal/address`·`internal/label`(레지스트리 스켈레톤).
  - `cluster.Replay()`는 DB 없이 순수 함수로 분리되어 있다 — AC-1(재생 결정성)을 `internal/cluster/replay_test.go`에서 실제 자동화 테스트로 검증한다(가짜 통과 아님, `go test ./...`로 확인 가능).
  - `ClusterStore.RebuildFromEvidence()`는 항상 **전체 삭제 후 재삽입**이다(부분 갱신 최적화 없음) — `docs/03` §9의 "정확성 기준 구현은 전체 재생" 원칙을 그대로 따름. 성능이 문제되면 이후 마일스톤에서 최적화하되 반드시 전체 재생과 동일 결과를 보장해야 한다.
  - Postgres 연동 코드(EvidenceStore/ClusterStore/AddressStore/LabelStore)는 라이브 DB 없이는 자동 테스트되지 않았다 — 실제 컨테이너로 검증 필요(테스트 대상 미포함, 정직하게 밝힘).
- **M1 완료**: `internal/ingestor` — `Store.Ingest()`(멱등 적재), `GetDeltasByTx`/`GetDeltasByBlock`, `GetCursor`/`SetCursor`(Kafka offset 등 수집 진행 위치, `ingest_cursor` 테이블), 순수 파생 함수 `GroupByTx`/`SpentAddresses`/`ReceivedEntries`(03문서 §0의 groupByTx/inputAddresses/outputEntries에 대응).
  - 멱등성(FR-3)은 `(chain_id, txid, delta_index)` PK에 대한 `ON CONFLICT DO NOTHING`으로 구현 — 재적재는 절대 UPDATE하지 않는다(같은 delta_index의 값이 달라지는 것 자체가 인덱서 버그이므로 덮어쓰지 않고 무시).
  - `amount`는 `NUMERIC(78,0)`(uint256 무손실 저장, `docs/06` §4) 결정을 그대로 따르기 위해 Go에서 `*big.Int`로 다룬다. `int64`/`float64`로 편하게 갔으면 안 됐던 지점 — 토큰 amount가 uint256 범위를 넘으면 조용히 깨진다.
  - `GroupByTx`/`SpentAddresses`/`ReceivedEntries`는 DB 없는 순수 함수로 분리해 자동 테스트했다(`internal/ingestor/derive_test.go`).
  - **Kafka 컨슈머 루프는 아직 안 만들었다.** 이 Store는 라이브러리 코드일 뿐 아직 아무도 호출하지 않는다 — BalanceDelta 메시지 envelope(특히 reorg 이벤트를 같은 topic에서 구분하는 포맷)이 여전히 TODO(자체 설계, 인덱서 미확정)라 지금 확정하면 나중에 다시 바꿔야 할 여지가 크다. envelope이 정해지면 `cmd/server`나 별도 컨슈머 바이너리에서 이 Store를 호출하는 코드를 추가하면 된다.
- **M2 완료**: `internal/heuristic`(HeuristicEngines 플러그인 인터페이스 + `CommonInputEngine`), `internal/merge`(MergeEngine), `internal/registry`(chain_heuristic/heuristic 신뢰도 조회 — 03문서 §10 "파라미터 하드코딩 금지"를 지키려고 `CONF_COMMON_INPUT`을 Go 상수가 아니라 DB에서 읽는다), `internal/preprocessor`(excluded_tx 읽기/쓰기 스켈레톤 — 실제 탐지 로직은 여전히 M3).
  - **MergeEngine은 자체 Union-Find를 안 갖는다.** 04문서 §2[4]는 MergeEngine 책임에 "Union-Find union"도 포함하지만, M0에서 이미 "정확성 기준 구현은 전체 재생"(03 §9)으로 정했기 때문에, MergeEngine이 별도로 점진적 union을 유지하면 `cluster.Replay()`가 계산하는 것과 다른 결과를 낼 위험이 있다. 그래서 MergeEngine은 근거 append(+ 허브 재확인)까지만 하고, 멤버십 반영은 배치 뒤에 `ClusterStore.RebuildFromEvidence()`를 호출하는 쪽 책임으로 뺐다.
  - `ingestor.GroupByTx()`가 원래 map을 리턴해서 순회 순서가 비결정적이었던 걸 M2 설계 중에 발견해서 고쳤다 — op_id 부여 순서가 canonical anchor(02 §6)를 결정하기 때문에, 같은 배치를 재처리했을 때 cluster_id가 흔들릴 수 있는 실제 버그였다. 지금은 입력 슬라이스의 첫 등장 순서를 보존하는 슬라이스를 리턴한다.
  - `CommonInputEngine`/`MergeEngine` 둘 다 의존성을 좁은 인터페이스(`HubChecker`/`ExclusionChecker`/`ConfidenceProvider`/`EvidenceAppender`)로 받아서, DB 없이 순수 로직을 테스트했다(`internal/heuristic/commoninput_test.go`, `internal/merge/engine_test.go`).
  - 라이브 DB로 M2 파이프라인 전체(Ingest → Generate → RecordAndMergeBatch → RebuildFromEvidence → 조회)를 `internal/merge/integration_test.go`에서 검증 완료.
  - 아직 없음: Preprocessor의 실제 탐지 로직(markHubs/markCollaborativeTx/markDust, M3), sweep/change 휴리스틱(M4), ReorgHandler(M5), confidence 결합·QueryService(M6), 계정 체인(M7), 관측성(M8).
- **M3 완료**: `internal/preprocessor`에 `MarkHubs`(`HubDetector`)/`MarkCollaborativeTx`/`MarkDust` 추가. 파라미터(HUB_THRESHOLD, DUST_THRESHOLD, EQUAL_OUTPUT_MIN 등)는 Go 상수가 아니라 `chain.config` JSONB에서 읽는다(`registry.PreprocessingParamsFor`, `domain.PreprocessingParams`) — 03문서 §10 "파라미터 하드코딩 금지"를 M2의 confidence 레지스트리와 같은 방식으로 지켰다.
  - **markHubs는 03문서 §1(b)의 3개 신호 중 counterparty degree 하나만 구현했다.** txRate는 BalanceDelta에 타임스탬프가 없어서(block_height뿐) 계산할 방법이 없고, sweepConvergence는 원래 M4(sweep 휴리스틱)의 `looksLikeSweep` 로직을 그대로 가져와야 정확한데 M4가 아직 없다. degree 하나만으로도 실제로 동작하는 보수적 신호이고, 나머지 둘은 의도적 보류로 코드 주석과 여기 모두에 남겨뒀다 — 조용히 빠뜨린 게 아니다.
  - **markCollaborativeTx는 `structureMatchesKnownImpl`(Wasabi/Whirlpool/JoinMarket 지문)을 구현하지 않았다.** 지갑 구현체별 트랜잭션 구조 지문 데이터베이스가 있어야 하는데 없다. 나머지 측정 가능한 3개 조건(동일 금액 output 그룹 크기, input 수, output 수)만으로 판정한다.
  - 세 함수 모두 순수 판정 로직(`DetectCollaborativeTx`, `DustInflows`, `hubScoreFromDegree`, `hubTypeFromLabels`)과 DB I/O(Store/HubDetector 메서드)를 분리해서, 판정 로직은 DB 없이 테스트했다(`internal/preprocessor/detect_test.go`).
  - 라이브 DB로 세 가지 흐름을 전부 검증: 허브로 판정된 주소가 공통입력 병합에서 제외됨, coinjoin으로 표시된 tx가 후보를 만들지 않음, dust로만 연결된 tx가 제외됨(`internal/preprocessor/integration_test.go`) — 이번에는 실제 버그가 없었다.
  - **실제 버그 하나 더 발견**: M1의 `Ingestor.Ingest()`가 `balance_delta`만 적재하고 `address` 레지스트리에 행을 만들지 않고 있었다. `address.Store.SetHub`/`SetDustFlag`는 UPDATE-only라서, 행이 없으면 조용히 아무 일도 안 일어나는 상태였다(에러도 안 남). M3에서 `SetHub`/`SetDustFlag`를 실제로 쓰면서 발견 — `Ingestor.NewStore`가 이제 `AddressUpserter`를 받아 배치의 모든 주소를 `Ingest()` 안에서 함께 upsert한다(`internal/ingestor/store.go`). 기존 호출부(`cmd`, 테스트)는 전부 갱신했다.
  - 아직 없음: sweep/change 휴리스틱(M4), ReorgHandler(M5), confidence 결합·QueryService(M6), 계정 체인(M7), 관측성(M8).
- **M4 완료**: `internal/sweeptarget`(sweep_target CRUD), `internal/heuristic`에 `SweepEngine`·`ChangeEngine` 추가.
  - `SweepEngine`은 03문서 §5의 두 조건을 그대로 구현했다: (a) 완결성 — 이 tx에서 쓴 금액이 그 주소가 지금까지 받은 누적 금액의 `completeness_min`(기본 0.9) 이상인가, (b) 반복성 — target이 서로 다른 `recurrence_min`(기본 2)개 이상의 주소로부터 받아본 적 있는가. 둘 다 실시간 SQL 집계(`cumulativeReceived`/`distinctSourcesToTarget`)로, 하드코딩 없이 `chain_heuristic.params`에서 두 임계치를 읽는다(`internal/heuristic/sweep.go`의 `sweepParams`).
  - **경계선을 코드로 강제**: 병합 후보는 항상 `(target, deposit_address)` 쌍만 만들고, 입금을 보낸 원천 주소는 애초에 `transfers` 순회에 등장하지 않는다 — M4 DoD를 `internal/heuristic/integration_test.go`의 `TestSweepEngine_DoD_...`에서 원천 주소가 어떤 클러스터에도 안 들어가는 것까지 직접 검증했다.
  - **expandFromSeeds의 hop 감쇠(하나의 시드에서 멀어질수록 confidence 낮추기)는 구현하지 않았다.** 지금 SweepEngine은 1-hop 직접 스윕만 탐지한다. 시드에서 여러 hop 떨어진 간접 연결은 시간이 지나며 Union-Find/재생으로 전이적으로 묶이긴 하지만, hop별로 confidence를 깎는 것은 confidence 결합(noisy-OR 등, 03 §7)과 같은 성격의 문제라 M6로 미뤘다 — 지금 만들면 M6에서 다시 만들어야 할 로직이었다.
  - `ChangeEngine`은 pseudocode의 약한 단서 4개 중 3개만 구현했다: 새 주소, 반올림 안 된 금액(1000 단위 나눗셈으로 근사), 즉시 재소비. **script type 동일 여부는 구현 안 함** — BalanceDelta에 스크립트 타입 필드가 없어서(native/token뿐) 비교할 데이터가 없다. 각 단서는 가중치 합산이고, 최종 confidence는 `min(레지스트리 confidence, 결합점수)`로 캡핑해서(03 §5b 그대로) 단독 확정을 막는다.
  - 파라미터 검증을 위해 registry의 `HeuristicConfig`/`ConfigFor`를 `registry` 패키지에서 `domain` 패키지로 옮겼다(`domain.HeuristicConfig`) — `heuristic` 패키지가 `registry`를 직접 import하지 않고 인터페이스(`ConfigProvider`)로만 의존하게 하기 위해서다. `sweepParams`/`changeParams`는 각 엔진이 자기 파라미터 모양을 스스로 해석한다(플러그인 원칙, 04문서 §2[3]) — registry/domain은 그 모양을 몰라도 된다.
  - `NUMERIC(78,0)` ↔ `*big.Int` 변환이 M1(ingestor)에 이어 SweepEngine에도 필요해져서, 중복 대신 `internal/pgnumeric`으로 뽑아 공유했다.
  - 라이브 DB로 M4 DoD 전체(입금주소들이 거래소 엔티티로 묶이되 원천 주소는 제외)와 미달 케이스(완결성 10%인 일반 결제는 스윕으로 안 잡힘)를 검증 완료 — 이번에도 새 버그는 없었다.
  - 아직 없음: ReorgHandler(M5), confidence 결합·QueryService(M6), 계정 체인(M7), 관측성(M8).
- **M5 완료**: `internal/reorg`(ReorgHandler: `OnReorg`/`OnManualCorrection`), `internal/audit`(FR-26 감사로그 기록).
  - 03문서 §9 그대로: 근거 무효화(`EvidenceStore.Invalidate`, 물리 삭제 아님) + `ClusterStore.RebuildFromEvidence()` 재생. `OnReorg`와 `OnManualCorrection`이 사실상 같은 두 단계(무효화 → 재생)라서 하나의 `Handler`가 둘 다 처리한다.
  - `EvidenceStore.Invalidate`가 이제 `(invalidated bool, error)`를 리턴하도록 바꿨다 — 이미 무효화됐거나 존재하지 않는 op을 다시 무효화 시도해도 에러가 아니라 "아무 변화 없음"으로 조용히 처리되고(멱등), **아무것도 안 바뀌었으면 `RebuildFromEvidence()`(전체 재생, 꽤 비쌀 수 있음)를 건너뛴다** — 이번엔 버그를 잡은 게 아니라, reorg 통지가 실제로 우리 근거를 안 건드리는 흔한 경우(reorg 대상 블록에 우리 병합 근거가 하나도 없는 경우)에 불필요한 재계산을 안 하기 위한 최적화다.
  - `OnReorg`/`OnManualCorrection` 둘 다 무효화 성공 시 `audit_log`에 자동으로 한 줄 남긴다(actor='system'/'operator', action='invalidate', target에 chain_id/op_id/block_hash). 라이브 DB로 실제 `audit_log` 행이 남는 것까지 확인했다(`docker compose exec postgres psql ... SELECT * FROM audit_log`).
  - 순수 로직(무효화 대상 필터링, 재생 트리거 여부 판단)은 fake 인터페이스로 DB 없이 테스트했다(`internal/reorg/handler_test.go`). M5 DoD(특정 block_hash 롤백 시 그 근거만 사라지고 나머지는 보존)는 라이브 DB로 별도 검증(`internal/reorg/integration_test.go`) — 이번에도 새 버그는 없었다.
  - **폴백 감지(인덱서가 reorg를 통지 못했을 때 자체적으로 block_hash 대조)는 구현하지 않았다** — 이건 "저장된 최신 체인 상태"와 비교할 외부 기준(인덱서 API나 노드 RPC)이 있어야 하는데, 그런 연동 자체가 아직 없다(Kafka 컨슈머와 마찬가지로 TODO).
  - 아직 없음: confidence 결합·QueryService(M6), 계정 체인(M7), 관측성(M8).
- **M6 완료**: confidence 결합, threshold 뷰, `LabelStore.Maintain`, `internal/queryservice`(REST API) — 이번 마일스톤에서 처음으로 실제 HTTP API가 생겼다(`cmd/server`에 마운트).
  - **`cluster.Replay()`를 03문서 §7 그대로 다시 짰다.** 이제 두 층으로 confidence를 결합한다: (1) 같은 주소쌍에 대한 여러 근거 중 **다른 source_txid**(독립 근거)는 noisy-OR(`combined = 1 - Π(1-conf_i)`)로 상향 결합하고, **같은 source_txid**(사실상 같은 관측 재기록)는 곱하지 않고 max만 취한다 — 03 §7의 "근거들이 사실상 같은 원천이면 독립이 아님" 경고를 그대로 코드화. (2) 앵커까지의 경로는 여전히 M0의 보수적 최소값(widest-path) 방식.
  - **threshold 뷰(`min_confidence`)를 "저장된 값 필터링"에서 "실시간 재구성"으로 바꿨다.** M0/M2~M5 내내 `ClusterOf`/`MembersOf`는 미리 계산된 `cluster_membership.membership_confidence` 컬럼을 필터링하는 방식이었는데, 이건 근사값일 뿐 진짜 맞는 방식이 아니다 — 예를 들어 강하게 뭉친 두 덩어리를 약한 다리(confidence 0.2) 하나가 잇고 있으면, `min_confidence=0.5`로 조회했을 때 "두 덩어리로 쪼개져야" 정답인데, 저장된 컬럼만 필터링하면 그 쪼개짐 자체를 표현할 수 없다(경로상 낮은 값 때문에 반대편 덩어리 전체가 통째로 빠지거나 잘못 남는다). 그래서 `min_confidence > 0`이면 이제 해당 체인의 active 근거를 confidence로 필터링한 뒤 `Replay()`를 그 자리에서 다시 돌린다(`ClusterStore.thresholdView`) — `min_confidence <= 0`(전체 보기)만 기존처럼 저장된 테이블을 빠르게 읽는다. 라이브 DB로 "약한 다리로 이어진 두 덩어리가 임계치를 넘기면 실제로 쪼개지는지"까지 검증했다(`TestClusterOf_ThresholdViewSplitsWeaklyBridgedClusters`).
  - `LabelStore.Maintain(chainID, staleTTL, decayFactor)`: `last_verified_at`이 `staleTTL`을 넘긴 active 라벨은 confidence를 감쇠시키고 `status='stale'`로(FR-20 — 별도 "reverify queue" 테이블이 스키마에 없어서, `status='stale'`인 라벨 자체가 곧 재검증 대기열 역할을 한다). 같은 target에 서로 다른 category 라벨이 active/stale로 공존하면 전부 `conflicted`로 전환(FR-21) — 자동 확정 안 함. 순수 충돌 탐지 로직(`DetectConflicts`)은 DB 없이 테스트, 전체 흐름은 라이브 DB로 검증.
  - **QueryService**: `internal/queryservice`, chi 라우터로 `/v1/chains/{chain}/...` 아래 01문서 §5의 조회 능력을 노출 — `cluster_of`/`members_of`/`same_cluster`/`evidence_of`(cluster_id 또는 address_pair)/`labels_of`(cluster 또는 address)/`hub_status`. 모든 응답에 confidence를 동반(AC-7). `evidence_of(cluster_id=...)`는 `ClusterStore.MembersOf` → `EvidenceStore.ByAddresses`로 조합해서, EvidenceStore가 ClusterStore에 직접 의존하지 않게 했다(04문서 §2[5] 각주 그대로).
  - **entity_flow는 구현하지 않았다** — 주소 단위 흐름을 엔티티 단위로 접는 건 완전히 다른 집계 로직이 필요하고, 01문서 §5 자체가 "이 흐름의 응용(수사·리스크)은 소비자의 몫"이라고 선을 그어둬서, 지금 만드는 것보다 실제 소비자가 나타났을 때 요구사항에 맞춰 만드는 게 낫다고 판단했다.
  - 라이브 DB + 실제 HTTP 서버(`cmd/server` 바이너리 직접 실행)로 전체 API 표면을 검증했다(`internal/queryservice/integration_test.go`) — 이번에도 새 버그는 없었다.
  - 아직 없음: 계정 체인 트랙(M7), 관측성·감사 파라미터 외부화(M8).
- **M7 완료**: `internal/heuristic/account.go` — `FundingEngine`/`DeployerEngine`/`BehavioralEngine`. `ethereum`을 `model_type='account'`로 등록하면 `add_chain()`이 이 셋을 자동으로 켠다(`common-input`/`change`는 반대로 자동으로 안 켜짐 — `applies_to='utxo'`).
  - **funding/deployer는 사실상 같은 감지 로직을 공유한다**: "이번 tx에서 처음 관측된 주소" + "그 tx의 첫 지출 주소(funder)". 둘의 차이는 오직 그 수신 delta에 `contract_creation` 플래그가 있는지뿐 — 있으면 deployer(신뢰도 0.85), 없으면 funding(신뢰도 0.6).
  - **`contract_creation` 플래그는 잠정 컨벤션이다.** BalanceDelta에 "이 tx가 컨트랙트 배포다"를 나타내는 필드가 원래 없어서(02문서 §1엔 native/token kind뿐), `meta` JSONB에 `{"contract_creation": true}`를 넣어주는 걸로 임시 약속해뒀다 — 실제 인덱서 계약이 아직 없다는 점(TODO)과 짝을 이루는 잠정 결정. 진짜 인덱서 연동 시 이 컨벤션 자체를 다시 확인해야 한다.
  - **behavioral은 03 §5c의 "인기 컨트랙트와 상호작용했다는 것만으로 묶지 말 것" 경고를 구조적으로 지킨다** — "둘 다 컨트랙트 C와 거래했다"가 아니라 "두 주소가 서로 직접, 반복적으로(기본 3회 이상) 거래했다"만 근거로 삼는다. 그래서 애초에 "인기 컨트랙트 하나에 다수가 연결" 패턴을 만들 수 없는 구조다.
  - **계정 체인 허브 처리는 새로 만들 게 없었다** — M3의 `HubDetector.MarkHubs`(counterparty degree 기반)가 체인 모델과 무관하게 이미 동작해서, 인기 dApp도 UTXO 허브와 같은 경로로 잡힌다. funding/behavioral 둘 다 기존 `IsHub` 체크를 그대로 재사용.
  - 라이브 DB로 M7 DoD(funding/deployer 병합 형성 + 인기 컨트랙트가 10개 주소와 거래해도 허브 판정 후 supernode 미형성)를 검증했다(`internal/heuristic/account_integration_test.go`) — 이번에도 새 버그는 없었다.
  - 아직 없음: 관측성·감사 파라미터 외부화(M8).
- **M8 완료**: `internal/metrics`(Prometheus `/metrics`), `LabelStore.Maintain` 주기 실행 스케줄러(`cmd/server`), `internal/pipeline`(전체 파이프라인 단일 진입점 — 아래 참고).
  - **메트릭은 이벤트 카운터가 아니라 스크랩 시점 실시간 쿼리다.** `MergeEngine`/`ReorgHandler`/`LabelStore` 생성자를 전부 건드려서 메트릭 레코더를 주입하는 대신, `/metrics`가 호출될 때마다 `registry.Store.ListChains()`로 등록된 체인을 찾아 그때그때 `cluster`/`merge_evidence`/`label` 테이블을 직접 집계한다(`clustering_cluster_max_size`, `clustering_merge_evidence_active/invalidated`, `clustering_label_status_total`). NFR-6이 요구하는 지표(supernode 발생, 병합률, 정정 빈도, 라벨 신선도)가 전부 "지금 DB가 뭐라고 말하는가"이므로, 굳이 인메모리 카운터를 여러 컴포넌트에 흩뿌리는 것보다 이 방식이 더 정확하고 훨씬 덜 침습적이었다.
  - **`LabelStore.Maintain`의 파라미터(주기·staleTTL·decayFactor)를 마지막으로 설정 가능하게 만들었다** — `configs/config.yaml`의 `label_maintenance` 섹션 + `.env`의 `LABEL_MAINTENANCE_*`. `cmd/server`가 이제 백그라운드 goroutine으로 이 주기마다 등록된 모든 체인에 대해 `Maintain`을 실제로 호출한다 — 이전까지는 호출자가 알아서 값을 넘겨야 한다고만 해뒀지 실제로 부르는 코드가 없었다.
  - **`internal/pipeline`을 새로 만들었다 — 이건 M8 항목이 아니라 05문서 §7 "구현 완료 선언 조건"을 마지막에 점검하다가 발견한 실제 구멍을 메운 것이다.** AC-3("전처리가 휴리스틱보다 먼저 실행됨이 코드/테스트로 보장된다")를 다시 보니, M0~M7 내내 Preprocessor·6개 HeuristicEngine·MergeEngine·ClusterStore를 각각 만들고 개별적으로 테스트했을 뿐, 이걸 올바른 순서로 묶어 실행하는 단일 함수가 없었다 — 순서를 지키는지는 "호출하는 쪽이 알아서 잘 부르면" 그렇다 수준이었다. `pipeline.Pipeline.Run()`이 이제 그 유일한 진입점이다: `[A] markHubs→markCollaborativeTx→markDust` → `[B] 등록된 모든 HeuristicEngine.Generate()` → `[C] MergeEngine.RecordAndMergeBatch` → `[D] ClusterStore.RebuildFromEvidence`(03문서 §0 그대로). 순서 자체를 fake로 코드 순서를 기록해서 단위 테스트로 직접 증명했고(`internal/pipeline/pipeline_test.go`), 실제 스토어를 전부 엮어서 라이브 DB로도 검증했다(`internal/pipeline/integration_test.go`) — 향후 Kafka 컨슈머가 생기면 이 `Run()`을 그대로 호출하면 된다.
  - 라이브 DB + 실제 서버 바이너리로 `/metrics`가 실제 누적 데이터(이전 마일스톤들의 테스트 결과가 그대로 쌓여있는 상태)를 정확히 보여주는 것까지 확인했다.
- **05문서 §7 "구현 완료 선언 조건" 최종 점검**:
  1. M0~M7 DoD 전부 충족 (`ethereum` 등록해서 M7도 대상에 포함) ✅
  2. AC-1~AC-8: **전부 충족** (AC-3는 이번에 `internal/pipeline`으로 마지막에 메웠다)
  3. 테스트 매트릭스 T-1~T-14: 전부 이름은 다르지만 대응하는 자동화 테스트 존재 ✅
  4. **평가 방법론(§4) precision 기준선 확보·회귀 감시 가동: 미충족.** 이건 진짜 ground-truth 라벨 데이터(known-deposit 실험 결과, 공개 신뢰 데이터셋)가 있어야 측정 가능한데, 그런 실データ 자체가 없다 — 합성 테스트 픽스처로는 "품질 기준선"을 대신할 수 없다. 실제 운영 데이터가 쌓이기 전까지는 원천적으로 채울 수 없는 항목이라고 판단했다.
  5. 파라미터 외부화·관측성 지표·감사 로그: 이번에 완료 ✅
  - **결론**: 코드/테스트로 증명 가능한 부분(1,2,3,5)은 전부 완료. 4번(품질 기준선)만 실제 데이터가 있어야 하는 운영 단계의 일이라 지금 상태로는 "구현 완료"이지 "검증된 프로덕션 품질"까지는 아니다.

### Kafka 컨슈머 구현 (`internal/consumer`, `cmd/consumer`) — 마지막 남은 TODO 해소

인덱서 프로젝트(`/Users/dustin/Desktop/test/cluster/blockchain-indexer`, 별도 리포)가 실제로 만들어지고 `docs/08-indexer-contract.md` §7의 열린 질문 대부분에 답을 내놓으면서(인덱서 쪽 `docs/06-contract-decisions.md`), 마지막까지 남아있던 "파이프라인은 완성됐지만 그걸 호출하는 상시 프로세스가 없다"는 구멍을 메웠다.

- **배치 단위 = 블록.** 인덱서가 "블록 단위로 원자적 발행"을 확정했으므로(인덱서 `docs/06` §6), `internal/consumer.Batcher`가 `block_hash`가 바뀌는 시점(또는 reorg 이벤트, 또는 주기적 안전망 타이머)에 그때까지 쌓인 델타를 하나의 배치로 `pipeline.Run()`에 넘긴다.
- **Kafka 오프셋 커밋 순서가 핵심이다.** 배치가 `pipeline.Run()`을 통해 실제로 DB에 반영된 **뒤에만** 그 배치를 구성한 메시지들의 오프셋을 커밋한다. 먼저 커밋하고 나중에 처리하면, 처리 전에 크래시 났을 때 그 델타가 영원히 유실된다(Kafka는 이미 커밋된 오프셋을 재전달하지 않음) — `Batcher`가 델타와 원본 `kafka.Message`를 함께 들고 있다가 flush 시점에 세트로 커밋하는 이유가 이거다.
- **`Pipeline.Run()` 자체에 진짜 버그를 하나 발견해서 고쳤다.** 컨슈머를 배선하다 보니, `Run()`이 `Ingestor.Ingest()`를 호출하지 않고 있었다 — 지금까지 모든 테스트가 `Run()` 호출 전에 수동으로 `Ingest()`를 먼저 불러줬기 때문에 안 드러난 문제였다. 실제 컨슈머는 그 관례를 몰랐다면 델타를 DB에 한 번도 안 적재한 채 전처리·휴리스틱을 돌려서 항상 빈 결과만 냈을 것이다. `Pipeline`에 `Ingester` 의존성을 추가해서 `Run()`의 진짜 첫 단계로 만들었다(`pipeline.go` 상단 주석에 이 배경을 남겨둠).
- **poison message(파싱 불가) 처리**: 로그만 남기고 커밋은 하고 넘어간다 — 안 그러면 메시지 하나가 파티션 전체를 영원히 막는다. 별도 dead-letter topic은 없음(TODO).
- 순수 로직(`Batcher`, `ParseMessage`)은 Kafka 없이 단위 테스트, 디스패치/커밋 로직(`Consumer.handle`)은 fake reader/pipeline/reorg로 테스트, **그리고 인덱서 프로젝트가 이미 띄워둔 실제 Kafka 인스턴스(`localhost:9092`, topic `balance-deltas`)에 진짜 메시지를 발행해서 종단간 검증까지 완료**(`internal/consumer/integration_test.go`) — 로컬 개발 환경에서 이 프로젝트와 인덱서 프로젝트의 Kafka 호스트 포트(9092)가 겹치므로, 인덱서 쪽 Kafka를 공용으로 썼다(둘 다 동시에 못 띄움, `.env` 주석 참고).
- `cmd/consumer`는 `cmd/server`(API+스케줄러)와 별도 바이너리다 — 컨슈머는 장기 실행 워커로 배포/재시작 특성이 다르다는 판단.

### 라이브 검증 완료 (M0~M6)

`docker compose up`으로 실제 Postgres/Kafka에 붙여서 `go test -tags=integration ./...`로 검증했다(각 패키지의 `integration_test.go` — 기본 `go test ./...`에는 안 잡히고 `integration` 빌드 태그로만 실행됨, DB 필요). M0+M1 검증 과정에서 스캐폴딩 단계에서는 안 보이던 실제 버그 3개를 잡아서 고쳤다:

1. **`add_chain()`/`add_chain_partitions()` 함수가 자기 `search_path`를 안 고정하고 있었다.** 초기화 스크립트 세션에서는 `SET search_path = clustering, public;`이 걸려 있어 통과했지만, 새 접속(운영자가 나중에 `SELECT add_chain(...)`을 호출하는 상황)에서는 `relation "chain" does not exist`로 실패했다. `docs/07-postgres-schema.sql`과 `migrations/0001_init.sql` 둘 다 `LANGUAGE plpgsql SET search_path = clustering, public;`로 고정해뒀다.
2. **`ClusterStore.RebuildFromEvidence()`가 DELETE 권한이 없어서 실패했다.** merge_evidence를 append-only로 지키려고 앱 유저에게 스키마 전체 DELETE를 안 줬는데, `cluster`/`cluster_membership`은 애초에 "언제든 전체 재생 가능한 파생 캐시"(02 §5)라 DELETE가 필요하다. `migrations/0002_create_app_user.sh`에 이 두 테이블만 예외로 DELETE를 추가했다(부모 파티션 테이블에 GRANT하면 chain별 파티션에도 자동 적용됨을 확인).
3. **로컬 macOS에 이미 `localhost:5432`를 쓰는 별도 Postgres가 떠 있어서** 앱이 도커 컨테이너가 아니라 그 프로세스에 연결되고 있었다(`role "clustering_app" does not exist` 에러로 발견). 우리 프로젝트와 무관한 프로세스라 건드리지 않고, `DB_PORT` 기본값을 `55432`로 옮겼다(`.env`, `.env.example`, `docker-compose.yml`). **다른 환경에서 포트 충돌이 다시 나면 `DB_PORT`만 바꾸면 된다.**
