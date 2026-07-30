# CLAUDE.md

## 서비스 설명

블록체인 노드에서 블록을 순차적으로 읽어, 각 트랜잭션을 방향성 있는 자산 이동(BalanceDelta)으로 변환하고, 블록 높이 순서로·멱등하게·reorg 통지와 함께 Kafka로 발행하는 생산자(producer) 시스템이다.

**이 인덱서는 클러스터링 시스템과 별개의 프로젝트다.** 클러스터링 시스템은 이미 완성되어 있고 이 인덱서의 유일한 소비자다. 인덱서가 무엇을·어떤 형식으로 내보내야 하는지는 클러스터링 팀과의 연동 계약이 고정하며, 이 리포는 그 계약을 준수하는 생산자를 구현한다.

전체 설계 근거와 의사코드는 `docs/00-README.md`부터 `docs/06-contract-decisions.md`까지에 있다 — 이 리포에서 작업할 때는 반드시 먼저 참고할 것. 특히 `docs/03`(추출·reorg 알고리즘)과 `docs/06`(계약 §7 미결정 항목에 대한 인덱서 측 결정)이 정확성의 근간이다.

## 도메인

- **BalanceDelta**: 인덱서의 출력 원자 단위. `(chain_id, txid, delta_index, address, amount±, kind, block_height, block_hash, meta)`.
- **reorg**: 저장했던 블록이 더 무거운 체인으로 교체되는 사건. 인덱서가 **권위 있는 감지자**이며, 소비자는 인덱서의 통지에 전적으로 의존한다(소비자 측 폴백 미구현, `docs/06 §5`).
- **prevout**: UTXO 체인에서 input이 참조하는 이전 output. MVP는 `-txindex` 노드의 `getrawtransaction`으로 조회하며, 자체 prevout 캐시는 채택하지 않았다(성능 병목 확인 시 전환 검토, `docs/03 §prevout`).
- **cursor**: 체인별로 마지막으로 완전히 발행 완료한 블록 높이.

## 절대 어기면 안 되는 것 (계약 §고정, 협의 대상 아님)

- `amount`는 항상 **문자열**로 발행한다. JSON number 금지 — 2^53 초과 uint256 값에서 정밀도가 깨진다.
- `(chain_id, txid, delta_index)`는 멱등 키다. `delta_index`는 한 txid 안에서 0부터 연속·유일해야 한다.
- 모든 BalanceDelta에 `block_hash`를 포함한다 — reorg 롤백의 유일한 근거다.
- `chain_id`는 클러스터링 레지스트리 등록값과 **대소문자까지 정확히 일치**해야 한다.
- reorg 이벤트는 **재발행보다 먼저** 발행한다. 인덱서가 reorg 통지를 누락하면 소비자 데이터가 영구히 오염된다.
- 발행이 원자적으로 끝난 뒤에만 커서를 전진시킨다(부분 실패 시 커서 미전진 → 재처리로 재개).
- 계정 체인에서 `receipt.status == 0`(revert)인 트랜잭션은 delta를 만들지 않는다.
- 주소는 발행 **전에** 정규화한다(EVM 소문자, Bitcoin 표준 인코딩) — 정규화 책임은 인덱서에 있다.

## 기술스택

- 언어: Go
- DB: PostgreSQL (내부 상태 — block/cursor/chain_config. sqlc로 쿼리 코드 생성, `sqlc.yaml` 참고)
- 메시지 버스: Kafka (topic `balance-deltas`, 파티션 키 `chain_id`)
- 관찰성: Prometheus `/metrics` 엔드포인트 (인덱싱 지연, reorg 빈도/깊이, 발행 실패율 — NFR-7)
- 로컬 인프라: 부모 디렉토리(`../docker-compose.yml`, `/Users/dustin/Desktop/test/cluster/`)에서 클러스터링 프로젝트와 공유(Kafka KRaft 단일 브로커 + kafka-ui + PostgreSQL ×2). 원래 이 프로젝트 자체 compose가 있었으나 Kafka 포트(9092) 충돌 문제로 통합했다 — 자세한 내용은 `README.md` §2 참고.

## 외부 통신

| 대상 | 방향 | 설명 | 명세 |
|---|---|---|---|
| 블록체인 노드 (Ethereum 계열) | outbound | JSON-RPC — 블록/영수증 조회 | 서드파티 RPC 제공자(Infura/Alchemy/QuickNode 등) 사용, **mainnet** 기준(테스트넷 아님 — 실제 데이터임에 유의). 엔드포인트/키는 `.env`의 `ETH_MAINNET_RPC_URL` |
| 블록체인 노드 (Bitcoin) | outbound | Bitcoin Core RPC (`-txindex` 필요) | **의도적으로 미구현(M3 보류, Ethereum 우선).** `internal/adapter/bitcoin`은 스텁, `chain_config`의 `bitcoin-testnet` 행은 `enabled=false` |
| 클러스터링 시스템 | outbound (비동기, Kafka 경유) | topic `balance-deltas`로 BalanceDelta/reorg 이벤트 발행. 메시지 스키마는 `docs/02-data-model-and-output-contract.md` §A | **TODO: 클러스터링 팀의 연동 계약 원본 문서(문서 08)를 아직 받지 못함.** 지금은 `docs/02`, `docs/06`에 요약·인용된 내용만 있다. 원본 확보 시 `docs/api/`에 저장. |

## 미해결 TODO

- `docs/00-README.md`가 참조하는 `docs/04-architecture-and-interfaces.md`(컴포넌트 분해·인터페이스·어댑터 패턴)가 아직 작성/전달되지 않았다. 어댑터 확장 규칙 등 세부 사항은 `docs/03`의 의사코드로 추정해 구현했으니, 문서 확보 시 `internal/adapter` 설계와 대조·조정할 것.
- 클러스터링 팀 연동 계약 원본(문서 08) 미수령 — `meta.token_contract` 컨벤션 편입, `unparsed:` 주소 표기법 명문화 등 협의 항목이 `docs/06 §9`에 정리되어 있으니 원본 확보 시 반영. `chain_config.chain_id = 'ethereum'`도 이 문서로 대조 확인 필요.
- **Bitcoin(UTXO) 어댑터(M3)는 의도적으로 보류** — 현재 스코프는 Ethereum 우선. 착수 시 `internal/adapter/bitcoin`, prevout 조회(`-txindex`), UTXO 관련 테스트(T-4,5,6)가 필요.
- reorg 롤백(M4)은 단위 테스트로만 검증됐고 실제 mainnet reorg로는 아직 검증되지 않음(발생 빈도가 낮아 실전 발생 대기 중).
- 운영 배포 대상(AWS/K8s 등)과 운영 시크릿 관리 방식(Vault/SSM/K8s Secret)은 아직 미정 — 현재는 로컬 단일 서버 개발에 한정.
