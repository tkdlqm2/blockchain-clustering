# 05. 구현 계획 & 테스트 명세

> 구현 순서(마일스톤)와 완료 정의(DoD), 인수 기준, 테스트 매트릭스를 규정한다. 05의 인수 기준을 통과하는 것이 "구현 완료"의 정의다. 계약 준수가 최우선 검증 대상이다.

---

## 1. 구현 순서 (마일스톤)

### M0 — 기반 (상태·설정)
- StateStore(blocks·cursor), ChainRegistry(chain_config). 체인별 워커 골격.
- **DoD**: 체인 설정을 읽어 워커를 기동하고, 커서를 읽고 쓸 수 있다.

### M1 — 페칭 + 발행 골격
- NodeClient(최소: getLatestHeight·getBlock), BlockFetcher, Publisher(Kafka 발행, `type`·파티션 키·`amount` 문자열).
- **DoD**: 임의의 목(mock) delta를 계약 포맷으로 Kafka에 발행하고, 소비 측에서 스키마가 계약과 일치함을 확인.

### M2 — 계정 체인 추출 (Ethereum 최소)
- EthereumAdapter: native + ERC-20 로그, status==0 skip, 주소 소문자 정규화.
- **DoD**: 실제 블록 픽스처에서 native·토큰 delta가 계약 포맷으로 나오고, revert tx는 delta 0건.

### M3 — UTXO 추출 (Bitcoin)
- BitcoinAdapter: vin/vout 펼치기, prevout 조회(`-txindex` 경로), coinbase·비표준 output 처리, 주소 정규화.
- **DoD**: 실제 블록 픽스처에서 input(−)/output(+) delta가 정확히 나오고, Σ 보존(수수료 제외). coinbase는 수취 delta만.

### M4 — reorg 감지·통지
- ReorgDetector, handleReorg(공통조상·롤백·통지·재발행). prevout 캐시 사용 시 캐시 롤백.
- **DoD**: 강제 reorg 주입 시 reorg 이벤트가 먼저 발행되고, 내부 상태가 공통조상까지 롤백되며, 새 체인이 재발행된다.

### M5 — 계약 컨벤션 완성
- `meta.contract_creation`(컨트랙트 배포), `meta.token_contract`(ERC-20 식별).
- **DoD**: 컨트랙트 배포 tx의 컨트랙트 delta에 `contract_creation:true`, 모든 토큰 delta에 `token_contract`가 채워진다.

### M6 — 백필 + 회복성 + 관찰성
- backfill 모드, 지수 백오프 재시도, 지표(인덱싱 지연·reorg 빈도/깊이·발행 실패율).
- **DoD**: 과거 구간 백필이 실시간과 동일 순서·멱등으로 처리되고, 노드/발행 실패에서 커서 기반으로 재개된다.

### M7 — 추가 체인/모델 (선택·확장)
- 새 체인 어댑터(필요 시), 신규 model_type 대응.
- **DoD**: 새 체인이 설정+어댑터 추가만으로 동작하고 코어 변경이 없다.

---

## 2. 인수 기준 (필수 통과 — 전역 DoD)

계약 준수와 정확성에 직결되는, 어떤 스택이든 만족해야 하는 기준.

- **AC-1 계약 스키마 준수**: 발행 메시지가 계약(02 §A) 필드·타입과 정확히 일치. `type` 구분, 필수 필드 존재.
- **AC-2 amount 문자열**: 모든 `amount`가 문자열이며, 2^53 초과 uint256 값이 손실 없이 전달된다.
- **AC-3 멱등 키**: `(chain_id, txid, delta_index)`가 유일하고, 같은 블록 재처리 시 동일 키 집합을 생성.
- **AC-4 revert 제외**: 계정 체인 `receipt.status==0` 트랜잭션의 delta가 발행되지 않는다.
- **AC-5 block_hash 완전성**: 모든 BalanceDelta에 `block_hash`가 포함된다.
- **AC-6 reorg 통지 정확**: reorg 시 롤백 해시 목록이 정확하고, 통지가 재발행보다 **먼저** 나간다.
- **AC-7 순서성**: 체인 내 발행이 블록 높이 오름차순, 파티션 키 `chain_id`.
- **AC-8 정규화**: EVM 주소가 소문자로, Bitcoin 주소가 표준 인코딩으로 발행된다.
- **AC-9 컨벤션**: `meta.contract_creation`·`meta.token_contract`가 규칙대로 채워진다.
- **AC-10 chain_id 일치**: 발행 `chain_id`가 소비자 레지스트리 등록값과 정확히 일치(대소문자 포함).

---

## 3. 테스트 매트릭스

각 케이스는 최소 하나의 자동화 테스트로. 기대는 관찰 가능해야 한다.

| ID | 시나리오 | 입력 | 기대 |
|---|---|---|---|
| **T-1 계약 스키마** | 임의 delta 발행 | 목 delta | 소비자 스키마 검증 통과, `type` 구분 |
| **T-2 amount 큰 값** | uint256 대형 값 토큰 전송 | 2^53 초과 value | 문자열로 정확 전달, 소비자 파싱 무손실 |
| **T-3 멱등** | 같은 블록 2회 처리 | 동일 블록 ×2 | 동일 `(chain,txid,delta_index)` 키 집합 |
| **T-4 UTXO 펼치기** | 다중 vin/vout tx | BTC tx | input(−)·output(+) delta, Σ 보존, delta_index 연속 |
| **T-5 coinbase** | 코인베이스 tx | BTC coinbase | 수취 delta만, 지출 delta 없음 |
| **T-6 prevout 조회** | vin이 이전 output 참조 | BTC tx | 소유 주소·금액 정확 해석 |
| **T-7 ETH native** | value>0 전송 | ETH tx | from(−)/to(+) native delta |
| **T-8 ETH revert** | status==0 tx | 실패 tx | delta 0건 |
| **T-9 ERC-20** | Transfer 로그 | 토큰 전송 | from(−)/to(+) token delta, `meta.token_contract` 채워짐 |
| **T-10 ERC-721 제외** | topics 4개 Transfer | NFT 전송 | delta 미생성 |
| **T-11 컨트랙트 배포** | to==null tx | 배포 tx | 컨트랙트 delta에 `contract_creation:true` |
| **T-12 주소 정규화** | 체크섬 대소문자 EVM 주소 | ETH tx | 소문자로 발행 |
| **T-13 reorg 통지** | 깊이 N 재구성 | parent_hash 불일치 | reorg 이벤트(정확한 해시), 재발행보다 먼저 |
| **T-14 reorg 상태 롤백** | 재구성 후 재개 | reorg | 블록·prevout 캐시 공통조상까지 롤백, 커서 정정 |
| **T-15 순서·파티션** | 연속 블록 발행 | 다중 블록 | 높이 오름차순, 파티션 키 `chain_id` |
| **T-16 발행 실패 재개** | 발행 중 실패 | 실패 주입 | 커서 미전진, 재시도 시 멱등 |
| **T-17 백필 일치** | 과거 구간 백필 | from~to | 실시간과 동일 순서·멱등 |

---

## 4. 픽스처 지침

- **합성 픽스처**: 각 T-케이스는 실제 노드 없이 재현되도록 결정적 블록/트랜잭션 픽스처로 구성.
- **실데이터 픽스처**: 알려진 UTXO·ERC-20·컨트랙트 배포·reorg 블록을 소량 캡처해 회귀셋에 포함.
- **소비자 연동 검증**: 완성된 클러스터링 시스템(또는 그 스키마 검증기)으로 발행 메시지를 실제 소비해 계약 정합을 확인(가장 강력한 인수 테스트).

---

## 5. 리스크와 완화

| 리스크 | 영향 | 완화 |
|---|---|---|
| amount를 number로 발행 | 대형 값 손상(치명) | 직렬화 계층에서 강제 문자열, T-2로 감시 |
| reorg 통지 누락 | 소비자 오염 잔존 | 감지 로직 1급 취급, 자체 감지 권위화(06 §5), T-13/14 |
| prevout 캐시 롤백 누락 | UTXO 주소 오해석 | reorg 시 캐시 롤백 필수, T-14 |
| 토큰 식별자 누락 | 소비자 토큰 구분 불가 | `meta.token_contract` 필수화(FR-7), T-9 |
| 순서 뒤섞임 | 소비자 재생 오류 | 높이순 처리 + 파티션 키 chain_id, T-15 |

---

## 6. 완료 게이트

1. M0~M6의 DoD 충족(M7은 대상 체인 확장 시).
2. 인수 기준 AC-1~AC-10 전부 통과.
3. 테스트 매트릭스 T-1~T-17 자동화 통과.
4. 완성된 클러스터링 시스템과의 실연동 검증(§4) 통과.
5. 관찰성 지표·재시도·백필 동작(M6).

> 최종 확인: 계약의 세 절대 제약 — **amount 문자열 / 멱등 키 / block_hash 포함** — 과 **reorg 통지 정확성**이 코드·테스트로 증명되면, 나머지는 그 위의 체인별 추출 확장이다.
