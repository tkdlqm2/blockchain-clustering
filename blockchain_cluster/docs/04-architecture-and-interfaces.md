# 04. 아키텍처 & 인터페이스 계약

> **스택 비종속**. 컴포넌트는 프레임워크가 아니라 **책임과 인터페이스 계약**으로 정의한다. 인터페이스는 논리 시그니처(입력→출력)로 기술하며, 물리 구현(클래스/함수/서비스 경계)은 구현자가 정한다.

---

## 1. 컴포넌트 분해

```
                ┌────────────────────────────────────────────────┐
   BalanceDelta │                클러스터링 시스템                  │
  ────────────▶ │                                                 │
  (인덱서)       │  [1] Ingestor      → 정규화·적재                 │
                │        │                                        │
                │        ▼                                        │
                │  [2] Preprocessor  → Hub/CoinJoin/Dust 표시     │
                │        │                                        │
                │        ▼                                        │
                │  [3] HeuristicEngines (플러그인)                │
                │       common-input / sweep / change / account   │
                │        │  (MergeCandidate 방출)                  │
                │        ▼                                        │
                │  [4] MergeEngine   → 근거 append + Union-Find    │
                │        │       ▲                                │
                │        ▼       │ (재생/롤백)                     │
                │  [5] EvidenceStore (진실의 원천, append-only)    │
                │  [6] ClusterStore (파생 캐시: cluster/membership)│
                │        │                                        │
                │  [7] LabelStore   → 라벨·신선도                  │
                │  [8] ReorgHandler → 무효화 + 재생                │
                │        │                                        │
                │        ▼                                        │
                │  [9] QueryService → 조회 표면 (01 §5)           │
                └────────────────────────────────────────────────┘
                                     │
                                     ▼
                          소비 서비스(리스크/추적/인텔리전스)
```

각 컴포넌트의 책임과 계약은 아래와 같다.

---

## 2. 컴포넌트별 책임 & 인터페이스 계약

### [1] Ingestor
- **책임**: 인덱서로부터 BalanceDelta를 수집(스트림/배치), 정규화(주소 정규화 규칙 적용), 멱등 적재. `source_block_hash` 보존.
- **계약**:
  - `ingest(deltas: BalanceDelta[]) → IngestResult` (멱등)
  - `getDeltasByTx(chain, txid) → BalanceDelta[]`
  - `getDeltasByBlock(chain, block_hash) → BalanceDelta[]`
- **불변식**: 중복 delta 재적재는 상태를 바꾸지 않는다(FR-3).

### [2] Preprocessor
- **책임**: 병합 이전 오염 차단. Hub/CoinJoin/Dust 판정 산출(03 §1~3). 결과를 `address.is_hub`, `excluded_tx`, `dust_flag`로 기록.
- **계약**:
  - `markHubs(scope) → void`
  - `markCollaborativeTx(scope) → void`
  - `markDust(scope) → void`
  - `isExcluded(chain, txid) → bool`, `isHub(chain, address) → (bool, type?, conf)`
- **불변식(순서)**: HeuristicEngines 실행 전에 완료되어야 한다(FR-7).

### [3] HeuristicEngines (플러그인 구조)
- **책임**: 각 휴리스틱이 **MergeCandidate만 방출**한다. 병합하지 않는다(FR-12).
- **공통 계약 (모든 엔진이 구현)**:
  - `name → string` (예 'common-input')
  - `chainSupport → Chain[]`
  - `generate(scope) → MergeCandidate[]`
- **MergeCandidate (자료구조)**:
  ```
  MergeCandidate {
    chain, address_a, address_b,
    heuristic, source_txid?, source_block_hash?, source_block_height?,
    confidence
  }
  ```
- **플러그인 원칙**: 새 휴리스틱 추가 = 새 엔진 구현 + 등록. 코어(4~6) 변경 불필요. (확장성 NFR-7)

### [4] MergeEngine
- **책임**: MergeCandidate를 받아 (a) 허브 경유 차단 재확인, (b) EvidenceStore에 근거 append, (c) Union-Find union, (d) 멤버십 confidence 갱신. (03 §6)
- **계약**:
  - `recordAndMerge(candidate) → op_id | rejected`
  - `find(chain, address) → cluster_root`
  - `union(a, b) → void` (내부용)
- **불변식**: 근거 append와 Union-Find union은 논리적으로 원자적. 근거 없이 union 없다.

### [5] EvidenceStore (★ 진실의 원천)
- **책임**: merge_evidence append-only 저장·조회·상태전이(active/invalidated).
- **계약**:
  - `append(evidence) → op_id`
  - `invalidate(op_id, reason) → void` (물리 삭제 아님, 상태 전이)
  - `scanActive(order_by=op_id) → stream<evidence>` (재생용)
  - `byBlockHash(hashes) → evidence[]` (reorg 롤백용)
  - `byAddressPair(a, b) → evidence[]` / `byCluster(cluster_id) → evidence[]` (감사)
- **불변식**: append-only. 재생은 결정적(02 §3-3).

### [6] ClusterStore (파생 캐시)
- **책임**: cluster/cluster_membership 물질화·조회. cluster_id 안정 규칙(02 §6) 적용.
- **계약**:
  - `clusterOf(chain, address, min_confidence?) → (cluster_id, confidence)`
  - `membersOf(cluster_id, min_confidence?, page) → address[]`
  - `sameCluster(chain, a, b, min_confidence?) → (bool, confidence)`
  - `rebuildFromEvidence() → void` (전체 재생)
- **불변식**: 항상 EvidenceStore(active)로부터 재생 가능. 불일치 시 재생값이 정답.

### [7] LabelStore
- **책임**: 라벨 CRUD, 출처 등급, 신선도 감쇠, 충돌 표시. sweep_target 앵커 관리.
- **계약**:
  - `addLabel(target, label, category, source, source_confidence) → label_id`
  - `labelsOf(target) → label[]`
  - `maintain() → void` (신선도·충돌; 03 §8)
  - `addSweepTarget(chain, address, entity_hint, source, confidence) → void`

### [8] ReorgHandler
- **책임**: reorg 통지 수신 → 근거 무효화 → 재생. 수동 정정도 동일 경로.
- **계약**:
  - `onReorg(rolled_back_block_hashes) → void`
  - `onManualCorrection(op_id) → void`
- **구현**: 03 §9 그대로. 무효화 + `ClusterStore.rebuildFromEvidence()`.

### [9] QueryService
- **책임**: 01 §5 조회 표면 제공. confidence 동반. 대용량 페이지네이션.
- **계약**: 01 §5의 논리 API를 물리 프로토콜로 노출(REST/gRPC/GraphQL은 구현자 선택).
- **불변식**: 모든 응답은 confidence를 포함(단정 금지, NFR·윤리).

---

## 3. 데이터 흐름 (정상 경로 & 예외 경로)

### 3.1 정상 (신규 블록 인덱싱)
```
인덱서 → Ingestor.ingest(deltas)
      → Preprocessor(markHubs/CoinJoin/Dust)
      → HeuristicEngines.generate() → MergeCandidate[]
      → MergeEngine.recordAndMerge() → EvidenceStore.append + Union-Find
      → ClusterStore 갱신
      → (주기) LabelStore.maintain()
```

### 3.2 예외 (reorg)
```
reorg 통지 → ReorgHandler.onReorg(hashes)
          → EvidenceStore.invalidate(관련 op)
          → ClusterStore.rebuildFromEvidence()
```

### 3.3 예외 (오탐 정정)
```
운영자 → ReorgHandler.onManualCorrection(op_id)
      → EvidenceStore.invalidate(op_id)
      → ClusterStore.rebuildFromEvidence()
```

---

## 4. 인덱서 연동 계약 (상류 경계)

- **입력 형태**: BalanceDelta 스트림 또는 배치. 전달 방식(메시지큐/DB 폴링/API)은 구현자 선택.
- **필수 필드**: 02 §1의 최소 필드, 특히 `source_block_hash`.
- **순서 보장**: 블록 높이 오름차순 권장. 순서가 흐트러질 수 있으면 Ingestor가 재정렬 버퍼를 둔다.
- **reorg 통지**: 인덱서가 reorg를 감지하면 롤백된 block_hash 목록을 ReorgHandler에 전달한다. 인덱서가 통지하지 못하면, 클러스터링 시스템이 자체적으로 저장된 block_hash와 최신 체인을 대조해 감지한다(폴백).

---

## 5. 확장 포인트

- **새 체인 추가**: HeuristicEngine 세트를 그 체인용으로 구현·등록(예 계정 체인 트랙). 코어(4~6)·데이터 모델 불변.
- **새 휴리스틱 추가**: HeuristicEngine 하나 추가. MergeCandidate 계약만 지키면 됨.
- **교차체인 엔티티 링크**: 동일 주체가 여러 체인 주소를 가질 때, 체인 내 cluster를 상위 "super-entity"로 링크하는 별도 매핑을 둔다(1차 범위 밖, 인터페이스만 예약).

---

## 6. 스택 선택 시 컴포넌트별 요구 능력 (체크)

| 컴포넌트 | 스택에 요구되는 능력 |
|---|---|
| Ingestor | 대량 멱등 upsert, 스트림/배치 수용 |
| EvidenceStore | append-only 대량 쓰기 + op_id 순 스캔 + block_hash/pair 인덱스 |
| MergeEngine | 수억 원소 Union-Find (인메모리 or 외부 상태 저장) |
| ClusterStore | 주소 키 조회, 대용량 membersOf 페이지네이션 |
| QueryService | 저지연 조회, confidence 동반 |

이 능력들을 만족하면 언어·DB·인프라는 자유롭게 선택 가능하다.
