# 05. 구현 계획 & 테스트 명세

> 구현 순서(마일스톤)와 각 단계의 완료 정의(Definition of Done), 그리고 검증용 테스트 매트릭스·평가 방법론을 규정한다. 05의 인수 기준을 통과하는 것이 "구현 완료"의 정의다.

---

## 1. 구현 순서 (마일스톤)

의존성 순서로 배치한다. 각 마일스톤은 앞 단계 위에 쌓인다.

### M0 — 기반 (데이터 계층)
- EvidenceStore(append-only), ClusterStore(파생), address 레지스트리, LabelStore 스켈레톤 구현.
- Union-Find(경로압축+rank)와 `rebuildFromEvidence()` 재생 로직.
- **DoD**: 임의의 active 근거 집합을 넣고 `rebuildFromEvidence()`를 부르면 결정적으로 동일한 멤버십이 나온다. (핵심 불변식)

### M1 — 수집 (Ingestor)
- BalanceDelta 멱등 적재, 파생(공통입력집합/수취집합) 산출, `source_block_hash` 보존.
- **DoD**: 동일 배치 2회 적재 → 상태 동일(멱등). tx/block 단위 조회 동작.

### M2 — 병합 엔진 + 공통 입력 휴리스틱 (UTXO 최소 동작)
- MergeEngine.recordAndMerge, common-input 엔진.
- **DoD**: 한 tx의 다중 input이 하나의 클러스터로 묶이고, 각 병합에 근거(`source_block_hash` 포함)가 남는다.

### M3 — 전처리 (오염 차단)
- markHubs, markCollaborativeTx, markDust. MergeEngine·common-input이 제외/허브를 존중.
- **DoD**: 허브·CoinJoin이 섞인 데이터에서 supernode(비정상 거대 클러스터)가 발생하지 않는다. (T-대표 케이스, 아래 §3)

### M4 — sweep + 잔돈 + 시드 확장
- sweepHeuristic, sweep_target 앵커, expandFromSeeds, changeHeuristic(보수적).
- **DoD**: known-deposit 시드로부터 같은 집금 목적지의 입금주소들이 거래소 엔티티로 묶이되, 입금을 보낸 원천 주소는 묶이지 않는다.

### M5 — reorg·정정 (되돌림)
- ReorgHandler.onReorg / onManualCorrection. 근거 무효화 + 재생.
- **DoD**: 특정 block_hash 롤백 시 그 근거 기반 병합만 사라지고 나머지는 보존되며, 결과가 재생과 일치한다. 수동 정정도 동일.

### M6 — 신뢰도·라벨 신선도·조회 표면
- confidence 결합, threshold 뷰, LabelStore.maintain(신선도·충돌), QueryService(01 §5).
- **DoD**: `min_confidence`에 따라 클러스터 뷰가 달라지고, 조회 응답이 confidence를 동반한다. 상충 라벨이 conflicted로 표시된다.

### M7 — 계정 체인 트랙 (선택/확장)
- accountModelHeuristics(funding/deployer/behavioral), 계정 체인 허브 처리.
- **DoD**: 계정 체인 데이터에서 funding/deployer로 병합이 형성되며, 인기 컨트랙트가 supernode를 만들지 않는다.

### M8 — 관찰성·운영
- 지표(supernode 발생, 병합률, 정정 빈도, 라벨 신선도), 감사 로그, 파라미터 외부화.
- **DoD**: 파라미터가 하드코딩 없이 설정 가능하고, 핵심 지표가 노출된다.

---

## 2. 인수 기준 (필수 통과 — Definition of Done 전역)

아래는 어떤 스택이든 반드시 만족해야 하는 시스템 수준 기준이다.

- **AC-1 재생 결정성**: 동일한 active 근거 집합 → 항상 동일한 클러스터 멤버십. (NFR-3)
- **AC-2 되돌림**: 임의 병합을 근거 무효화만으로 되돌릴 수 있고, 전체 재계산 없이(또는 동일결과 부분재생으로) 반영된다. (NFR-2)
- **AC-3 순서 불변**: 전처리가 휴리스틱보다 먼저 실행됨이 코드/테스트로 보장된다. (FR-7)
- **AC-4 supernode 억제**: 허브·CoinJoin 포함 데이터에서 비정상 거대 클러스터가 생기지 않는다. (핵심)
- **AC-5 sweep 경계**: 입금주소는 거래소로 묶되, 입금을 보낸 원천 주소는 묶이지 않는다. (FR-9)
- **AC-6 근거 완전성**: 모든 온체인 병합이 `source_block_hash`를 가진 근거로 설명된다. (NFR-5)
- **AC-7 confidence 동반**: 모든 클러스터/라벨 조회 응답이 신뢰도를 포함한다. (윤리·NFR-1)
- **AC-8 멱등 수집**: 동일 입력 재수집이 상태를 바꾸지 않는다. (FR-3)

---

## 3. 테스트 매트릭스 (정확성 케이스)

각 케이스는 최소 하나의 자동화 테스트로 구현한다. "기대"는 반드시 관찰 가능해야 한다.

| ID | 시나리오 | 입력 | 기대 |
|---|---|---|---|
| **T-1 멱등** | 같은 블록 2회 처리 | 동일 delta 배치 ×2 | 멤버십·근거 수 동일 |
| **T-2 공통입력** | 1 tx에 input A,B,C | 지출 delta 3개 | {A,B,C} 한 클러스터, 근거 2건(별 병합) |
| **T-3 허브 제외** | 거래소 핫월렛이 다수에게 지급 | 허브 표시된 tx | 수취인들이 서로 묶이지 **않음**, supernode 없음 |
| **T-4 CoinJoin 제외** | 동일 금액 output 다수 | coinjoin tx | 공통입력 병합 미발생, excluded로 표시, signal 보존 |
| **T-5 sweep 클러스터** | 입금주소들이 같은 target으로 집금 | seed target + 집금 tx | 입금주소들 ∪ target = 거래소 엔티티 |
| **T-6 sweep 경계** | 사용자가 입금주소로 입금 후 집금 | 입금 tx + 집금 tx | 입금주소·target은 묶이나, 보낸 사용자 원천주소는 **제외** |
| **T-7 잔돈 보수** | 잔돈 후보가 있는 tx | 단서 결합 | 낮은 confidence 병합만, 단독 확정 없음 |
| **T-8 dust 방어** | dust 유입 후 함께 소비 | dusted tx | dust-only 근거 병합 차단 |
| **T-9 reorg 롤백** | 깊이 N 롤백 | block_hash 무효화 | 해당 근거 병합만 소거, 나머지 보존, 재생 일치 |
| **T-10 수동 정정** | 오탐 op 무효화 | invalidate(op) | 그 병합만 되돌려지고 재생과 일치 |
| **T-11 threshold 뷰** | min_confidence 상·하 | 동일 근거 | 임계치에 따라 멤버십 달라짐 |
| **T-12 라벨 충돌** | 같은 target 상충 라벨 | 두 category | conflicted 표시, 자동확정 없음 |
| **T-13 라벨 신선도** | TTL 초과 라벨 | maintain 실행 | confidence 감쇠/stale, 재검증 큐 |
| **T-14 계정 funding** | 새 EOA가 funder로부터 시드 | funding tx | funder-EOA 병합(중간 confidence), 허브 funder면 미병합 |
| **T-15 batch collect** | 다수 입금주소 1 tx 집금 | 다중 input 집금 tx | 공통입력으로도 동일 엔티티 수렴 |

---

## 4. 평가 방법론 (품질 측정)

기능 테스트(§3)를 넘어, 클러스터링 **품질**을 지속 측정한다.

- **Ground truth**: known-deposit 실험으로 확보한 검증 라벨, 공개 신뢰 데이터셋을 정답셋으로.
- **지표**:
  - **정밀도(precision)** 우선: 병합된 쌍 중 실제로 같은 주체인 비율. 오탐 억제가 목표라 precision을 recall보다 중시(NFR-1).
  - **재현율(recall)**: 같은 주체인 쌍 중 실제로 병합된 비율. 보조 지표.
  - **supernode 지표**: 최대 클러스터 크기, 크기 분포. 급증은 오탐 신호.
  - **정정율**: 운영자 수동 무효화 빈도. 상승 시 파라미터 재튜닝 신호.
- **회귀 감시**: 파라미터·휴리스틱 변경 시 위 지표의 변화를 회귀로 감시. precision 하락은 배포 차단 기준.

---

## 5. 시드/픽스처 준비 지침

- **합성 픽스처**: 각 T-케이스는 실제 체인 데이터 없이도 재현되도록 합성 delta 픽스처로 구성(결정적).
- **실데이터 픽스처**: 알려진 거래소 집금 패턴, 알려진 CoinJoin tx를 소량 캡처해 회귀셋에 포함(라이선스·프라이버시 유의).
- **known-deposit 시드**: 운영 초기 확보한 시드를 테스트·평가의 정답 앵커로 재사용.

---

## 6. 리스크와 완화 (구현 관점)

| 리스크 | 영향 | 완화 |
|---|---|---|
| 허브 미탐지 | supernode 붕괴 | HUB_THRESHOLD 보수적, 지표 감시, known 라벨 우선 |
| 잔돈 오판 | 상대방 오병합 | change confidence 낮게, 단독 확정 금지 |
| 재생 비용 | 롤백/정정 지연 | 부분 재생 최적화(단, 전체 재생과 동일결과 보장 시만) |
| Union-Find 메모리 | 확장성 한계 | 외부화 전략(스택 선택 시), 연결요소 분할 |
| 라벨 노후화 | 잘못된 정체성 전파 | 신선도 감쇠, 출처 등급, 충돌 표시 |

---

## 7. 구현 완료 선언 조건 (최종 게이트)

다음을 모두 만족하면 이 시스템은 "완성"으로 본다.

1. M0~M6 마일스톤의 DoD 전부 충족(M7 계정 트랙은 대상 체인에 계정 체인이 포함될 때 필수).
2. 인수 기준 AC-1 ~ AC-8 전부 통과.
3. 테스트 매트릭스 T-1 ~ T-13(+ 계정 체인 시 T-14) 자동화 통과.
4. 평가 방법론(§4)의 precision 기준선 확보 및 회귀 감시 가동.
5. 파라미터 외부화·관찰성 지표·감사 로그 동작(M8).

> 마지막 확인: 세 가지 핵심(허브를 먼저 제외 / 근거를 진실의 원천으로 / 되돌릴 수 있게 재생)이 코드와 테스트로 증명되면, 나머지는 그 위의 확장이다.
