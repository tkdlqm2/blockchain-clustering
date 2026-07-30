-- ============================================================================
-- 온체인 주소 클러스터링 시스템 — PostgreSQL 물리 스키마
-- 논리 모델(02-data-model)과 다체인 확장성 설계(06-multichain-extensibility)의 실현.
--
-- 설계 원칙:
--   * merge_evidence = 진실의 원천(append-only), cluster/membership = 파생 캐시.
--   * 코어 테이블은 chain 무관. 체인 변이는 레지스트리(chain/heuristic/chain_heuristic)로 흡수.
--   * 대용량 테이블은 chain_id 기준 LIST 파티셔닝 → 새 체인 = 파티션 추가.
--   * PostgreSQL 강점 활용: NUMERIC(78,0)로 uint256 무손실, 부분 인덱스, JSONB.
--
-- 실행 순서: 이 파일을 위에서 아래로 실행. 이후 SELECT add_chain('bitcoin', ...) 등으로 체인 등록.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS clustering;
SET search_path = clustering, public;

-- ============================================================================
-- 1. 레지스트리 (확장성의 핵심) — 파티션하지 않음(소형)
-- ============================================================================

-- 지원 체인 선언. 새 체인 추가의 1차 지점.
-- model_type은 enum이 아니라 text + CHECK로 두어, 새 모델 타입 확장을 쉽게 함(06 §7).
CREATE TABLE chain (
    chain_id              TEXT        PRIMARY KEY,               -- 'bitcoin','ethereum',...
    display_name          TEXT        NOT NULL,
    model_type            TEXT        NOT NULL,                  -- 'utxo' | 'account' | 그 외
    native_symbol         TEXT        NOT NULL,
    native_decimals       INT         NOT NULL,
    finality_depth        INT         NOT NULL DEFAULT 6,
    address_normalization TEXT        NOT NULL DEFAULT 'none',   -- 어댑터가 참조할 정규화 방식 식별자
    config                JSONB       NOT NULL DEFAULT '{}'::jsonb,
    enabled               BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (model_type IN ('utxo','account','other'))            -- 새 타입 추가 시 이 CHECK만 확장
);

-- 휴리스틱 카탈로그.
CREATE TABLE heuristic (
    heuristic_key      TEXT        PRIMARY KEY,   -- 'common-input','sweep-seed','change','funding','deployer','behavioral','manual'
    description        TEXT        NOT NULL,
    applies_to         TEXT        NOT NULL,      -- 'utxo' | 'account' | 'both'
    default_confidence NUMERIC(4,3) NOT NULL,     -- 0.000 ~ 1.000
    CHECK (applies_to IN ('utxo','account','both')),
    CHECK (default_confidence >= 0 AND default_confidence <= 1)
);

-- 체인별 휴리스틱 활성화·튜닝. 새 체인은 여기에 몇 줄 INSERT로 휴리스틱을 켠다.
CREATE TABLE chain_heuristic (
    chain_id            TEXT         NOT NULL REFERENCES chain(chain_id) ON DELETE CASCADE,
    heuristic_key       TEXT         NOT NULL REFERENCES heuristic(heuristic_key) ON DELETE CASCADE,
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE,
    confidence_override NUMERIC(4,3),                            -- null이면 heuristic.default_confidence 사용
    params              JSONB        NOT NULL DEFAULT '{}'::jsonb, -- 예: {"dust_threshold": 546, "hub_threshold": 0.9}
    PRIMARY KEY (chain_id, heuristic_key),
    CHECK (confidence_override IS NULL OR (confidence_override >= 0 AND confidence_override <= 1))
);

-- ============================================================================
-- 2. 수집/스테이징: balance_delta (인덱서로부터 Ingestor가 적재) — chain 파티셔닝
--    이 시스템이 소비하는 delta를 자체 보관(그룹핑·reorg에 필요).
-- ============================================================================

CREATE TABLE balance_delta (
    chain_id      TEXT          NOT NULL,
    txid          TEXT          NOT NULL,
    delta_index   INT           NOT NULL,
    address       TEXT          NOT NULL,
    amount        NUMERIC(78,0)  NOT NULL,          -- 부호 있는 정수. uint256 무손실.
    kind          TEXT          NOT NULL,           -- 'native' | 'token'
    block_height  BIGINT        NOT NULL,
    block_hash    TEXT          NOT NULL,           -- reorg 롤백 근거(필수)
    meta          JSONB,
    PRIMARY KEY (chain_id, txid, delta_index)        -- 파티션 키(chain_id) 포함
) PARTITION BY LIST (chain_id);

-- 공통 입력 그룹핑: 동일 txid의 지출(amount<0) 조회
CREATE INDEX bd_tx_idx        ON balance_delta (chain_id, txid);
-- reorg 롤백: block_hash로 delta 회수
CREATE INDEX bd_block_idx     ON balance_delta (chain_id, block_hash);
-- 주소 기준 조회
CREATE INDEX bd_addr_idx      ON balance_delta (chain_id, address, block_height);

-- 수집 진행 커서 (전달 방식 무관: Kafka offset / outbox height 등을 기록)
CREATE TABLE ingest_cursor (
    chain_id   TEXT        NOT NULL,
    source     TEXT        NOT NULL,               -- 'kafka' | 'outbox' | ...
    position   TEXT        NOT NULL,               -- offset 또는 height 등(문자열로 일반화)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, source)
);

-- ============================================================================
-- 3. 주소 레지스트리 — chain 파티셔닝
-- ============================================================================

CREATE TABLE address (
    chain_id          TEXT        NOT NULL,
    address           TEXT        NOT NULL,
    first_seen_height BIGINT,
    last_seen_height  BIGINT,
    is_hub            BOOLEAN     NOT NULL DEFAULT FALSE,   -- 전처리 산출(FR-4)
    hub_type          TEXT,                                 -- 'exchange'|'mixer'|'bridge'|'contract-hub'|null
    hub_confidence    NUMERIC(4,3),
    dust_flag         BOOLEAN     NOT NULL DEFAULT FALSE,   -- 전처리 산출(FR-6)
    PRIMARY KEY (chain_id, address)
) PARTITION BY LIST (chain_id);

-- 허브 조회(병합 차단 검사에서 사용)
CREATE INDEX addr_hub_idx ON address (chain_id, address) WHERE is_hub = TRUE;

-- ============================================================================
-- 4. merge_evidence — 진실의 원천 (append-only) — chain 파티셔닝
-- ============================================================================

-- op_id는 전역 시퀀스에서 발급(재생 순서). 파티션 PK에 chain_id 포함 필요.
CREATE SEQUENCE merge_op_seq;

CREATE TABLE merge_evidence (
    chain_id            TEXT         NOT NULL,
    op_id               BIGINT       NOT NULL DEFAULT nextval('merge_op_seq'),
    address_a           TEXT         NOT NULL,
    address_b           TEXT         NOT NULL,
    heuristic_key       TEXT         NOT NULL,               -- heuristic(heuristic_key) 논리 참조
    source_txid         TEXT,                                 -- manual/seed는 null 가능
    source_block_hash   TEXT,                                 -- 온체인 근거면 필수(reorg 롤백)
    source_block_height BIGINT,
    confidence          NUMERIC(4,3) NOT NULL,
    status              TEXT         NOT NULL DEFAULT 'active', -- 'active' | 'invalidated'
    invalidated_reason  TEXT,                                 -- 'reorg' | 'manual-correction' | null
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_by          TEXT         NOT NULL DEFAULT 'system',
    PRIMARY KEY (chain_id, op_id),
    CHECK (status IN ('active','invalidated')),
    CHECK (confidence >= 0 AND confidence <= 1)
) PARTITION BY LIST (chain_id);

-- 재생: active 근거를 op_id 순으로 스캔 (부분 인덱스로 효율화)
CREATE INDEX me_replay_idx ON merge_evidence (chain_id, op_id) WHERE status = 'active';
-- reorg 롤백: block_hash 기준 근거 회수
CREATE INDEX me_block_idx  ON merge_evidence (chain_id, source_block_hash);
-- 감사: 주소쌍 기준 근거 조회
CREATE INDEX me_addr_a_idx ON merge_evidence (chain_id, address_a);
CREATE INDEX me_addr_b_idx ON merge_evidence (chain_id, address_b);

-- ============================================================================
-- 5. 파생 캐시: cluster / cluster_membership — chain 파티셔닝
--    항상 merge_evidence(active)로부터 재생 가능. 불일치 시 재생값이 정답.
-- ============================================================================

CREATE TABLE cluster (
    chain_id                  TEXT         NOT NULL,
    cluster_id                TEXT         NOT NULL,     -- 결정적 규칙(02 §6): 대표 주소 유도
    size                      BIGINT       NOT NULL DEFAULT 0,
    entity_type               TEXT         NOT NULL DEFAULT 'unknown', -- 라벨에서 유도
    representative_confidence NUMERIC(4,3),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, cluster_id)
) PARTITION BY LIST (chain_id);

CREATE TABLE cluster_membership (
    chain_id              TEXT         NOT NULL,
    address               TEXT         NOT NULL,
    cluster_id            TEXT         NOT NULL,
    membership_confidence NUMERIC(4,3) NOT NULL,
    PRIMARY KEY (chain_id, address)                      -- 한 주소는 한 클러스터에만
) PARTITION BY LIST (chain_id);

-- 클러스터 구성원 조회(membersOf)
CREATE INDEX cm_cluster_idx ON cluster_membership (chain_id, cluster_id);

-- ============================================================================
-- 6. 라벨 / 시드 / 전처리 산출 — 소형, 파티션하지 않음
-- ============================================================================

CREATE TABLE label (
    label_id          BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_type       TEXT         NOT NULL,             -- 'cluster' | 'address'
    chain_id          TEXT         NOT NULL,
    target_cluster_id TEXT,                               -- target_type='cluster'
    target_address    TEXT,                               -- target_type='address'
    label             TEXT         NOT NULL,
    category          TEXT         NOT NULL,              -- 'exchange'|'mixer'|'bridge'|'scam'|'protocol'|...
    source            TEXT         NOT NULL,              -- 'known-deposit'|'official'|'crowdsourced'|'investigation'|'operator'
    source_confidence NUMERIC(4,3) NOT NULL,
    collected_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_verified_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status            TEXT         NOT NULL DEFAULT 'active', -- 'active'|'stale'|'conflicted'|'retired'
    CHECK (target_type IN ('cluster','address')),
    CHECK (status IN ('active','stale','conflicted','retired')),
    CHECK ( (target_type='cluster' AND target_cluster_id IS NOT NULL)
         OR (target_type='address' AND target_address   IS NOT NULL) )
);
CREATE INDEX label_cluster_idx ON label (chain_id, target_cluster_id) WHERE target_type='cluster';
CREATE INDEX label_address_idx ON label (chain_id, target_address)    WHERE target_type='address';
CREATE INDEX label_stale_idx   ON label (last_verified_at)            WHERE status='active';

-- 집금 목적지 앵커(거래소 클러스터 시드)
CREATE TABLE sweep_target (
    chain_id    TEXT         NOT NULL,
    address     TEXT         NOT NULL,
    entity_hint TEXT,                                     -- 어느 거래소로 추정/확정
    source      TEXT         NOT NULL,                    -- 'known-deposit'|'observed'
    confidence  NUMERIC(4,3) NOT NULL,
    PRIMARY KEY (chain_id, address)
);

-- 병합 제외 트랜잭션(coinjoin/hub-touch/dust-only)
CREATE TABLE excluded_tx (
    chain_id           TEXT         NOT NULL,
    txid               TEXT         NOT NULL,
    reason             TEXT         NOT NULL,             -- 'coinjoin'|'hub-touch'|'dust-only'
    detector_confidence NUMERIC(4,3),
    signal             TEXT,                               -- 보존 신호(예 'coinjoin:wasabi')
    PRIMARY KEY (chain_id, txid)
);

-- ============================================================================
-- 7. 크로스체인 엔티티 (super-entity) — 코어와 분리된 링크 계층 (06 §5)
-- ============================================================================

CREATE TABLE super_entity (
    super_entity_id BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    display_name    TEXT         NOT NULL,
    entity_type     TEXT         NOT NULL DEFAULT 'unknown',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 각 체인 클러스터를 상위 엔티티에 링크
CREATE TABLE super_entity_member (
    super_entity_id BIGINT       NOT NULL REFERENCES super_entity(super_entity_id) ON DELETE CASCADE,
    chain_id        TEXT         NOT NULL,
    cluster_id      TEXT         NOT NULL,
    confidence      NUMERIC(4,3) NOT NULL,                -- 이 링크의 신뢰도(라벨일치/공시/운영자)
    linked_by       TEXT         NOT NULL DEFAULT 'operator',
    PRIMARY KEY (super_entity_id, chain_id, cluster_id)
);
CREATE INDEX sem_cluster_idx ON super_entity_member (chain_id, cluster_id);

-- ============================================================================
-- 8. 감사 로그 (FR-26) — 소형
-- ============================================================================

CREATE TABLE audit_log (
    event_id  BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor     TEXT         NOT NULL,                      -- 'system' | operator-id
    action    TEXT         NOT NULL,                      -- 'merge'|'invalidate'|'label-add'|'hub-set'|'seed-add'|...
    target    JSONB        NOT NULL,                      -- 대상 참조(유연)
    rationale TEXT,
    at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ============================================================================
-- 9. 체인 추가 헬퍼 — "새 체인 = 함수 한 번 호출 + 휴리스틱 매핑"
--    파티션 대상 테이블 전부에 대해 해당 체인 파티션을 생성한다.
-- ============================================================================

CREATE OR REPLACE FUNCTION add_chain_partitions(p_chain TEXT) RETURNS void AS $$
DECLARE
    tbl TEXT;
    part_tables TEXT[] := ARRAY[
        'balance_delta','address','merge_evidence','cluster','cluster_membership'
    ];
BEGIN
    FOREACH tbl IN ARRAY part_tables LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES IN (%L)',
            tbl || '_' || p_chain, tbl, p_chain
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = clustering, public;

-- 체인 등록 + 파티션 생성을 한 번에. (휴리스틱 매핑은 아래 seed 예시 참고)
CREATE OR REPLACE FUNCTION add_chain(
    p_chain_id TEXT, p_display TEXT, p_model TEXT,
    p_symbol TEXT, p_decimals INT, p_finality INT, p_norm TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO chain(chain_id, display_name, model_type, native_symbol,
                      native_decimals, finality_depth, address_normalization)
    VALUES (p_chain_id, p_display, p_model, p_symbol, p_decimals, p_finality, p_norm)
    ON CONFLICT (chain_id) DO NOTHING;

    PERFORM add_chain_partitions(p_chain_id);

    -- model_type에 맞는 휴리스틱을 자동 활성화(applies_to 매칭)
    INSERT INTO chain_heuristic(chain_id, heuristic_key)
    SELECT p_chain_id, h.heuristic_key
    FROM heuristic h
    WHERE h.applies_to = p_model OR h.applies_to = 'both'
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql SET search_path = clustering, public;

-- ============================================================================
-- 10. 시드 데이터 — 휴리스틱 카탈로그
-- ============================================================================

INSERT INTO heuristic(heuristic_key, description, applies_to, default_confidence) VALUES
    ('common-input','같은 tx에서 함께 지출된 input은 같은 주체','utxo',   0.950),
    ('sweep-seed',  '검증 시드 집금 목적지로 모이는 입금주소','both',      0.900),
    ('change',      '잔돈 주소 추정(보수적)','utxo',                        0.400),
    ('funding',     '새 계정의 첫 자금 출처','account',                     0.600),
    ('deployer',    '컨트랙트 배포자','account',                           0.850),
    ('behavioral',  '반복 상호작용·시간 패턴(보조)','account',              0.300),
    ('manual',      '운영자 확정/부인','both',                            1.000)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- 11. 체인 등록 예시 (실제 운영 시 실행)
--   Bitcoin/Ethereum 등록 → 파티션 생성 + model_type별 휴리스틱 자동 매핑.
-- ============================================================================
-- SELECT add_chain('bitcoin',  'Bitcoin',  'utxo',    'BTC',  8, 6,  'bitcoin');
-- SELECT add_chain('ethereum', 'Ethereum', 'account', 'ETH', 18, 12, 'evm-lowercase');
-- 이후 필요 시 체인별 파라미터 오버라이드:
-- UPDATE chain_heuristic SET params = '{"dust_threshold": 546}'::jsonb
--   WHERE chain_id='bitcoin' AND heuristic_key='common-input';

-- ============================================================================
-- 설계 메모
--   * 새 체인 추가는 SELECT add_chain(...) 한 번 = 레지스트리 INSERT + 파티션 생성 + 휴리스틱 매핑.
--     코어 스키마 변경 0. (06 §6 체크리스트의 1~3단계를 이 함수가 수행)
--   * merge_evidence는 append-only. 무효화는 UPDATE status='invalidated'만(물리 삭제 금지).
--   * cluster/cluster_membership은 파생 캐시: merge_evidence(active) op_id 순 재생으로 재구성.
--   * reorg: DELETE 아님. me_block_idx로 근거를 찾아 status='invalidated' 후 재생.
--   * FK는 hot 파티션 테이블 간에는 최소화(성능). 정합성은 재생 가능성 + 애플리케이션 트랜잭션으로 보장.
--   * cluster_id 안정성(02 §6): 대표 주소의 결정적 함수로 유도해 재생 시 동일 id 유지.
-- ============================================================================
