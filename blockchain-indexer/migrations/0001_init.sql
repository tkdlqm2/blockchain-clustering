-- 내부 상태 모델 (docs/02 §B). 물리 스키마는 구현자 선택이며, 여기서는 PostgreSQL로 결정.

CREATE TABLE chain_config (
    chain_id              TEXT PRIMARY KEY,       -- 클러스터링 레지스트리 등록값과 정확히 일치해야 함 (docs/06 §3 요약표)
    model_type            TEXT NOT NULL,          -- 'utxo' | 'account'
    node_endpoint         TEXT NOT NULL,
    node_auth_ref         TEXT,                   -- 값이 아니라 시크릿 참조 (docs/06 §7)
    finality_depth        BIGINT NOT NULL,
    address_normalization TEXT NOT NULL,          -- 'evm-lowercase' | 'bitcoin'
    start_height          BIGINT NOT NULL DEFAULT 0, -- 0 = 미설정. 커서 없는 최초 기동 시 latest tip 근처부터 시작(실시간 팔로우). 특정 높이부터 백필하려면 그 값으로 설정.
    enabled               BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE block (
    chain_id    TEXT NOT NULL REFERENCES chain_config (chain_id),
    height      BIGINT NOT NULL,
    hash        TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (chain_id, hash)
);

-- reorg 감지·조회용 (docs/02 §B.1: 조회 인덱스 (chain_id, height))
CREATE INDEX idx_block_chain_height ON block (chain_id, height);

CREATE TABLE cursor (
    chain_id TEXT PRIMARY KEY REFERENCES chain_config (chain_id),
    height   BIGINT NOT NULL
);

-- amount는 uint256까지 무손실 저장 가능해야 하므로 NUMERIC(78,0) (docs/02 §A.4).
-- MVP는 -txindex 노드로 prevout을 조회하므로(docs/03 §prevout 선택) prevout_cache 테이블은 생성하지 않는다.
-- 자체 캐시로 전환 시 이 마이그레이션 이후 별도 migration으로 추가한다.
