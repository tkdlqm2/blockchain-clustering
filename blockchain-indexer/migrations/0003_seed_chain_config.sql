-- chain_config 초기 시드. 대상 체인은 Ethereum mainnet + Bitcoin 테스트넷 (05 마일스톤 M2/M3).
--
-- 주의: chain_id 값('ethereum', 'bitcoin-testnet')은 아직 클러스터링 팀의 연동 계약
-- 원본(문서 08)을 받지 못해 확정된 레지스트리 값이 아니다 — 반드시 확인 후 필요시 수정할 것
-- (docs/02 §A: chain_id는 클러스터링 레지스트리 등록값과 대소문자까지 정확히 일치해야 함).
--
-- node_endpoint도 아직 실제 RPC 제공자(Infura/Alchemy/QuickNode 등) URL이 없어 placeholder다.
-- 그래서 enabled=false로 넣는다 — 실제 값 채운 뒤 enabled=true로 UPDATE할 것.
--
-- start_height=0은 "미설정"을 뜻한다 — 커서가 없는 최초 기동 시 인덱서가 latest tip 근처부터
-- 실시간 팔로우로 시작한다(genesis 전체 백필 아님, cmd/indexer/main.go runEthereumLoop 참고).
-- 특정 높이부터 과거를 백필하고 싶으면 이 값을 그 높이로 UPDATE할 것.

INSERT INTO chain_config (chain_id, model_type, node_endpoint, node_auth_ref, finality_depth, address_normalization, start_height, enabled)
VALUES
  ('ethereum',        'account', 'TODO: ETH_MAINNET_RPC_URL 채우기', NULL, 64, 'evm-lowercase', 0, false),
  ('bitcoin-testnet', 'utxo',    'TODO: BTC_TESTNET_RPC_URL 채우기', NULL, 6,  'bitcoin',       0, false)
ON CONFLICT (chain_id) DO NOTHING;
