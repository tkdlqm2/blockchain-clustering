-- name: ListEnabledChainConfigs :many
SELECT * FROM chain_config WHERE enabled = true;

-- name: GetChainConfig :one
SELECT * FROM chain_config WHERE chain_id = $1;
