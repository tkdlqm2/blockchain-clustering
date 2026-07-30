package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// defaultPreprocessingParams are conservative starting points (docs/03 §10:
// "고정값 맹신 금지... 실측 튜닝") — a chain overrides any subset of these via
// chain.config JSONB (docs/06 §2.1: "config JSONB — 체인별 자유 설정").
// DustThreshold's default (546) is Bitcoin's classic dust limit; it means
// nothing for other chains and MUST be overridden per docs/03 §3 ("체인·
// 수수료 수준에 맞춰 설정") — it's a fallback, not a recommendation.
func defaultPreprocessingParams() domain.PreprocessingParams {
	return domain.PreprocessingParams{
		HubThreshold:            0.5,
		HubDegreeSaturation:     50,
		DustThreshold:           big.NewInt(546),
		CoinjoinConfidence:      0.8,
		DustExclusionConfidence: 0.8,
		EqualOutputMin:          3,
		CollabInputMin:          3,
		CollabOutputMin:         3,
	}
}

type preprocessingParamsOverrides struct {
	HubThreshold            *float64 `json:"hub_threshold"`
	HubDegreeSaturation     *float64 `json:"hub_degree_saturation"`
	DustThreshold           *string  `json:"dust_threshold"` // string: arbitrary precision, like Amount
	CoinjoinConfidence      *float64 `json:"coinjoin_confidence"`
	DustExclusionConfidence *float64 `json:"dust_exclusion_confidence"`
	EqualOutputMin          *int     `json:"equal_output_min"`
	CollabInputMin          *int     `json:"collab_input_min"`
	CollabOutputMin         *int     `json:"collab_output_min"`
}

// PreprocessingParamsFor resolves a chain's Preprocessor thresholds:
// defaults, overridden by whatever keys are present in chain.config.
func (s *Store) PreprocessingParamsFor(ctx context.Context, chainID string) (domain.PreprocessingParams, error) {
	params := defaultPreprocessingParams()

	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT config FROM clustering.chain WHERE chain_id = $1`, chainID).Scan(&raw)
	if err != nil {
		if err == pgx.ErrNoRows {
			return params, fmt.Errorf("registry: chain %q is not registered", chainID)
		}
		return params, fmt.Errorf("registry: preprocessing_params_for: %w", err)
	}
	if len(raw) == 0 {
		return params, nil
	}

	var overrides preprocessingParamsOverrides
	if err := json.Unmarshal(raw, &overrides); err != nil {
		return params, fmt.Errorf("registry: parse chain.config: %w", err)
	}

	if overrides.HubThreshold != nil {
		params.HubThreshold = *overrides.HubThreshold
	}
	if overrides.HubDegreeSaturation != nil {
		params.HubDegreeSaturation = *overrides.HubDegreeSaturation
	}
	if overrides.DustThreshold != nil {
		v, ok := new(big.Int).SetString(*overrides.DustThreshold, 10)
		if !ok {
			return params, fmt.Errorf("registry: invalid dust_threshold %q in chain.config", *overrides.DustThreshold)
		}
		params.DustThreshold = v
	}
	if overrides.CoinjoinConfidence != nil {
		params.CoinjoinConfidence = *overrides.CoinjoinConfidence
	}
	if overrides.DustExclusionConfidence != nil {
		params.DustExclusionConfidence = *overrides.DustExclusionConfidence
	}
	if overrides.EqualOutputMin != nil {
		params.EqualOutputMin = *overrides.EqualOutputMin
	}
	if overrides.CollabInputMin != nil {
		params.CollabInputMin = *overrides.CollabInputMin
	}
	if overrides.CollabOutputMin != nil {
		params.CollabOutputMin = *overrides.CollabOutputMin
	}
	return params, nil
}
