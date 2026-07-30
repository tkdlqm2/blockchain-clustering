package heuristic

import (
	"math/big"
	"testing"
)

func TestIsRoundAmount(t *testing.T) {
	cases := []struct {
		amount int64
		want   bool
	}{
		{1000, true},
		{5000, true},
		{1234, false},
		{999, false},
		{0, true},
	}
	for _, c := range cases {
		got := isRoundAmount(big.NewInt(c.amount))
		if got != c.want {
			t.Fatalf("isRoundAmount(%d) = %v, want %v", c.amount, got, c.want)
		}
	}
}

func TestParseChangeParams_Defaults(t *testing.T) {
	p := parseChangeParams(nil)
	if p.ChangeMin != 0.3 || p.NewAddressWeight != 0.5 || p.NotRoundAmountWeight != 0.2 || p.RespentWeight != 0.3 {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseChangeParams_PartialOverride(t *testing.T) {
	p := parseChangeParams([]byte(`{"change_min": 0.5}`))
	if p.ChangeMin != 0.5 {
		t.Fatalf("expected change_min override, got %v", p.ChangeMin)
	}
	if p.NewAddressWeight != 0.5 {
		t.Fatalf("expected new_address_weight to keep default, got %v", p.NewAddressWeight)
	}
}
