// Package pgnumeric converts between pgtype.Numeric and *big.Int without
// loss, for the NUMERIC(78,0) columns chosen to hold uint256 amounts
// (docs/06-multichain-extensibility.md §4). Shared by every package that
// reads or writes such a column, so this conversion is only written once.
package pgnumeric

import (
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"
)

// ToBigInt converts a scanned NUMERIC into *big.Int. It only accepts an
// integral value (Exp == 0, or Exp > 0 which is still exactly an integer);
// a negative Exp would mean fractional digits, which NUMERIC(x,0) columns
// should never produce.
func ToBigInt(n pgtype.Numeric) (*big.Int, error) {
	if !n.Valid {
		return nil, fmt.Errorf("pgnumeric: value is NULL")
	}
	if n.Int == nil {
		return nil, fmt.Errorf("pgnumeric: value is non-finite (NaN/Inf)")
	}
	switch {
	case n.Exp == 0:
		return new(big.Int).Set(n.Int), nil
	case n.Exp > 0:
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)
		return new(big.Int).Mul(n.Int, scale), nil
	default:
		return nil, fmt.Errorf("pgnumeric: fractional scale (exp=%d), expected an integral NUMERIC", n.Exp)
	}
}

// FromBigInt encodes v as a scale-0 NUMERIC parameter.
func FromBigInt(v *big.Int) (pgtype.Numeric, error) {
	if v == nil {
		return pgtype.Numeric{}, fmt.Errorf("pgnumeric: value is required (nil)")
	}
	return pgtype.Numeric{Int: new(big.Int).Set(v), Exp: 0, Valid: true}, nil
}
