package block

import (
	"math"
)

// modpos is a very slimmed-down approximation of math.Mod, but without support
// for any of the things we don't need here. It is intended for when x is known
// to be positive. All calls have been hand-inlined for performance.
func modpos(x, y float64) float64 {
	const (
		mask  = 0x7FF
		shift = 64 - 11 - 1
		bias  = 1023
	)

	ybits := math.Float64bits(y)

	bits := ybits
	yexp := int((bits>>shift)&mask) - bias + 1
	bits &^= mask << shift
	bits |= (-1 + bias) << shift
	yfr := math.Float64frombits(bits)

	r := x
	for r >= y {
		bits = math.Float64bits(r)
		rexp := int((bits>>shift)&mask) - bias + 1
		bits &^= mask << shift
		bits |= (-1 + bias) << shift
		rfr := math.Float64frombits(bits)

		if rfr < yfr {
			rexp = rexp - 1
		}

		x := ybits
		exp := (rexp - yexp) + int(x>>shift)&mask - bias
		x &^= mask << shift
		x |= uint64(exp+bias) << shift
		r = r - math.Float64frombits(x)
	}
	return r
}
