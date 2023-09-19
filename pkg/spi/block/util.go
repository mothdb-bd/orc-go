package block

type RandSource interface {
	Uint64() uint64
}

// DifferenceU128 subtracts the smaller of a and b from the larger.
func DifferenceU128(a, b Uint128) Uint128 {
	if a.hi > b.hi {
		return a.Sub(b)
	} else if a.hi < b.hi {
		return b.Sub(a)
	} else if a.lo > b.lo {
		return a.Sub(b)
	} else if a.lo < b.lo {
		return b.Sub(a)
	}
	return Uint128{}
}

func LargerU128(a, b Uint128) Uint128 {
	if a.hi > b.hi {
		return a
	} else if a.hi < b.hi {
		return b
	} else if a.lo > b.lo {
		return a
	} else if a.lo < b.lo {
		return b
	}
	return a
}

func SmallerU128(a, b Uint128) Uint128 {
	if a.hi < b.hi {
		return a
	} else if a.hi > b.hi {
		return b
	} else if a.lo < b.lo {
		return a
	} else if a.lo > b.lo {
		return b
	}
	return a
}

// DifferenceI128 subtracts the smaller of a and b from the larger.
func DifferenceI128(a, b Int128) Int128 {
	if a.hi > b.hi {
		return a.Sub(b)
	} else if a.hi < b.hi {
		return b.Sub(a)
	} else if a.lo > b.lo {
		return a.Sub(b)
	} else if a.lo < b.lo {
		return b.Sub(a)
	}
	return Int128{}
}
