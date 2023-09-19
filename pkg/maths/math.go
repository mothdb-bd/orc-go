package maths

/**
 *
 */
func MinInt32(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

/**
 *
 */
func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MinFloat64(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

/**
 *
 */
func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func MinInt64s(x ...int64) int64 {
	min := x[0]
	for _, y := range x {
		if y < min {
			min = y
		}
	}
	return min
}

// MaxIntNum 算术右移可以判断一个数字的符号位，那么就可以不用比较符号实现两个数字的大小比较
func MaxIntNum(a, b int) int {
	c := a - b
	x := (c>>31)&1 ^ 1
	y := x ^ 1
	t := (a>>31)&1 ^ 1
	p := (b>>31)&1 ^ 1
	r1 := a*x + b*y
	r2 := a*t + b*p
	s1 := t ^ p ^ 1
	s2 := s1 ^ 1
	return r1*s1 + r2*s2
}

/**
 *
 */
func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func MaxFloat64(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}

func MaxInt16(x, y int16) int16 {
	if x > y {
		return x
	}
	return y
}

func MaxInt32(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

func AbsInt64(a int64) int64 {
	if a < 0 {
		return -a
	} else {
		return a
	}
}

func AbsInt32(a int32) int32 {
	if a < 0 {
		return -a
	} else {
		return a
	}
}

func Compare(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}

func CompareUnsigned(a, b int64) int {
	ua := uint64(a)
	ub := uint64(b)
	if ua < ub {
		return -1
	} else if ua > ub {
		return 1
	} else {
		return 0
	}
}

func CompareInt32(a, b int32) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}

func FloorDiv(x, y int64) int64 {
	r := x / y
	// if the signs are different and modulo not zero, round down
	if (x^y) < 0 && (r*y != x) {
		r--
	}
	return r
}

func FloorMod(x, y int64) int64 {
	return x - FloorDiv(x, y)*y
}

func NumberOfTrailingZeros(i int64) int32 {
	// HD, Figure 5-14
	var x, y int32
	if i == 0 {
		return 64
	}

	var n int32 = 63
	y = int32(i)
	if y != 0 {
		n = n - 32
		x = y
	} else {
		x = (int32)(uint64(i) >> 32)
	}

	y = x << 16
	if y != 0 {
		n = n - 16
		x = y
	}
	y = x << 8
	if y != 0 {
		n = n - 8
		x = y
	}
	y = x << 4
	if y != 0 {
		n = n - 4
		x = y
	}
	y = x << 2
	if y != 0 {
		n = n - 2
		x = y
	}
	return n - int32(uint32(x<<1)>>31)
}

func NumberOfTrailingZerosInt32(i int32) int32 {
	// HD, Figure 5-14
	var y int32
	if i == 0 {
		return 32
	}

	var n int32 = 31
	y = i << 16
	if y != 0 {
		n = n - 16
		i = y
	}
	y = i << 8
	if y != 0 {
		n = n - 8
		i = y
	}
	y = i << 4
	if y != 0 {
		n = n - 4
		i = y
	}
	y = i << 2
	if y != 0 {
		n = n - 2
		i = y
	}
	return n - int32(uint32(i<<1)>>31)
}

func MultiplyHigh(x int64, y int64) int64 {
	if x < 0 || y < 0 {
		x1 := x >> 32
		x2 := x & 0xFFFFFFFF
		y1 := y >> 32
		y2 := y & 0xFFFFFFFF
		z2 := x2 * y2
		t := x1*y2 + int64(uint64(z2)>>32)
		z1 := t & 0xFFFFFFFF
		z0 := t >> 32
		z1 += x2 * y1
		return x1*y1 + z0 + (z1 >> 32)
	} else {
		x1 := uint64(x) >> 32
		y1 := uint64(y) >> 32
		x2 := x & 0xFFFFFFFF
		y2 := y & 0xFFFFFFFF
		A := x1 * y1
		B := x2 * y2
		C := (int64(x1) + x2) * (int64(y1) + y2)
		K := C - int64(A) - B
		// return (UnsignedRightShift(((UnsignedRightShift(B, 32)) + K), 32)) + A
		return int64(uint64(int64(uint64(B)>>32)+K)>>32) + int64(A)
	}
}

func BitCount(i int64) int32 {
	// HD, Figure 5-14
	i = i - int64((uint64(i)>>1)&0x5555555555555555)
	i = (i & 0x3333333333333333) + int64((uint64(i)>>2)&0x3333333333333333)
	i = (i + int64(uint64(i)>>4)) & 0x0f0f0f0f0f0f0f0f
	i = i + int64(uint64(i)>>8)
	i = i + int64(uint64(i)>>16)
	i = i + int64(uint64(i)>>32)
	return int32(i) & 0x7f
}

func UnsignedRightShift(v int64, i int64) int64 {
	if i > 0 {
		return int64(uint64(v) >> uint64(i))
	} else {
		return int64(uint64(v) >> -uint64(i))
	}
}

func UnsignedRightShiftInt32(v int32, i int32) int32 {
	if i > 0 {
		return int32(uint32(v) >> uint32(i))
	} else {
		return int32(uint32(v) >> -uint32(i))
	}
}

func RotateLeft(i int64, distance int32) int64 {
	return (i << int64(distance)) | UnsignedRightShift(i, -int64(distance))
}

func AddExact(x int64, y int64) int64 {
	r := x + y
	// HD 2-12 Overflow iff both arguments have the opposite sign of the result
	if ((x ^ r) & (y ^ r)) < 0 {
		panic("long overflow")
	}
	return r
}
