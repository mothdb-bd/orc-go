package array

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
)

var (
	INT_SEGMENT_MASK int32 = SEGMENT_SIZE - 1
	SMALL            int32 = 7
	MEDIUM           int32 = 40
)

type IntBigArrays struct {
}

func NewIntBigArrays() *IntBigArrays {
	is := new(IntBigArrays)
	return is
}

func Segment(index int64) int32 {
	return int32(uint64(index) >> SEGMENT_SHIFT)
}

func Displacement(index int64) int32 {
	return int32(index & int64(SEGMENT_MASK))
}

func Get(array [][]int32, index int64) int32 {
	return array[Segment(index)][Displacement(index)]
}

func Set(array [][]int32, index int64, value int32) {
	array[Segment(index)][Displacement(index)] = value
}

func Swap(array [][]int32, first int64, second int64) {
	t := array[Segment(first)][Displacement(first)]
	array[Segment(first)][Displacement(first)] = array[Segment(second)][Displacement(second)]
	array[Segment(second)][Displacement(second)] = t
}

// @SuppressWarnings("checkstyle:InnerAssignment")
func QuickSort(x [][]int32, from int64, to int64, comp IntComparator) {
	len := to - from
	if len < int64(SMALL) {
		selectionSort(x, from, to, comp)
		return
	}
	m := from + len/2
	if len > int64(SMALL) {
		l := from
		n := to - 1
		if len > int64(MEDIUM) {
			s := len / 8
			l = med3(x, l, l+s, l+2*s, comp)
			m = med3(x, m-s, m, m+s, comp)
			n = med3(x, n-2*s, n-s, n, comp)
		}
		m = med3(x, l, m, n, comp)
	}
	v := Get(x, m)
	a := from
	b := a
	c := to - 1
	d := c
	for {
		var comparison int32
		comparison = comp.Compare(Get(x, b), v)
		for b <= c && comparison <= 0 {
			if comparison == 0 {
				a++
				Swap(x, a, b)
			}
			b++
			comparison = comp.Compare(Get(x, b), v)
		}
		comparison = comp.Compare(Get(x, c), v)
		for c >= b && comparison >= 0 {
			if comparison == 0 {
				d++
				Swap(x, c, d)
			}
			c--
			comparison = comp.Compare(Get(x, c), v)
		}
		if b > c {
			break
		}
		b++
		c++
		Swap(x, b, c)
	}
	var s int64
	n := to
	s = maths.Min(a-from, b-a)
	vecSwap(x, from, b-s, s)
	s = maths.Min(d-c, n-d-1)
	vecSwap(x, b, n-s, s)

	s = b - a
	if s > 1 {
		QuickSort(x, from, from+s, comp)
		s = b - a
	}
	s = d - c
	if s > 1 {
		QuickSort(x, n-s, n, comp)
		s = d - c
	}
}

func vecSwap(x [][]int32, a int64, b int64, n int64) {
	for i := 0; i < int(n); i++ {
		Swap(x, a, b)
	}
}

func med3(x [][]int32, a int64, b int64, c int64, comp IntComparator) int64 {
	ab := comp.Compare(Get(x, a), Get(x, b))
	ac := comp.Compare(Get(x, a), Get(x, c))
	bc := comp.Compare(Get(x, b), Get(x, c))

	// return (ab < 0 ?
	// 	(bc < 0 ? b : ac < 0 ? c : a) :
	// 	(bc > 0 ? b : ac > 0 ? c : a));
	if ab < 0 {
		if bc < 0 {
			return b
		} else if ac < 0 {
			return c
		} else {
			return a
		}
	} else if bc > 0 {
		return b
	} else if ac > 0 {
		return c
	} else {
		return a
	}
}

func selectionSort(a [][]int32, from int64, to int64, comp IntComparator) {
	for i := from; i < to-1; i++ {
		m := i
		for j := i + 1; j < to; j++ {
			if comp.Compare(Get(a, j), Get(a, m)) < 0 {
				m = j
			}
		}
		if m != i {
			Swap(a, i, m)
		}
	}
}
