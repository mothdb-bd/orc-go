package util

import "github.com/mothdb-bd/orc-go/pkg/maths"

var QUICKSORT_NO_REC int32 = 16
var QUICKSORT_MEDIAN_OF_9 int32 = 128

/**
 * Sorts the specified range of elements according to the natural ascending
 * order using indirect quicksort.
 *
 * <p>
 * The sorting algorithm is a tuned quicksort adapted from Jon L. Bentley and M.
 * Douglas McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software:
 * Practice and Experience</i>, 23(11), pages 1249&minus;1265, 1993.
 *
 * <p>
 * This method implement an <em>indirect</em> sort. The elements of {@code perm}
 * (which must be exactly the numbers in the interval {@code [0..perm.length)})
 * will be permuted so that {@code x[perm[i]] &le; x[perm[i + 1]]}.
 *
 * <p>
 * Note that this implementation does not allocate any object, contrarily to the
 * implementation used to sort primitive types in {@link java.util.Arrays},
 * which switches to mergesort on large inputs.
 *
 * @param perm a permutation array indexing {@code x}.
 * @param x the array to be sorted.
 * @param from the index of the first element (inclusive) to be sorted.
 * @param to the index of the last element (exclusive) to be sorted.
 */
func QuickSortIndirect(perm []int32, x []float64, from int32, to int32) {
	len := to - from
	// Selection sort on smallest arrays
	if len < QUICKSORT_NO_REC {
		insertionSortIndirect(perm, x, from, to)
		return
	}
	// Choose a partition element, v
	m := from + len/2
	l := from
	n := to - 1
	if len > QUICKSORT_MEDIAN_OF_9 { // Big arrays, pseudomedian of 9
		s := len / 8
		l = med3Indirect(perm, x, l, l+s, l+2*s)
		m = med3Indirect(perm, x, m-s, m, m+s)
		n = med3Indirect(perm, x, n-2*s, n-s, n)
	}
	m = med3Indirect(perm, x, l, m, n) // Mid-size, med of 3
	v := x[perm[m]]
	// Establish Invariant: v* (<v)* (>v)* v*
	a := from
	b := a
	c := to - 1
	d := c
	for {
		var comparison float64
		comparison = x[perm[b]] - v
		for b <= c && comparison <= 0 {
			if comparison == 0 {
				swap(perm, a, b)
				a++
			}
			b++
			comparison = x[perm[b]] - v
		}

		comparison = x[perm[c]] - v
		for c >= b && comparison >= 0 {
			if comparison == 0 {
				swap(perm, c, d)
				d--
			}
			c--
			comparison = x[perm[c]] - v
		}
		if b > c {
			break
		}
		swap(perm, b, c)
		b++
		c--
	}
	// Swap partition elements back to middle
	var s int32
	s = maths.MinInt32(a-from, b-a)
	swap2(perm, from, b-s, s)
	s = maths.MinInt32(d-c, to-d-1)
	swap2(perm, b, to-s, s)
	// Recursively sort non-partition-elements
	s = b - a
	if s > 1 {
		QuickSortIndirect(perm, x, from, from+s)
	}
	s = d - c
	if s > 1 {
		QuickSortIndirect(perm, x, to-s, to)
	}
}

func insertionSortIndirect(perm []int32, a []float64, from int32, to int32) {
	for i := from; numAdd(&i) < to; {
		t := perm[i]
		j := i

		for u := perm[j-1]; compare((a[t]), (a[u])) < 0; u = perm[numSub(&j)-1] {
			perm[j] = u
			if from == j-1 {
				j--
				break
			}
		}
		perm[j] = t
	}
}

func compare(a, b float64) float64 {
	return a - b
}

func numAdd(n *int32) int32 {
	(*n)++
	return *n
}

func numSub(n *int32) int32 {
	(*n)--
	return *n
}

func med3Indirect(perm []int32, x []float64, a int32, b int32, c int32) int32 {
	aa := x[perm[a]]
	bb := x[perm[b]]
	cc := x[perm[c]]
	ab := compare((aa), (bb))
	ac := compare((aa), (cc))
	bc := compare((bb), (cc))

	return Ternary(ab < 0, Ternary(bc < 0, b, Ternary(ac < 0, c, a)), Ternary(bc > 0, b, Ternary(ac > 0, c, a)))
}

func swap(x []int32, a int32, b int32) {
	t := x[a]
	x[a] = x[b]
	x[b] = t
}

func swap2(x []int32, a int32, b int32, n int32) {
	for i := INT32_ZERO; i < n; numAdd2(&i, &a, &b) {
		swap(x, a, b)
	}
}

func numAdd2(a, b, c *int32) {
	(*a)++
	(*b)++
	(*c)++
}
