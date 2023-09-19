package block

import (
	"fmt"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
)

var (
	DEFAULT_RETURN_VALUE int32   = -1
	INT_PHI              uint32  = 0x9E3779B9
	DEFAULT_LOAD_FACTOR  float32 = 0.75
)

type Int2IntOpenHashMap struct {
	key             []int32
	value           []int32
	mask            int32
	containsNullKey bool
	n               int32
	maxFill         int32
	minN            int32
	size            int32
	f               float32
}

func NewInt2IntOpenHashMap(expected int32) *Int2IntOpenHashMap {
	return NewInt2IntOpenHashMap2(expected, DEFAULT_LOAD_FACTOR)
}
func NewInt2IntOpenHashMap2(expected int32, f float32) *Int2IntOpenHashMap {
	ip := new(Int2IntOpenHashMap)
	if f <= 0 || f > 1 {
		panic("Load factor must be greater than 0 and smaller than or equal to 1")
	}
	if expected < 0 {
		panic("The expected number of elements must be nonnegative")
	}
	ip.f = f
	ip.n = arraySize(expected, f)
	ip.minN = ip.n
	ip.mask = ip.n - 1
	ip.maxFill = maxFill(ip.n, f)
	ip.key = make([]int32, ip.n+1)
	ip.value = make([]int32, ip.n+1)
	return ip
}

func (ip *Int2IntOpenHashMap) PutIfAbsent(k int32, v int32) int32 {
	pos := ip.find(k)
	if pos >= 0 {
		return ip.value[pos]
	}
	ip.insert(-pos-1, k, v)
	return DEFAULT_RETURN_VALUE
}

func (ip *Int2IntOpenHashMap) Get(k int32) int32 {
	if k == 0 {
		if ip.containsNullKey {
			return ip.value[ip.n]
		} else {
			return DEFAULT_RETURN_VALUE
		}
	}
	key := ip.key
	pos := (mix((k))) & ip.mask
	curr := key[pos]
	if curr == (0) {
		return DEFAULT_RETURN_VALUE
	}
	if (k) == (curr) {
		return ip.value[pos]
	}
	for {
		pos = (pos + 1) & ip.mask
		curr = key[pos]
		if curr == (0) {
			return DEFAULT_RETURN_VALUE
		}
		if (k) == (curr) {
			return ip.value[pos]
		}
	}
}

func (ip *Int2IntOpenHashMap) ContainsKey(k int32) bool {
	if (k) == (0) {
		return ip.containsNullKey
	}
	key := ip.key
	pos := (mix((k))) & ip.mask
	curr := key[pos]
	if curr == (0) {
		return false
	}
	if (k) == (curr) {
		return true
	}
	for {
		pos = (pos + 1) & ip.mask
		curr = key[pos]
		if curr == (0) {
			return false
		}
		if (k) == (curr) {
			return true
		}
	}
}

func (ip *Int2IntOpenHashMap) insert(pos int32, k int32, v int32) {
	if pos == ip.n {
		ip.containsNullKey = true
	}
	ip.key[pos] = k
	ip.value[pos] = v
	if ip.size >= ip.maxFill {
		ip.rehash(arraySize(ip.size+1, ip.f))
	}
	ip.size++
}

func (ip *Int2IntOpenHashMap) find(k int32) int32 {
	if k == 0 {
		if ip.containsNullKey {
			return ip.n
		} else {
			return -(ip.n + 1)
		}
	}
	key := ip.key
	pos := (mix((k))) & ip.mask
	curr := key[pos]
	if curr == (0) {
		return -(pos + 1)
	}
	if (k) == (curr) {
		return pos
	}
	for {
		pos = (pos + 1) & ip.mask
		curr = key[pos]
		if curr == (0) {
			return -(pos + 1)
		}
		if (k) == (curr) {
			return pos
		}
	}
}

func (ip *Int2IntOpenHashMap) rehash(newN int32) {
	key := ip.key
	value := ip.value
	mask := newN - 1
	newKey := make([]int32, newN+1)
	newValue := make([]int32, newN+1)
	i := ip.n
	var pos int32
	for j := ip.realSize(); j != 0; {
		i--
		for (key[i]) == (0) {
			i--
		}
		pos = (mix((key[i]))) & mask
		if !(newKey[pos] == (0)) {
			pos = (pos + 1) & mask
			for !((newKey[pos]) == (0)) {
				pos = (pos + 1) & mask
			}
		}
		newKey[pos] = key[i]
		newValue[pos] = value[i]

		// j 处理
		j--
	}
	newValue[newN] = value[ip.n]
	ip.n = newN
	ip.mask = mask
	ip.maxFill = maxFill(ip.n, ip.f)
	ip.key = newKey
	ip.value = newValue
}

func (ip *Int2IntOpenHashMap) realSize() int32 {
	if ip.containsNullKey {
		return ip.size - 1
	} else {
		return ip.size
	}
}

func mix(x int32) int32 {
	h := uint32(x) * INT_PHI
	return int32(h ^ (h >> 16))
}

func maxFill(n int32, f float32) int32 {
	return maths.MinInt32(int32(math.Ceil(float64(float32(n)*f))), n-1)
}

func arraySize(expected int32, f float32) int32 {
	s := maths.MaxInt32(2, int32(nextPowerOfTwo(int64(math.Ceil(float64(float32(expected)/f))))))
	if s > (1 << 30) {
		panic(fmt.Sprintf("Too large (%d expected elements with load factor %f)", expected, f))
	}
	return s
}

func nextPowerOfTwo(x int64) int64 {
	if x == 0 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	return (x | x>>32) + 1
}
