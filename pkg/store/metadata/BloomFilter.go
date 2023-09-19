package metadata

import (
	"encoding/binary"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	BLOOM_FILTER_INSTANCE_SIZE int32 = util.SizeOf(&BloomFilter{})
	BLOOM_FILTER_NULL_HASHCODE int64 = int64(2862933555777941757)
)

type BloomFilter struct {
	// 继承
	Hashable

	bitSet           *BitSet
	numBits          int32
	numHashFunctions int32
}

func NewBloomFilter(expectedEntries int64, fpp float64) *BloomFilter {
	br := new(BloomFilter)
	nb := optimalNumOfBits(expectedEntries, fpp)
	br.numBits = nb + (util.INT64_SIZE - (nb % util.INT64_SIZE))
	br.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, int64(br.numBits))
	br.bitSet = NewBitSet(int64(br.numBits))
	return br
}
func NewBloomFilter2(bits []int64, numFuncs int32) *BloomFilter {
	br := new(BloomFilter)
	br.bitSet = NewBitSet2(bits)
	br.numBits = int32(br.bitSet.BitSize())
	br.numHashFunctions = numFuncs
	return br
}

func optimalNumOfHashFunctions(n int64, m int64) int32 {
	return maths.MaxInt32(1, int32(math.Round(float64(m/n)*math.Log(2))))
}

func optimalNumOfBits(n int64, p float64) int32 {
	return int32(float64(-n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

func (br *BloomFilter) GetRetainedSizeInBytes() int64 {
	return int64(BLOOM_FILTER_INSTANCE_SIZE + util.SizeOf(br.GetBitSet()))
}

// @Override
func (br *BloomFilter) AddHash(hasher *StatisticsHasher) {
	hasher.PutInt(br.GetNumBits()).PutInt(br.GetNumHashFunctions()).PutLongs(br.GetBitSet())
}

func (br *BloomFilter) Add(val []byte) {
	hash64 := util.Ternary((val == nil), BLOOM_FILTER_NULL_HASHCODE, Hash64(val))
	br.AddHash2(hash64)
}

func (br *BloomFilter) Add2(val *slice.Slice) {
	hash64 := util.Ternary((val == nil), BLOOM_FILTER_NULL_HASHCODE, Hash64_2(val))
	br.AddHash2(hash64)
}

func (br *BloomFilter) AddHash2(hash64 int64) {
	hash1 := int32(hash64)
	hash2 := int32(maths.UnsignedRightShift(hash64, 32))
	var i int32
	for i = 1; i <= br.numHashFunctions; i++ {
		combinedHash := hash1 + (i * hash2)
		if combinedHash < 0 {
			combinedHash = ^combinedHash
		}
		pos := combinedHash % br.numBits
		br.bitSet.Set(pos)
	}
}

func (br *BloomFilter) AddLong(val int64) {
	br.AddHash2(getLongHash(val))
}

func (br *BloomFilter) AddDouble(val float64) {
	br.AddLong(int64(math.Float64bits(val)))
}

func (br *BloomFilter) AddFloat(val float32) {
	br.AddDouble(float64(val))
}

func (br *BloomFilter) Test(val []byte) bool {
	hash64 := util.Ternary((val == nil), BLOOM_FILTER_NULL_HASHCODE, Hash64(val))
	return br.testHash(hash64)
}

func (br *BloomFilter) TestSlice(val *slice.Slice) bool {
	hash64 := util.Ternary((val == nil), BLOOM_FILTER_NULL_HASHCODE, Hash64_2(val))
	return br.testHash(hash64)
}

func (br *BloomFilter) testHash(hash64 int64) bool {
	hash1 := int32(hash64)
	hash2 := int32(maths.UnsignedRightShift(hash64, 32))

	var i int32
	for i = 1; i <= br.numHashFunctions; i++ {
		combinedHash := hash1 + (i * hash2)
		if combinedHash < 0 {
			combinedHash = ^combinedHash
		}
		pos := combinedHash % br.numBits
		if !br.bitSet.Get(pos) {
			return false
		}
	}
	return true
}

func (br *BloomFilter) TestLong(val int64) bool {
	return br.testHash(getLongHash(val))
}

func getLongHash(key int64) int64 {
	key = (^key) + (key << 21)
	key ^= (key >> 24)
	key = (key + (key << 3)) + (key << 8)
	key ^= (key >> 14)
	key = (key + (key << 2)) + (key << 4)
	key ^= (key >> 28)
	key += (key << 31)
	return key
}

func (br *BloomFilter) TestDouble(val float64) bool {
	return br.TestLong(int64(math.Float64bits(val)))
}

func (br *BloomFilter) TestFloat(val float32) bool {
	return br.TestDouble(float64(val))
}

func (br *BloomFilter) GetNumBits() int32 {
	return br.numBits
}

func (br *BloomFilter) GetNumHashFunctions() int32 {
	return br.numHashFunctions
}

func (br *BloomFilter) GetNumHashFunctionsPtr() *uint32 {
	u := uint32(br.numHashFunctions)
	return &u
}

func (br *BloomFilter) GetBitSet() []int64 {
	return br.bitSet.GetData()
}

type BitSet struct {
	data []int64
}

func NewBitSet(bits int64) *BitSet {
	// ??? 在golang中int32(math.Ceil(float(a/b)))可以等同于int32(a/b)
	// return NewBitSet2(make([]int64, int32(math.Ceil(float64(bits/util.INT64_SIZE)))))
	return NewBitSet2(make([]int64, int32(bits/util.INT64_SIZE)))
}
func NewBitSet2(data []int64) *BitSet {
	bt := new(BitSet)
	bt.data = data
	return bt
}

func (bt *BitSet) Set(index int32) {
	bt.data[maths.UnsignedRightShiftInt32(index, 6)] |= (int64(1) << wordsIndex(index))
}

const wordSize int32 = 64

// wordsIndex calculates the index of words in a int32
func wordsIndex(i int32) int32 {
	return i & (wordSize - 1)
}

func (bt *BitSet) Get(index int32) bool {
	return (bt.data[maths.UnsignedRightShiftInt32(index, 6)] & (int64(1) << wordsIndex(index))) != 0
}

func (bt *BitSet) BitSize() int64 {
	return util.LensInt64(bt.data) * util.INT64_SIZE
}

func (bt *BitSet) GetData() []int64 {
	return bt.data
}

var (
	C1           uint64 = 0x87c37b91114253d5
	C2           int64  = 0x4cf5ad432745937f
	R1           int32  = 31
	R2           int32  = 27
	M            int32  = 5
	N1           int32  = 0x52dce729
	DEFAULT_SEED int32  = 104729
)

type MothMurmur3 struct {
}

func NewMothMurmur3() *MothMurmur3 {
	m3 := new(MothMurmur3)
	return m3
}

// @SuppressWarnings("fallthrough")
func Hash64(data []byte) int64 {
	hash := int64(DEFAULT_SEED)
	fastLimit := (util.Lens(data) - util.INT64_BYTES) + 1
	current := util.INT32_ZERO
	for current < fastLimit {
		k := int64(binary.LittleEndian.Uint64(data[current : current+8]))

		current += util.INT64_BYTES
		k *= int64(C1)
		// TODO 是否有必要，为什么不使用bits.RotateLeft
		k = maths.RotateLeft(k, R1)
		k *= C2
		hash ^= k
		hash = maths.RotateLeft(hash, R2)*int64(M) + int64(N1)
	}
	k := util.INT64_ZERO
	switch util.Lens(data) - current {
	case 7:
		k ^= (int64(data[current+6]) & 0xff) << 48
	case 6:
		k ^= (int64(data[current+5]) & 0xff) << 40
	case 5:
		k ^= (int64(data[current+4]) & 0xff) << 32
	case 4:
		k ^= (int64(data[current+3]) & 0xff) << 24
	case 3:
		k ^= (int64(data[current+2]) & 0xff) << 16
	case 2:
		k ^= (int64(data[current+1]) & 0xff) << 8
	case 1:
		k ^= (int64(data[current]) & 0xff)
		k *= int64(C1)
		k = maths.RotateLeft(k, R1)
		k *= C2
		hash ^= k
	}
	hash ^= util.LensInt64(data)
	hash = fmix64(hash)
	return hash
}

// @SuppressWarnings("fallthrough")
func Hash64_2(data *slice.Slice) int64 {
	hash := int64(DEFAULT_SEED)
	fastLimit := int32(data.Size()-util.INT64_BYTES) + 1
	current := util.INT32_ZERO
	for current < fastLimit {
		k, _ := data.GetInt64LE(int(current))
		current += util.INT64_BYTES
		k *= int64(C1)
		k = maths.RotateLeft(k, R1)
		k *= C2
		hash ^= k
		hash = maths.RotateLeft(hash, R2)*int64(M) + int64(N1)
	}
	k := util.INT64_ZERO
	switch data.SizeInt32() - current {
	case 7:
		k ^= (int64(data.GetByteInt32(current+6)) & 0xff) << 48
	case 6:
		k ^= (int64(data.GetByteInt32(current+5)) & 0xff) << 40
	case 5:
		k ^= (int64(data.GetByteInt32(current+4)) & 0xff) << 32
	case 4:
		k ^= (int64(data.GetByteInt32(current+3)) & 0xff) << 24
	case 3:
		k ^= (int64(data.GetByteInt32(current+2)) & 0xff) << 16
	case 2:
		k ^= (int64(data.GetByteInt32(current+1)) & 0xff) << 8
	case 1:
		k ^= (int64(data.GetByteInt32(current)) & 0xff)
		k *= int64(C1)
		k = maths.RotateLeft(k, R1)
		k *= C2
		hash ^= k
	}
	hash ^= int64(data.Size())
	hash = fmix64(hash)
	return hash
}

func fmix64(h int64) int64 {
	var k uint64 = 0xff51afd7ed558ccd
	var k2 uint64 = 0xc4ceb9fe1a85ec53

	h ^= (maths.UnsignedRightShift(h, 33))
	h *= int64(k)
	h ^= (maths.UnsignedRightShift(h, 33))
	h *= int64(k2)
	h ^= (maths.UnsignedRightShift(h, 33))
	return h
}
