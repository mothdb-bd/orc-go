package store

import (
	"fmt"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/array"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DICTIONARY_INSTANCE_SIZE            int32   = util.SizeOf(&DictionaryBuilder{})
	DICTIONARY_FILL_RATIO               float32 = 0.75
	DICTIONARY_EMPTY_SLOT               int32   = -1
	DICTIONARY_NULL_POSITION            int32   = 0
	DICTIONARY_EXPECTED_BYTES_PER_ENTRY int32   = 32
)

type DictionaryBuilder struct {
	blockPositionByHash *array.IntBigArray
	elementBlock        block.BlockBuilder
	maxFill             int32
	hashMask            int32
	containsNullElement bool
}

func NewDictionaryBuilder(expectedSize int32) *DictionaryBuilder {
	dr := new(DictionaryBuilder)
	util.CheckArgument2(expectedSize >= 0, "expectedSize must not be negative")

	dr.blockPositionByHash = array.NewIntBigArray()
	expectedEntries := maths.MinInt32(expectedSize, block.DEFAULT_MAX_PAGE_SIZE_IN_BYTES/block.EXPECTED_BYTES_PER_ENTRY)
	dr.elementBlock = block.NewVariableWidthBlockBuilder(nil, expectedEntries, expectedEntries*block.EXPECTED_BYTES_PER_ENTRY)
	dr.elementBlock.AppendNull()
	hashSize := arraySize(expectedSize, DICTIONARY_FILL_RATIO)
	dr.maxFill = calculateMaxFill(hashSize)
	dr.hashMask = hashSize - 1
	dr.blockPositionByHash.EnsureCapacity(int64(hashSize))
	dr.blockPositionByHash.Fill(DICTIONARY_EMPTY_SLOT)
	dr.containsNullElement = false
	return dr
}

func arraySize(expected int32, f float32) int32 {
	s := maths.Max(2, nextPowerOfTwo(int64(math.Ceil(float64(expected)/float64(f)))))
	if s > (1 << 30) {
		panic(fmt.Sprintf("Too large (%d expected elements with load factor %f)", expected, f))
	}
	return int32(s)
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

func (dr *DictionaryBuilder) GetSizeInBytes() int64 {
	return dr.elementBlock.GetSizeInBytes()
}

func (dr *DictionaryBuilder) GetRetainedSizeInBytes() int64 {
	return int64(DICTIONARY_INSTANCE_SIZE) + dr.elementBlock.GetRetainedSizeInBytes() + dr.blockPositionByHash.SizeOf()
}

func (dr *DictionaryBuilder) GetElementBlock() block.Block {
	return dr.elementBlock
}

func (dr *DictionaryBuilder) Clear() {
	dr.containsNullElement = false
	dr.blockPositionByHash.Fill(DICTIONARY_EMPTY_SLOT)
	dr.elementBlock = dr.elementBlock.NewBlockBuilderLike(nil)
	dr.elementBlock.AppendNull()
}

func (dr *DictionaryBuilder) Contains(block block.Block, position int32) bool {
	util.CheckArgument2(position >= 0, "position must be >= 0")
	if block.IsNull(position) {
		return dr.containsNullElement
	} else {
		return dr.blockPositionByHash.Get(dr.getHashPositionOfElement(block, position)) != DICTIONARY_EMPTY_SLOT
	}
}

func (dr *DictionaryBuilder) PutIfAbsent(block block.Block, position int32) int32 {
	if block.IsNull(position) {
		dr.containsNullElement = true
		return DICTIONARY_NULL_POSITION
	}
	var blockPosition int32
	hashPosition := dr.getHashPositionOfElement(block, position)
	if dr.blockPositionByHash.Get(hashPosition) != DICTIONARY_EMPTY_SLOT {
		blockPosition = dr.blockPositionByHash.Get(hashPosition)
	} else {
		blockPosition = dr.addNewElement(hashPosition, block, position)
	}
	util.Verify(blockPosition != DICTIONARY_NULL_POSITION)
	return blockPosition
}

func (dr *DictionaryBuilder) GetEntryCount() int32 {
	return dr.elementBlock.GetPositionCount()
}

func (dr *DictionaryBuilder) getHashPositionOfElement(block block.Block, position int32) int64 {
	util.CheckArgument2(!block.IsNull(position), "position is null")
	length := block.GetSliceLength(position)
	hashPosition := dr.getMaskedHash(block.Hash(position, 0, length))
	for {
		blockPosition := dr.blockPositionByHash.Get(hashPosition)
		if blockPosition == DICTIONARY_EMPTY_SLOT {
			return hashPosition
		}
		if dr.elementBlock.GetSliceLength(blockPosition) == length && block.Equals(position, 0, dr.elementBlock, blockPosition, 0, length) {
			return hashPosition
		}
		hashPosition = dr.getMaskedHash(hashPosition + 1)
	}
}

func (dr *DictionaryBuilder) addNewElement(hashPosition int64, block block.Block, position int32) int32 {
	util.CheckArgument2(!block.IsNull(position), "position is null")
	block.WriteBytesTo(position, 0, block.GetSliceLength(position), dr.elementBlock)
	dr.elementBlock.CloseEntry()
	newElementPositionInBlock := dr.elementBlock.GetPositionCount() - 1
	dr.blockPositionByHash.Set(hashPosition, newElementPositionInBlock)
	if dr.elementBlock.GetPositionCount() >= dr.maxFill {
		dr.rehash(dr.maxFill * 2)
	}
	return newElementPositionInBlock
}

func (dr *DictionaryBuilder) rehash(size int32) {
	newHashSize := arraySize(size+1, DICTIONARY_FILL_RATIO)
	dr.hashMask = newHashSize - 1
	dr.maxFill = calculateMaxFill(newHashSize)
	dr.blockPositionByHash.EnsureCapacity(int64(newHashSize))
	dr.blockPositionByHash.Fill(DICTIONARY_EMPTY_SLOT)
	for blockPosition := int32(1); blockPosition < dr.elementBlock.GetPositionCount(); blockPosition++ {
		dr.blockPositionByHash.Set(dr.getHashPositionOfElement(dr.elementBlock, blockPosition), blockPosition)
	}
}

func calculateMaxFill(hashSize int32) int32 {
	maxFill := int32(math.Ceil(float64(hashSize) * float64(DICTIONARY_FILL_RATIO)))
	if maxFill == hashSize {
		maxFill--
	}
	return maxFill
}

func (dr *DictionaryBuilder) getMaskedHash(rawHash int64) int64 {
	return rawHash & int64(dr.hashMask)
}
