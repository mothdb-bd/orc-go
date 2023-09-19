package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// var dictionaryBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(DictionaryBlock.class).instanceSize() + ClassLayout.parseClass(DictionaryId.class).instanceSize()
var DB_INSTANCE_SIZE int32 = util.SizeOf(&DictionaryBlock{})

type DictionaryBlock struct {
	// 继承
	Block

	positionCount       int32
	dictionary          Block
	idsOffset           int32
	ids                 []int32
	retainedSizeInBytes int64
	sizeInBytes         int64
	logicalSizeInBytes  int64
	uniqueIds           int32
	isSequentialIds     bool
	dictionarySourceId  *DictionaryId
	mayHaveNull         bool
}

func NewDictionaryBlock(dictionary Block, ids []int32) *DictionaryBlock {
	return NewDictionaryBlock2(int32(len(ids)), dictionary, ids)
}
func NewDictionaryBlock2(positionCount int32, dictionary Block, ids []int32) *DictionaryBlock {
	return NewDictionaryBlock6(0, positionCount, dictionary, ids, false, RandomDictionaryId())
}
func NewDictionaryBlock3(positionCount int32, dictionary Block, ids []int32, dictionaryId *DictionaryId) *DictionaryBlock {
	return NewDictionaryBlock6(0, positionCount, dictionary, ids, false, dictionaryId)
}
func NewDictionaryBlock4(positionCount int32, dictionary Block, ids []int32, dictionaryIsCompacted bool) *DictionaryBlock {
	return NewDictionaryBlock6(0, positionCount, dictionary, ids, dictionaryIsCompacted, RandomDictionaryId())
}
func NewDictionaryBlock5(positionCount int32, dictionary Block, ids []int32, dictionaryIsCompacted bool, dictionarySourceId *DictionaryId) *DictionaryBlock {
	return NewDictionaryBlock6(0, positionCount, dictionary, ids, dictionaryIsCompacted, dictionarySourceId)
}
func NewDictionaryBlock6(idsOffset int32, positionCount int32, dictionary Block, ids []int32, dictionaryIsCompacted bool, dictionarySourceId *DictionaryId) *DictionaryBlock {
	return NewDictionaryBlock7(idsOffset, positionCount, dictionary, ids, dictionaryIsCompacted, false, dictionarySourceId)
}
func NewDictionaryBlock7(idsOffset int32, positionCount int32, dictionary Block, ids []int32, dictionaryIsCompacted bool, isSequentialIds bool, dictionarySourceId *DictionaryId) *DictionaryBlock {
	dk := new(DictionaryBlock)
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	dk.idsOffset = idsOffset
	if int32(len(ids))-idsOffset < positionCount {
		panic("ids length is less than positionCount")
	}
	dk.positionCount = positionCount
	dk.dictionary = dictionary
	dk.ids = ids
	dk.dictionarySourceId = dictionarySourceId
	dk.retainedSizeInBytes = int64(DB_INSTANCE_SIZE + util.SizeOf(ids))
	dk.mayHaveNull = positionCount > 0 && (!dictionary.IsLoaded() || dictionary.MayHaveNull())
	if dictionaryIsCompacted {
		_, flag := dictionary.(*DictionaryBlock)
		if flag {
			panic("compacted dictionary should not have dictionary base block")
		}
		dk.sizeInBytes = dictionary.GetSizeInBytes() + int64(util.INT32_BYTES*positionCount)
		dk.uniqueIds = dictionary.GetPositionCount()
	}
	if isSequentialIds && !dictionaryIsCompacted {
		panic("sequential ids flag is only valid for compacted dictionary")
	}
	dk.isSequentialIds = isSequentialIds
	return dk
}

func (dk *DictionaryBlock) getRawIds() []int32 {
	return dk.ids
}

func (dk *DictionaryBlock) getRawIdsOffset() int32 {
	return dk.idsOffset
}

// @Override
func (dk *DictionaryBlock) GetSliceLength(position int32) int32 {
	return dk.dictionary.GetSliceLength(dk.GetId(position))
}

// @Override
func (dk *DictionaryBlock) GetByte(position int32, offset int32) byte {
	return dk.dictionary.GetByte(dk.GetId(position), offset)
}

// @Override
func (dk *DictionaryBlock) GetShort(position int32, offset int32) int16 {
	return dk.dictionary.GetShort(dk.GetId(position), offset)
}

// @Override
func (dk *DictionaryBlock) GetInt(position int32, offset int32) int32 {
	return dk.dictionary.GetInt(dk.GetId(position), offset)
}

// @Override
func (dk *DictionaryBlock) GetLong(position int32, offset int32) int64 {
	return dk.dictionary.GetLong(dk.GetId(position), offset)
}

// @Override
func (dk *DictionaryBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return dk.dictionary.GetSlice(dk.GetId(position), offset, length)
}

// @Override
func (dk *DictionaryBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	return dk.dictionary.GetObject(dk.GetId(position), clazz)
}

// @Override
func (dk *DictionaryBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return dk.dictionary.BytesEqual(dk.GetId(position), offset, otherSlice, otherOffset, length)
}

// @Override
func (dk *DictionaryBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return dk.dictionary.BytesCompare(dk.GetId(position), offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (dk *DictionaryBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	dk.dictionary.WriteBytesTo(dk.GetId(position), offset, length, blockBuilder)
}

// @Override
func (dk *DictionaryBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return dk.dictionary.Equals(dk.GetId(position), offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (dk *DictionaryBlock) Hash(position int32, offset int32, length int32) int64 {
	return dk.dictionary.Hash(dk.GetId(position), offset, length)
}

// @Override
func (dk *DictionaryBlock) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return dk.dictionary.CompareTo(dk.GetId(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (dk *DictionaryBlock) GetSingleValueBlock(position int32) Block {
	return dk.dictionary.GetSingleValueBlock(dk.GetId(position))
}

// @Override
func (dk *DictionaryBlock) GetPositionCount() int32 {
	return dk.positionCount
}

// @Override
func (dk *DictionaryBlock) GetSizeInBytes() int64 {
	if dk.sizeInBytes == -1 {
		dk.calculateCompactSize()
	}
	return dk.sizeInBytes
}

func (dk *DictionaryBlock) calculateCompactSize() {
	var uniqueIds int32 = 0
	used := make([]bool, dk.dictionary.GetPositionCount())
	var previousPosition int32 = -1
	isSequentialIds := true
	var i int32
	for i = 0; i < dk.positionCount; i++ {
		position := dk.GetId(i)
		uniqueIds += util.Ternary(used[position], int32(0), 1)
		used[position] = true
		isSequentialIds = isSequentialIds && previousPosition < position
		previousPosition = position
	}
	var dictionaryBlockSize int64

	dblock, flag := dk.dictionary.(*DictionaryBlock)
	if flag {
		nestedDictionary := dblock
		if uniqueIds == dk.dictionary.GetPositionCount() {
			dictionaryBlockSize = nestedDictionary.getCompactedDictionarySizeInBytes()
		} else {
			dictionaryBlockSize = nestedDictionary.getCompactedDictionaryPositionsSizeInBytes(used)
		}
		isSequentialIds = false
	} else {
		if uniqueIds == dk.dictionary.GetPositionCount() {
			dictionaryBlockSize = dk.dictionary.GetSizeInBytes()
		} else {
			dictionaryBlockSize = dk.dictionary.GetPositionsSizeInBytes2(used, uniqueIds)
		}
	}
	dk.sizeInBytes = dictionaryBlockSize + int64(util.BYTE_BYTES*dk.positionCount)
	dk.uniqueIds = uniqueIds
	dk.isSequentialIds = isSequentialIds
}

func (dk *DictionaryBlock) getCompactedDictionarySizeInBytes() int64 {
	if dk.sizeInBytes == -1 {
		dk.calculateCompactSize()
	}
	return dk.sizeInBytes - int64(util.BYTE_BYTES*dk.positionCount)
}

func (dk *DictionaryBlock) getCompactedDictionaryPositionsSizeInBytes(positions []bool) int64 {
	used := make([]bool, dk.dictionary.GetPositionCount())

	for i := 0; i < len(positions); i++ {
		if positions[i] {
			used[dk.GetId(int32(i))] = true
		}
	}
	dblock, flag := dk.dictionary.(*DictionaryBlock)
	if flag {
		return dblock.getCompactedDictionaryPositionsSizeInBytes(used)
	}
	return dk.dictionary.GetPositionsSizeInBytes(used)
}

// @Override
func (dk *DictionaryBlock) GetLogicalSizeInBytes() int64 {
	if dk.logicalSizeInBytes >= 0 {
		return dk.logicalSizeInBytes
	}
	var sizeInBytes int64 = 0
	seenSizes := make([]int64, dk.dictionary.GetPositionCount())
	util.FillInt64s(seenSizes, -1)

	var i int32
	for i = 0; i < dk.GetPositionCount(); i++ {
		position := dk.GetId(i)
		if seenSizes[position] < 0 {
			seenSizes[position] = dk.dictionary.GetRegionSizeInBytes(position, 1)
		}
		sizeInBytes += seenSizes[position]
	}
	dk.logicalSizeInBytes = sizeInBytes
	return sizeInBytes
}

// @Override
func (dk *DictionaryBlock) GetRegionSizeInBytes(positionOffset int32, length int32) int64 {
	if positionOffset == 0 && length == dk.GetPositionCount() {
		return dk.GetSizeInBytes()
	}
	used := make([]bool, dk.dictionary.GetPositionCount())
	for i := positionOffset; i < positionOffset+length; i++ {
		used[dk.GetId(i)] = true
	}

	return dk.dictionary.GetPositionsSizeInBytes(used) + int64(util.BYTE_BYTES*length)
}

// @Override
func (dk *DictionaryBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	checkValidPositions(positions, dk.positionCount)
	used := make([]bool, dk.dictionary.GetPositionCount())
	var i int32
	for i = 0; i < int32(len(positions)); i++ {
		if positions[i] {
			used[dk.GetId(i)] = true
		}
	}
	return dk.dictionary.GetPositionsSizeInBytes(used) + int64(util.BYTE_BYTES*countUsedPositions(positions))
}

// @Override
func (dk *DictionaryBlock) GetRetainedSizeInBytes() int64 {
	return dk.retainedSizeInBytes + dk.dictionary.GetRetainedSizeInBytes()
}

// @Override
func (dk *DictionaryBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return dk.dictionary.GetEstimatedDataSizeForStats(dk.GetId(position))
}

// @Override
func (dk *DictionaryBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)

	_, flag := dk.dictionary.(*DictionaryBlock)
	if length <= 1 || flag || dk.uniqueIds == dk.positionCount {
		positionsToCopy := make([]int32, length)

		var i int32
		for i = 0; i < length; i++ {
			positionsToCopy[i] = dk.GetId(positions[offset+i])
		}
		return dk.dictionary.CopyPositions(positionsToCopy, 0, length)
	}
	positionsToCopy := NewIntArrayList2()
	oldIndexToNewIndex := NewInt2IntOpenHashMap(maths.MinInt32(length, dk.dictionary.GetPositionCount()))
	newIds := make([]int32, length)
	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		oldIndex := dk.GetId(position)
		newId := oldIndexToNewIndex.PutIfAbsent(oldIndex, positionsToCopy.Size())
		if newId == -1 {
			newId = positionsToCopy.Size()
			positionsToCopy.Add(oldIndex)
		}
		newIds[i] = newId
	}
	compactDictionary := dk.dictionary.CopyPositions(positionsToCopy.Elements(), 0, positionsToCopy.Size())
	if positionsToCopy.Size() == length {
		return compactDictionary
	}
	return NewDictionaryBlock7(0, length, compactDictionary, newIds, true, false, RandomDictionaryId())
}

// @Override
func (dk *DictionaryBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(dk.positionCount, positionOffset, length)
	if length == dk.positionCount {
		return dk
	}
	return NewDictionaryBlock6(dk.idsOffset+positionOffset, length, dk.dictionary, dk.ids, false, dk.dictionarySourceId)
}

// @Override
func (dk *DictionaryBlock) CopyRegion(position int32, length int32) Block {
	checkValidRegion(dk.positionCount, position, length)
	uniqueIds := dk.uniqueIds
	if length <= 1 || (uniqueIds == dk.dictionary.GetPositionCount() && dk.isSequentialIds) {
		return dk.dictionary.CopyRegion(dk.GetId(position), length)
	}
	_, flag := dk.dictionary.(*DictionaryBlock)
	if flag || uniqueIds == dk.positionCount {
		return dk.dictionary.CopyPositions(dk.ids, dk.idsOffset+position, length)
	}
	newIds := util.CopyInt32sOfRange(dk.ids, dk.idsOffset+position, dk.idsOffset+position+length)
	dictionaryBlock := NewDictionaryBlock(dk.dictionary, newIds)
	return dictionaryBlock.Compact()
}

// @Override
func (dk *DictionaryBlock) MayHaveNull() bool {
	return dk.mayHaveNull && dk.dictionary.MayHaveNull()
}

// @Override
func (dk *DictionaryBlock) IsNull(position int32) bool {
	if !dk.mayHaveNull {
		return false
	}
	checkValidPosition(position, dk.positionCount)
	return dk.dictionary.IsNull(dk.getIdUnchecked(position))
}

// @Override
func (dk *DictionaryBlock) GetPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	newIds := make([]int32, length)
	isCompact := length >= dk.dictionary.GetPositionCount() && dk.IsCompact()
	var seen []bool = nil
	if isCompact {
		seen = make([]bool, dk.dictionary.GetPositionCount())
	}
	var i int32
	for i = 0; i < length; i++ {
		newIds[i] = dk.GetId(positions[offset+i])
		if isCompact {
			seen[newIds[i]] = true
		}
	}
	for i = 0; i < dk.dictionary.GetPositionCount() && isCompact; i++ {
		// isCompact = isCompact & seen[i]
		isCompact = isCompact && seen[i]
	}
	return NewDictionaryBlock5(int32(len(newIds)), dk.GetDictionary(), newIds, isCompact, dk.GetDictionarySourceId())
}

// @Override
func (dk *DictionaryBlock) IsLoaded() bool {
	return dk.dictionary.IsLoaded()
}

// @Override
func (dk *DictionaryBlock) GetLoadedBlock() Block {
	loadedDictionary := dk.dictionary.GetLoadedBlock()
	if loadedDictionary == dk.dictionary {
		return dk
	}
	return NewDictionaryBlock6(dk.idsOffset, dk.GetPositionCount(), loadedDictionary, dk.ids, false, RandomDictionaryId())
}

// @Override
func (dk *DictionaryBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(dk.GetDictionary())
}

func (dk *DictionaryBlock) GetDictionary() Block {
	return dk.dictionary
}

func (dk *DictionaryBlock) IsSequentialIds() bool {
	if dk.uniqueIds == -1 {
		dk.calculateCompactSize()
	}
	return dk.isSequentialIds
}

func (dk *DictionaryBlock) GetId(position int32) int32 {
	checkValidPosition(position, dk.positionCount)
	return dk.getIdUnchecked(position)
}

func (dk *DictionaryBlock) getIdUnchecked(position int32) int32 {
	return dk.ids[position+dk.idsOffset]
}

func (dk *DictionaryBlock) GetDictionarySourceId() *DictionaryId {
	return dk.dictionarySourceId
}

func (dk *DictionaryBlock) IsCompact() bool {
	_, flag := dk.dictionary.(*DictionaryBlock)
	if flag {
		return false
	}
	if dk.uniqueIds == -1 {
		dk.calculateCompactSize()
	}
	return dk.uniqueIds == dk.dictionary.GetPositionCount()
}

func (dk *DictionaryBlock) Compact() *DictionaryBlock {
	if dk.IsCompact() {
		return dk
	}
	unnested := dk.unnest()
	if unnested != dk {
		return unnested.Compact()
	}
	dictionarySize := dk.dictionary.GetPositionCount()
	dictionaryPositionsToCopy := NewIntArrayList(maths.MinInt32(dictionarySize, dk.positionCount))
	remapIndex := make([]int32, dictionarySize)

	util.FillInt32s(remapIndex, -1)
	var newIndex int32 = 0
	var i int32
	for i = 0; i < dk.positionCount; i++ {
		dictionaryIndex := dk.GetId(i)
		if remapIndex[dictionaryIndex] == -1 {
			dictionaryPositionsToCopy.Add(dictionaryIndex)
			remapIndex[dictionaryIndex] = newIndex
			newIndex++
		}
	}
	if dictionaryPositionsToCopy.Size() == dictionarySize {
		return dk
	}
	newIds := make([]int32, dk.positionCount)

	for i = 0; i < dk.positionCount; i++ {
		newId := remapIndex[dk.GetId(i)]
		if newId == -1 {
			panic("reference to a non-existent key")
		}
		newIds[i] = newId
	}
	compactDictionary := dk.dictionary.CopyPositions(dictionaryPositionsToCopy.Elements(), 0, dictionaryPositionsToCopy.Size())
	return NewDictionaryBlock7(0, dk.positionCount, compactDictionary, newIds, true, dk.uniqueIds == dk.positionCount, RandomDictionaryId())
}

func (dk *DictionaryBlock) unnest() *DictionaryBlock {
	_, flag := dk.dictionary.(*DictionaryBlock)
	if !(flag) {
		return dk
	}
	ids := make([]int32, dk.positionCount)
	var i int32
	for i = 0; i < dk.positionCount; i++ {
		ids[i] = dk.GetId(i)
	}
	dictionary := dk.dictionary
	_, f := dictionary.(*DictionaryBlock)
	for f {
		nestedDictionary := dictionary.(*DictionaryBlock)

		for i = 0; i < dk.positionCount; i++ {
			ids[i] = nestedDictionary.GetId(ids[i])
		}
		dictionary = nestedDictionary.GetDictionary()

		_, f = dictionary.(*DictionaryBlock)
	}
	return NewDictionaryBlock(dictionary, ids)
}
