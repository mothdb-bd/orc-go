package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type VariableWidthBlockBuilder struct {
	// 继承
	AbstractVariableWidthBlock
	// 继承
	BlockBuilder

	blockBuilderStatus        *BlockBuilderStatus
	initialized               bool
	initialEntryCount         int32
	initialSliceOutputSize    int32
	sliceOutput               *slice.Slice
	hasNullValue              bool
	valueIsNull               []bool
	offsets                   []int32
	positions                 int32
	currentEntrySize          int32
	arraysRetainedSizeInBytes int64
}

// var variableWidthBlockBuilderiNSTANCE_SIZE int32 = ClassLayout.parseClass(VariableWidthBlockBuilder.class).instanceSize()
var VWBB_INSTANCE_SIZE = util.SizeOf(&VariableWidthBlockBuilder{})

func NewVariableWidthBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytes int32) *VariableWidthBlockBuilder {
	vr := new(VariableWidthBlockBuilder)
	vr.sliceOutput = slice.NewWithSize(0)
	vr.valueIsNull = make([]bool, 0)
	vr.offsets = make([]int32, 0)

	vr.blockBuilderStatus = blockBuilderStatus
	vr.initialEntryCount = expectedEntries
	vr.initialSliceOutputSize = maths.MinInt32(expectedBytes, MAX_ARRAY_SIZE)
	vr.updateArraysDataSize()
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) getPositionOffset(position int32) int32 {
	checkValidPosition(position, vr.positions)
	return vr.getOffset(position)
}

// @Override
func (vr *VariableWidthBlockBuilder) GetSliceLength(position int32) int32 {
	checkValidPosition(position, vr.positions)
	return vr.getOffset((position + 1)) - vr.getOffset(position)
}

// @Override
func (vr *VariableWidthBlockBuilder) getRawSlice(position int32) *slice.Slice {
	// return vr.sliceOutput.getUnderlyingSlice()
	return vr.sliceOutput
}

// @Override
func (vr *VariableWidthBlockBuilder) GetPositionCount() int32 {
	return vr.positions
}

// @Override
func (vr *VariableWidthBlockBuilder) GetSizeInBytes() int64 {
	// arraysSizeInBytes := (Integer.BYTES + Byte.BYTES) * positions.(int64)
	// return sliceOutput.size() + arraysSizeInBytes
	arraysSizeInBytes := (util.INT32_BYTES + util.BYTE_BYTES) * vr.positions
	return int64(vr.sliceOutput.Size()) + int64(arraysSizeInBytes)
}

// @Override
func (vr *VariableWidthBlockBuilder) GetRegionSizeInBytes(positionOffset int32, length int32) int64 {
	positionCount := vr.GetPositionCount()
	checkValidRegion(positionCount, positionOffset, length)
	// arraysSizeInBytes := (Integer.BYTES + Byte.BYTES) * length.(int64)
	arraysSizeInBytes := (util.INT32_BYTES + util.BYTE_BYTES) * length
	return int64(vr.getOffset(positionOffset+length) - vr.getOffset(positionOffset) + arraysSizeInBytes)
}

// @Override
func (vr *VariableWidthBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	checkValidPositions(positions, vr.GetPositionCount())
	sizeInBytes := 0
	usedPositionCount := 0
	for i := 0; i < len(positions); i++ {
		if positions[i] {
			usedPositionCount++
			sizeInBytes += int(vr.getOffset(int32(i)+1) - vr.getOffset(int32(i)))
		}
	}
	// return sizeInBytes + (Integer.BYTES+Byte.BYTES)*usedPositionCount.(int64)
	return int64(sizeInBytes + (util.INT32_BYTES+util.BYTE_BYTES)*usedPositionCount)
}

// @Override
func (vr *VariableWidthBlockBuilder) GetRetainedSizeInBytes() int64 {
	size := int64(VWBB_INSTANCE_SIZE+vr.sliceOutput.GetRetainedSize()) + vr.arraysRetainedSizeInBytes
	if vr.blockBuilderStatus != nil {
		// size += BlockBuilderStatus.INSTANCE_SIZE
		size += int64(BBSINSTANCE_SIZE)
	}
	return size
}

// @Override
func (vr *VariableWidthBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	finalLength := 0
	for i := offset; i < offset+length; i++ {
		finalLength += int(vr.GetSliceLength(positions[i]))
	}
	// newSlice := Slices.allocate(finalLength).getOutput()
	newSlice := slice.NewWithSize(finalLength)
	newOffsets := make([]int32, length+1)
	var newValueIsNull []bool = nil
	if vr.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		if vr.isEntryNull(position) {
			newValueIsNull[i] = true
		} else {
			newSlice.WriteSlice(vr.sliceOutput, int(vr.getPositionOffset(position)), int(vr.GetSliceLength(position)))
		}
		newOffsets[i+1] = int32(newSlice.Size())
	}
	return NewVariableWidthBlock2(0, length, newSlice, newOffsets, newValueIsNull)
}

// @Override
func (vr *VariableWidthBlockBuilder) WriteByte(value byte) BlockBuilder {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	vr.sliceOutput.WriteByte(value)
	vr.currentEntrySize += util.BYTE_BYTES
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) WriteShort(value int16) BlockBuilder {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	vr.sliceOutput.WriteInt16LE(value)
	vr.currentEntrySize += util.INT16_BYTES
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) WriteInt(value int32) BlockBuilder {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	vr.sliceOutput.WriteInt32LE(value)
	vr.currentEntrySize += util.INT32_BYTES
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) WriteLong(value int64) BlockBuilder {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	vr.sliceOutput.WriteInt64LE(value)
	vr.currentEntrySize += util.INT64_BYTES
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) WriteBytes(source *slice.Slice, sourceIndex int32, length int32) BlockBuilder {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	vr.sliceOutput.WriteSlice(source, int(sourceIndex), int(length))
	vr.currentEntrySize += length
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) CloseEntry() BlockBuilder {
	vr.entryAdded(vr.currentEntrySize, false)
	vr.currentEntrySize = 0
	return vr
}

// @Override
func (vr *VariableWidthBlockBuilder) AppendNull() BlockBuilder {
	if vr.currentEntrySize > 0 {
		panic("Current entry must be closed before a null can be written")
	}
	vr.hasNullValue = true
	vr.entryAdded(0, true)
	return vr
}

func (vr *VariableWidthBlockBuilder) entryAdded(bytesWritten int32, isNull bool) {
	if !vr.initialized {
		vr.initializeCapacity()
	}
	if len(vr.valueIsNull) <= int(vr.positions) {
		vr.growCapacity()
	}
	vr.valueIsNull[vr.positions] = isNull
	vr.offsets[vr.positions+1] = int32(vr.sliceOutput.Size())
	vr.positions++
	if vr.blockBuilderStatus != nil {
		vr.blockBuilderStatus.AddBytes(util.BYTE_BYTES + util.INT32_BYTES + bytesWritten)
	}
}

func (vr *VariableWidthBlockBuilder) growCapacity() {
	newSize := calculateNewArraySize(int32(len(vr.valueIsNull)))
	// vr.valueIsNull = Arrays.copyOf(valueIsNull, newSize)
	// vr.offsets = Arrays.copyOf(offsets, newSize+1)
	vr.valueIsNull = util.CopyOfBools(vr.valueIsNull, newSize)
	vr.offsets = util.CopyOfInt32(vr.offsets, newSize+1)
	vr.updateArraysDataSize()
}

func (vr *VariableWidthBlockBuilder) initializeCapacity() {
	if vr.positions != 0 || vr.currentEntrySize != 0 {
		panic("VariableWidthBlockBuilder was used before initialization")
	}
	vr.initialized = true
	vr.valueIsNull = make([]bool, vr.initialEntryCount)
	vr.offsets = make([]int32, vr.initialEntryCount+1)
	// vr.sliceOutput = NewDynamicSliceOutput(vr.initialSliceOutputSize)
	vr.sliceOutput = slice.NewWithSize(int(vr.initialSliceOutputSize))
	vr.updateArraysDataSize()
}

func (vr *VariableWidthBlockBuilder) updateArraysDataSize() {
	vr.arraysRetainedSizeInBytes = int64(util.SizeOf(vr.valueIsNull) + util.SizeOf(vr.offsets))
}

// @Override
func (vr *VariableWidthBlockBuilder) MayHaveNull() bool {
	return vr.hasNullValue
}

// @Override
func (vr *VariableWidthBlockBuilder) isEntryNull(position int32) bool {
	return vr.valueIsNull[position]
}

// @Override
func (vr *VariableWidthBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	positionCount := vr.GetPositionCount()
	checkValidRegion(positionCount, positionOffset, length)
	if vr.hasNullValue {
		return NewVariableWidthBlock2(positionOffset, length, vr.sliceOutput, vr.offsets, vr.valueIsNull)
	} else {
		return NewVariableWidthBlock2(positionOffset, length, vr.sliceOutput, vr.offsets, nil)
	}

}

// @Override
func (vr *VariableWidthBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	positionCount := vr.GetPositionCount()
	checkValidRegion(positionCount, positionOffset, length)
	newOffsets := compactOffsets(vr.offsets, positionOffset, length)
	var newValueIsNull []bool = nil
	if vr.hasNullValue {
		newValueIsNull = compactBoolArray(vr.valueIsNull, positionOffset, length)
	}
	slice := compactSlice(vr.sliceOutput, vr.offsets[positionOffset], newOffsets[length])
	return NewVariableWidthBlock2(0, length, slice, newOffsets, newValueIsNull)
}

// @Override
func (vr *VariableWidthBlockBuilder) Build() Block {
	if vr.currentEntrySize > 0 {
		panic("Current entry must be closed before the block can be built")
	}
	if vr.hasNullValue {
		return NewVariableWidthBlock2(0, vr.positions, vr.sliceOutput, vr.offsets, vr.valueIsNull)
	} else {
		return NewVariableWidthBlock2(0, vr.positions, vr.sliceOutput, vr.offsets, nil)
	}
}

func (vr *VariableWidthBlockBuilder) getOffset(position int32) int32 {
	return vr.offsets[position]
}

// @Override
func (vr *VariableWidthBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	currentSizeInBytes := util.Ternary(vr.positions == 0, vr.positions, (vr.getOffset(vr.positions) - vr.getOffset(0)))
	return NewVariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(vr.positions), calculateBlockResetBytes(currentSizeInBytes))
}

// 所有父类
// @Override
func (ak *VariableWidthBlockBuilder) GetByte(position int32, offset int32) byte {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetByte(int(ak.getPositionOffset(position) + offset))
	if err == nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlockBuilder) GetShort(position int32, offset int32) int16 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt16LE(int(ak.getPositionOffset(position) + offset))
	if err == nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlockBuilder) GetInt(position int32, offset int32) int32 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt32LE(int(ak.getPositionOffset(position) + offset))
	if err == nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlockBuilder) GetLong(position int32, offset int32) int64 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt64LE(int(ak.getPositionOffset(position) + offset))
	if err != nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlockBuilder) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	ak.checkReadablePosition(position)
	s, err := ak.getRawSlice(position).MakeSlice(int(ak.getPositionOffset(position)+offset), int(length))
	if err != nil {
		panic(err.Error())
	}
	return s
}

// @Override
func (ak *VariableWidthBlockBuilder) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	rawSlice := ak.getRawSlice(position)
	if ak.GetSliceLength(position) < length {
		return false
	}
	return otherBlock.BytesEqual(otherPosition, otherOffset, rawSlice, int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlockBuilder) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	return ak.getRawSlice(position).Equal2(int(ak.getPositionOffset(position)+offset), otherSlice, int(otherOffset), int(length))
}

// @Override
func (ak *VariableWidthBlockBuilder) Hash(position int32, offset int32, length int32) int64 {
	ak.checkReadablePosition(position)
	// return XxHash64.hash(ak.getRawSlice(position), ak.getPositionOffset(position)+offset, length)
	return int64(ak.getRawSlice(position).HashCodeValue(int(ak.getPositionOffset(position)+offset), int(length)))
}

// @Override
func (ak *VariableWidthBlockBuilder) CompareTo(position int32, offset int32, length int32, otherBlock Block, otherPosition int32, otherOffset int32, otherLength int32) int32 {
	ak.checkReadablePosition(position)
	rawSlice := ak.getRawSlice(position)
	if ak.GetSliceLength(position) < length {
		panic("Length longer than value length")
	}
	return -otherBlock.BytesCompare(otherPosition, otherOffset, otherLength, rawSlice, int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlockBuilder) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	ak.checkReadablePosition(position)
	return int32(ak.getRawSlice(position).CompareTo2(int(ak.getPositionOffset(position)+offset), int(length), otherSlice, int(otherOffset), int(otherLength)))
}

// @Override
func (ak *VariableWidthBlockBuilder) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	ak.checkReadablePosition(position)
	blockBuilder.WriteBytes(ak.getRawSlice(position), int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlockBuilder) GetSingleValueBlock(position int32) Block {
	if ak.IsNull(position) {
		return NewVariableWidthBlock2(0, 1, slice.EMPTY_SLICE, []int32{0, 0}, []bool{true})
	}
	offset := ak.getPositionOffset(position)
	entrySize := ak.GetSliceLength(position)
	// copy := Slices.copyOf(ak.getRawSlice(position), offset, entrySize)
	copy, _ := ak.getRawSlice(position).MakeSlice(int(offset), int(entrySize))
	return NewVariableWidthBlock2(0, 1, copy, []int32{0, int32(copy.Size())}, nil)
}

// @Override
func (ak *VariableWidthBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	if ak.IsNull(position) {
		return 0
	} else {
		return int64(ak.GetSliceLength(position))
	}
}

func (ak *VariableWidthBlockBuilder) checkReadablePosition(position int32) {
	checkValidPosition(position, ak.GetPositionCount())
}

// 继承
func (ak *VariableWidthBlockBuilder) GetLogicalSizeInBytes() int64 {
	return ak.GetSizeInBytes()
}

// 继承
func (ak *VariableWidthBlockBuilder) GetPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)

	return NewDictionaryBlock6(offset, length, ak, positions, false, RandomDictionaryId())
}

func (ak *VariableWidthBlockBuilder) IsLoaded() bool {
	return true
}

func (ak *VariableWidthBlockBuilder) GetLoadedBlock() Block {
	return ak
}

func (ak *VariableWidthBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList[Block]()
}

// @Override
func (ak *VariableWidthBlockBuilder) IsNull(position int32) bool {

	ak.checkReadablePosition(position)
	return ak.isEntryNull(position)
}
