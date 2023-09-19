package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Int128ArrayBlockBuilder struct { //@Nullable
	// 继承
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	values              []int64
	retainedSizeInBytes int64
	entryPositionCount  int32
}

var (
	INT128_BB_INSTANCE_SIZE int32 = util.SizeOf(&Int128ArrayBlockBuilder{})
	INT128_NULL_VALUE_BLOCK Block = NewInt128ArrayBlock2(0, 1, []bool{true}, make([]int64, 2))
)

func NewInt128ArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *Int128ArrayBlockBuilder {
	ir := new(Int128ArrayBlockBuilder)
	ir.valueIsNull = make([]bool, 0)
	ir.values = make([]int64, 0)

	ir.blockBuilderStatus = blockBuilderStatus
	ir.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	ir.updateDataSize()
	return ir
}

// @Override
func (ir *Int128ArrayBlockBuilder) WriteLong(value int64) BlockBuilder {
	if util.BoolsLenInt32(ir.valueIsNull) <= ir.positionCount {
		ir.growCapacity()
	}
	ir.values[(ir.positionCount*2)+ir.entryPositionCount] = value
	ir.entryPositionCount++
	ir.hasNonNullValue = true
	return ir
}

// @Override
func (ir *Int128ArrayBlockBuilder) CloseEntry() BlockBuilder {
	if ir.entryPositionCount != 2 {
		panic(fmt.Sprintf("Expected entry size to be exactly %d bytes but was %d", INT128_BYTES, (ir.entryPositionCount * util.INT64_BYTES)))
	}
	ir.positionCount++
	ir.entryPositionCount = 0
	if ir.blockBuilderStatus != nil {
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + INT128_BYTES)
	}
	return ir
}

// @Override
func (ir *Int128ArrayBlockBuilder) AppendNull() BlockBuilder {
	if util.BoolsLenInt32(ir.valueIsNull) <= ir.positionCount {
		ir.growCapacity()
	}
	if ir.entryPositionCount != 0 {
		panic("Current entry must be closed before a null can be written")
	}
	ir.valueIsNull[ir.positionCount] = true
	ir.hasNullValue = true
	ir.positionCount++
	if ir.blockBuilderStatus != nil {
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + INT128_BYTES)
	}
	return ir
}

// @Override
func (ir *Int128ArrayBlockBuilder) Build() Block {
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT128_NULL_VALUE_BLOCK, ir.positionCount)
	}
	if ir.hasNullValue {
		return NewInt128ArrayBlock2(0, ir.positionCount, ir.valueIsNull, ir.values)
	} else {
		return NewInt128ArrayBlock2(0, ir.positionCount, nil, ir.values)
	}
}

// @Override
func (ir *Int128ArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewInt128ArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(ir.positionCount))
}

func (ir *Int128ArrayBlockBuilder) growCapacity() {
	var newSize int32
	if ir.initialized {
		newSize = calculateNewArraySize(util.BoolsLenInt32(ir.valueIsNull))
	} else {
		newSize = ir.initialEntryCount
		ir.initialized = true
	}
	ir.valueIsNull = util.CopyOfBools(ir.valueIsNull, newSize)
	ir.values = util.CopyOfInt64s(ir.values, newSize*2)
	ir.updateDataSize()
}

func (ir *Int128ArrayBlockBuilder) updateDataSize() {
	ir.retainedSizeInBytes = int64(INT128_BB_INSTANCE_SIZE + util.SizeOf(ir.valueIsNull) + util.SizeOf(ir.values))
	if ir.blockBuilderStatus != nil {
		ir.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetSizeInBytes() int64 {
	return int64((INT128_BYTES + util.BYTE_BYTES) * ir.positionCount)
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64((INT128_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	return int64((INT128_BYTES + util.BYTE_BYTES) * countUsedPositions(positions))
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return ir.retainedSizeInBytes
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ir.IsNull(position), 0, int64(INT128_BYTES))
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetPositionCount() int32 {
	return ir.positionCount
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetLong(position int32, offset int32) int64 {
	ir.checkReadablePosition(position)
	if offset == 0 {
		return ir.values[position*2]
	}
	if offset == 8 {
		return ir.values[(position*2)+1]
	}
	panic("offset must be 0 or 8")
}

// @Override
func (ir *Int128ArrayBlockBuilder) MayHaveNull() bool {
	return ir.hasNullValue
}

// @Override
func (ir *Int128ArrayBlockBuilder) IsNull(position int32) bool {
	ir.checkReadablePosition(position)
	return ir.valueIsNull[position]
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	ir.checkReadablePosition(position)
	if ir.valueIsNull[position] {
		return NewInt128ArrayBlock2(0, 1, []bool{true}, []int64{ir.values[position*2], ir.values[(position*2)+1]})
	} else {
		return NewInt128ArrayBlock2(0, 1, nil, []int64{ir.values[position*2], ir.values[(position*2)+1]})
	}
}

// @Override
func (ir *Int128ArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT128_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if ir.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int64, length*2)

	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		ir.checkReadablePosition(position)
		if ir.hasNullValue {
			newValueIsNull[i] = ir.valueIsNull[position]
		}
		newValues[i*2] = ir.values[(position * 2)]
		newValues[(i*2)+1] = ir.values[(position*2)+1]
	}
	return NewInt128ArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (ir *Int128ArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT128_NULL_VALUE_BLOCK, length)
	}
	if ir.hasNullValue {
		return NewInt128ArrayBlock2(positionOffset, length, ir.valueIsNull, ir.values)
	} else {
		return NewInt128ArrayBlock2(positionOffset, length, nil, ir.values)
	}
}

// @Override
func (ir *Int128ArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT128_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if ir.hasNullValue {
		newValueIsNull = compactBoolArray(ir.valueIsNull, positionOffset, length)
	}
	newValues := compactInt64Array(ir.values, positionOffset*2, length*2)
	return NewInt128ArrayBlock2(0, length, newValueIsNull, newValues)
}

func (ir *Int128ArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= ir.GetPositionCount() {
		panic("position is not valid")
	}
}

func (ak *Int128ArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}

func (ik *Int128ArrayBlockBuilder) IsLoaded() bool {
	return true
}

func (ik *Int128ArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
