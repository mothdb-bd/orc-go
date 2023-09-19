package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ByteArrayBlockBuilder struct { //@Nullable
	// 继承 BlockBuilder
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	values              []byte
	retainedSizeInBytes int64
}

// var (
// 	byteArrayBlockBuilderiNSTANCE_SIZE    int32  = ClassLayout.parseClass(ByteArrayBlockBuilder.class).instanceSize()
// 	byteArrayBlockBuildernULL_VALUE_BLOCK Block = NewByteArrayBlock(0, 1, make([]bool), make([]byte, 1))
// )

var (
	BYTE_ARRAY_BB_INSTANCE_SIZE    int32 = util.SizeOf(&ByteArrayBlockBuilder{})
	BYTE_ARRAY_BB_UULL_VALUE_BLOCK Block = NewByteArrayBlock2(0, 1, []bool{true}, make([]byte, 1))
)

func NewByteArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *ByteArrayBlockBuilder {
	br := new(ByteArrayBlockBuilder)

	br.valueIsNull = make([]bool, 0)
	br.values = make([]byte, 0)

	br.blockBuilderStatus = blockBuilderStatus
	br.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	br.updateDataSize()
	return br
}

// @Override
func (br *ByteArrayBlockBuilder) WriteByte(value byte) BlockBuilder {
	if util.BytesLenInt32(br.values) <= br.positionCount {
		br.growCapacity()
	}
	br.values[br.positionCount] = value
	br.hasNonNullValue = true
	br.positionCount++
	if br.blockBuilderStatus != nil {
		br.blockBuilderStatus.AddBytes(BYTE_BYTE_BYTES)
	}
	return br
}

// @Override
func (br *ByteArrayBlockBuilder) CloseEntry() BlockBuilder {
	return br
}

// @Override
func (br *ByteArrayBlockBuilder) AppendNull() BlockBuilder {
	if util.BytesLenInt32(br.values) <= br.positionCount {
		br.growCapacity()
	}
	br.valueIsNull[br.positionCount] = true
	br.hasNullValue = true
	br.positionCount++
	if br.blockBuilderStatus != nil {
		br.blockBuilderStatus.AddBytes(BYTE_BYTE_BYTES)
	}
	return br
}

// @Override
func (br *ByteArrayBlockBuilder) Build() Block {
	if !br.hasNonNullValue {
		return NewRunLengthEncodedBlock(BYTE_ARRAY_BB_UULL_VALUE_BLOCK, br.positionCount)
	}
	if br.hasNullValue {
		return NewByteArrayBlock2(0, br.positionCount, br.valueIsNull, br.values)
	} else {
		return NewByteArrayBlock2(0, br.positionCount, nil, br.values)
	}
}

// @Override
func (br *ByteArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewByteArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(br.positionCount))
}

func (br *ByteArrayBlockBuilder) growCapacity() {
	var newSize int32
	if br.initialized {
		newSize = calculateNewArraySize(util.BytesLenInt32(br.values))
	} else {
		newSize = br.initialEntryCount
		br.initialized = true
	}
	br.valueIsNull = util.CopyOfBools(br.valueIsNull, newSize)
	br.values = util.CopyOfBytes(br.values, newSize)
	br.updateDataSize()
}

func (br *ByteArrayBlockBuilder) updateDataSize() {
	br.retainedSizeInBytes = int64(BYTE_ARRAY_BB_INSTANCE_SIZE + util.SizeOf(br.valueIsNull) + util.SizeOf(br.values))
	if br.blockBuilderStatus != nil {
		br.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (br *ByteArrayBlockBuilder) GetSizeInBytes() int64 {
	// return (Byte.BYTES + Byte.BYTES) * positionCount.(int64)
	return int64(BYTE_BYTE_BYTES * br.positionCount)
}

// @Override
func (br *ByteArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Byte.BYTES + Byte.BYTES) * length.(int64)
	return int64(BYTE_BYTE_BYTES * length)
}

// @Override
func (br *ByteArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	// return (Byte.BYTES + Byte.BYTES) * countUsedPositions(positions).(int64)
	return int64(BYTE_BYTE_BYTES * countUsedPositions(positions))
}

// @Override
func (br *ByteArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return br.retainedSizeInBytes
}

// @Override
func (br *ByteArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.If(br.IsNull(position), 0, util.BYTE_BYTES).(int64)
}

// @Override
func (br *ByteArrayBlockBuilder) GetPositionCount() int32 {
	return br.positionCount
}

// @Override
func (br *ByteArrayBlockBuilder) GetByte(position int32, offset int32) byte {
	br.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return br.values[position]
}

// @Override
func (br *ByteArrayBlockBuilder) MayHaveNull() bool {
	return br.hasNullValue
}

// @Override
func (br *ByteArrayBlockBuilder) IsNull(position int32) bool {
	br.checkReadablePosition(position)
	return br.valueIsNull[position]
}

// @Override
func (br *ByteArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	br.checkReadablePosition(position)
	if br.valueIsNull[position] {
		return NewByteArrayBlock2(0, 1, []bool{true}, []byte{br.values[position]})
	} else {
		return NewByteArrayBlock2(0, 1, nil, []byte{br.values[position]})
	}
}

// @Override
func (br *ByteArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !br.hasNonNullValue {
		return NewRunLengthEncodedBlock(BYTE_ARRAY_BB_UULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if br.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]byte, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		br.checkReadablePosition(position)
		if br.hasNullValue {
			newValueIsNull[i] = br.valueIsNull[position]
		}
		newValues[i] = br.values[position]
	}
	return NewByteArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (br *ByteArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(br.GetPositionCount(), positionOffset, length)
	if !br.hasNonNullValue {
		return NewRunLengthEncodedBlock(BYTE_ARRAY_BB_UULL_VALUE_BLOCK, length)
	}
	if br.hasNullValue {
		return NewByteArrayBlock2(positionOffset, length, br.valueIsNull, br.values)
	} else {
		return NewByteArrayBlock2(positionOffset, length, nil, br.values)
	}

}

// @Override
func (br *ByteArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(br.GetPositionCount(), positionOffset, length)
	if !br.hasNonNullValue {
		return NewRunLengthEncodedBlock(BYTE_ARRAY_BB_UULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = make([]bool, length)
	if br.hasNullValue {
		// newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset+length)
		util.CopyBools(br.valueIsNull, positionOffset, newValueIsNull, 0, length)
	}
	// newValues := Arrays.copyOfRange(values, positionOffset, positionOffset+length)
	var newValues []byte = make([]byte, length)
	util.CopyBytes(br.values, positionOffset, newValues, 0, length)
	return NewByteArrayBlock2(0, length, newValueIsNull, newValues)
}

func (br *ByteArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= br.GetPositionCount() {
		panic("position is not valid")
	}
}

func (ak *ByteArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}

func (ik *ByteArrayBlockBuilder) IsLoaded() bool {
	return true
}

func (ik *ByteArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
