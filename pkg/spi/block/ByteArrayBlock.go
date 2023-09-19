package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ByteArrayBlock struct {
	Block

	arrayOffset         int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	values              []byte
	sizeInBytes         int64
	retainedSizeInBytes int64
}

// var byteArrayBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(ByteArrayBlock.class).instanceSize()
var BYTE_ARRAY_INSTANCE_SIZE int32 = util.SizeOf(&ByteArrayBlock{})

func NewByteArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], values []byte) *ByteArrayBlock {
	return NewByteArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), values)
}
func NewByteArrayBlock2(arrayOffset int32, positionCount int32, valueIsNull []bool, values []byte) *ByteArrayBlock {
	bk := new(ByteArrayBlock)
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	bk.arrayOffset = arrayOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	bk.positionCount = positionCount
	if int32(len(values))-arrayOffset < positionCount {
		panic("values length is less than positionCount")
	}
	bk.values = values
	if valueIsNull != nil && int32(len(valueIsNull))-arrayOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	bk.valueIsNull = valueIsNull
	// bk.sizeInBytes = (Byte.BYTES + Byte.BYTES) * positionCount.(int64)
	bk.sizeInBytes = int64((util.INT32_BYTES + util.BYTE_BYTES) * positionCount)
	bk.retainedSizeInBytes = int64(BYTE_ARRAY_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(values))
	return bk
}

// @Override
func (bk *ByteArrayBlock) GetSizeInBytes() int64 {
	return bk.sizeInBytes
}

// @Override
func (bk *ByteArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Byte.BYTES + Byte.BYTES) * length.(int64)
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (bk *ByteArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return bk.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (bk *ByteArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	// return (Byte.BYTES + Byte.BYTES).(int64) * selectedPositionsCount
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * selectedPositionsCount)
}

// @Override
func (bk *ByteArrayBlock) GetRetainedSizeInBytes() int64 {
	return bk.retainedSizeInBytes
}

// @Override
func (bk *ByteArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.If(bk.IsNull(position), 0, util.BYTE_BYTES).(int64)
}

// @Override
func (bk *ByteArrayBlock) GetPositionCount() int32 {
	return bk.positionCount
}

// @Override
func (bk *ByteArrayBlock) GetByte(position int32, offset int32) byte {
	bk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return bk.values[position+bk.arrayOffset]
}

// @Override
func (bk *ByteArrayBlock) MayHaveNull() bool {
	return bk.valueIsNull != nil
}

// @Override
func (bk *ByteArrayBlock) IsNull(position int32) bool {
	bk.checkReadablePosition(position)
	return bk.valueIsNull != nil && bk.valueIsNull[position+bk.arrayOffset]
}

// @Override
func (bk *ByteArrayBlock) GetSingleValueBlock(position int32) Block {
	bk.checkReadablePosition(position)
	values := []byte{}

	if bk.IsNull(position) {
		return NewByteArrayBlock2(0, 1, []bool{true}, values)
	} else {
		return NewByteArrayBlock2(0, 1, nil, values)
	}

}

// @Override
func (bk *ByteArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if bk.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]byte, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		bk.checkReadablePosition(position)
		if bk.valueIsNull != nil {
			newValueIsNull[i] = bk.valueIsNull[position+bk.arrayOffset]
		}
		newValues[i] = bk.values[position+bk.arrayOffset]
	}
	return NewByteArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (bk *ByteArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(bk.GetPositionCount(), positionOffset, length)
	return NewByteArrayBlock2(positionOffset+bk.arrayOffset, length, bk.valueIsNull, bk.values)
}

// @Override
func (bk *ByteArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(bk.GetPositionCount(), positionOffset, length)
	positionOffset += bk.arrayOffset

	var newValueIsNull []bool = nil
	if bk.valueIsNull != nil {
		newValueIsNull = compactBoolArray(bk.valueIsNull, positionOffset, length)
	}
	newValues := compactByteArray(bk.values, positionOffset, length)
	if reflect.DeepEqual(newValueIsNull, bk.valueIsNull) && reflect.DeepEqual(newValues, bk.values) {
		return bk
	}
	return NewByteArrayBlock2(0, length, newValueIsNull, newValues)
}

func (bk *ByteArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= bk.GetPositionCount() {
		panic("position is not valid")
	}
}

// @Override
func (ak *ByteArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}

// @Override
func (ak *ByteArrayBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	return nil
}

func (ak *ByteArrayBlock) GetLoadedBlock() Block {
	return ak
}

func (ik *ByteArrayBlock) IsLoaded() bool {
	return true
}
