package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BOOLEAN *BooleanType = NewBooleanType()

type BooleanType struct {
	// 继承 FixedWidthType
	FixedWidthType

	// 继承
	AbstractType
}

func WrapByteArrayAsBooleanBlockWithoutNulls(booleansAsBytes []byte) Block {
	return NewByteArrayBlock(int32(len(booleansAsBytes)), optional.Empty[[]bool](), booleansAsBytes)
}

func CreateBlockForSingleNonNullValue(value bool) Block {
	var byteValue byte = 0
	if value {
		byteValue = 1
	}
	return NewByteArrayBlock(1, optional.Empty[[]bool](), []byte{byteValue})
}

func NewBooleanType() *BooleanType {
	be := new(BooleanType)

	// 初始化父类变量
	be.signature = NewTypeSignature(ST_BOOLEAN)
	be.goKind = reflect.Bool
	be.AbstractType = *NewAbstractType(be.signature, be.goKind)
	return be
}

func (be *BooleanType) GetBaseName() string {
	return be.AbstractType.GetBaseName()
}

// @Override
func (be *BooleanType) GetFixedSize() int32 {
	return util.BYTE_BYTES
}

// @Override
func (be *BooleanType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}

	return NewByteArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.BYTE_BYTES))
}

// @Override
func (be *BooleanType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return be.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.BYTE_BYTES)
}

// @Override
func (be *BooleanType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewByteArrayBlockBuilder(nil, positionCount)
}

// @Override
func (be *BooleanType) IsComparable() bool {
	return true
}

// @Override
func (be *BooleanType) IsOrderable() bool {
	return true
}

// @Override
func (be *BooleanType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteByte(block.GetByte(position, 0))
		blockBuilder.CloseEntry()
	}

}

// @Override
func (be *BooleanType) GetBoolean(block Block, position int32) bool {
	return block.GetByte(position, 0) != 0
}

// @Override
func (be *BooleanType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	if value {
		blockBuilder.WriteByte(1)
	} else {
		blockBuilder.WriteByte(0)
	}

	blockBuilder.CloseEntry()
}

// 继承Type
// @Override
func (te *BooleanType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *BooleanType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *BooleanType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *BooleanType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *BooleanType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *BooleanType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *BooleanType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *BooleanType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *BooleanType) GetSlice(block Block, position int32) *slice.Slice {
	// return block.getSlice(position, 0, block.getSliceLength(position))
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *BooleanType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *BooleanType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *BooleanType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *BooleanType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *BooleanType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *BooleanType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
