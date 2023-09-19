package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BIGINT *BigintType = NewBigintType()

type BigintType struct {
	// // 继承
	AbstractLongType
}

func NewBigintType() *BigintType {
	be := new(BigintType)
	be.signature = NewTypeSignature(ST_BIGINT)
	be.goKind = reflect.Int64
	be.AbstractType = *NewAbstractType(be.signature, be.goKind)
	return be
}

// @Override
// @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
func (te *BigintType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}

// 继承Type
// @Override
func (te *BigintType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *BigintType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *BigintType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *BigintType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *BigintType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *BigintType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *BigintType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *BigintType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *BigintType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *BigintType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *BigintType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *BigintType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *BigintType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *BigintType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (ae *BigintType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (ae *BigintType) IsComparable() bool {
	return true
}

// @Override
func (ae *BigintType) IsOrderable() bool {
	return true
}

// @Override
func (ae *BigintType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (ae *BigintType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, ae.GetFixedSize())
}

// @Override
func (ae *BigintType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (ae *BigintType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ae *BigintType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}

	// return NewLongArrayBlockBuilder(blockBuilderStatus, Math.min(expectedEntries, maxBlockSizeInBytes/Long.BYTES))
	newBB := NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT64_BYTES))
	return newBB
}

// @Override
func (ae *BigintType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	// return createBlockBuilder(blockBuilderStatus, expectedEntries, Long.BYTES)
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT32_BYTES)
}

// @Override
func (ae *BigintType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}
