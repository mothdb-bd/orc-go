package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var REAL *RealType = newRealType()

type RealType struct {
	// 继承
	AbstractIntType
}

func newRealType() *RealType {
	re := new(RealType)
	re.signature = NewTypeSignature(ST_REAL)
	re.goKind = reflect.Int64
	// re.ait = NewAbstractIntType()
	re.AbstractType = *NewAbstractType(re.signature, re.goKind)
	return re
}

// @Override
func (re *RealType) WriteLong(blockBuilder BlockBuilder, value int64) {
	var floatValue int32
	floatValue, e := util.ToInt32Exact(value)
	if e != nil {
		panic(e.Error)
	}
	blockBuilder.WriteInt(floatValue).CloseEntry()
}

// @Override
func (re *RealType) Equals(other Type) bool {
	return basic.ObjectEqual(other, REAL)
}

// 继承Type

// @Override
func (ae *RealType) GetFixedSize() int32 {
	return util.INT32_BYTES
}

// @Override
func (ae *RealType) IsComparable() bool {
	return true
}

// @Override
func (ae *RealType) IsOrderable() bool {
	return true
}

// @Override
func (ae *RealType) GetLong(block Block, position int32) int64 {
	return int64(block.GetInt(position, 0))
}

// @Override
func (ae *RealType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, ae.GetFixedSize())
}

// @Override
func (ae *RealType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteInt(block.GetInt(position, 0))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ae *RealType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}

	// return block.NewIntArrayBlockBuilder(blockBuilderStatus, Math.min(expectedEntries, maxBlockSizeInBytes/Integer.BYTES))
	newBB := NewIntArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT32_BYTES))
	return newBB
}

// @Override
func (ae *RealType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	// return createBlockBuilder(blockBuilderStatus, expectedEntries, Integer.BYTES)
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, 4)
}

// @Override
func (ae *RealType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewIntArrayBlockBuilder(nil, positionCount)
}

// 继承Type
// @Override
func (te *RealType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *RealType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *RealType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *RealType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *RealType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *RealType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *RealType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *RealType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *RealType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *RealType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *RealType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *RealType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *RealType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *RealType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}
