package block

import (
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DoubleType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType
}

var DOUBLE *DoubleType = NewDoubleType()

func NewDoubleType() *DoubleType {
	de := new(DoubleType)
	de.signature = NewTypeSignature(ST_DOUBLE)
	de.goKind = reflect.Float64
	de.AbstractType = *NewAbstractType(de.signature, de.goKind)
	return de
}

// @Override
func (de *DoubleType) GetFixedSize() int32 {
	return util.FLOAT64_BYTES
}

// @Override
func (de *DoubleType) IsComparable() bool {
	return true
}

// @Override
func (de *DoubleType) IsOrderable() bool {
	return true
}

// @Override
func (de *DoubleType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0)).CloseEntry()
	}
}

// @Override
func (de *DoubleType) GetDouble(block Block, position int32) float64 {
	return math.Float64frombits(uint64(block.GetLong(position, 0)))
}

// @Override
func (de *DoubleType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	blockBuilder.WriteLong(int64(math.Float64bits(value))).CloseEntry()
}

// @Override
func (de *DoubleType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewLongArrayBlockBuilder(blockBuilderStatus, maths.MaxInt32(expectedEntries, maxBlockSizeInBytes/util.FLOAT64_BYTES))
}

// @Override
func (de *DoubleType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return de.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.FLOAT64_BYTES)
}

// @Override
func (de *DoubleType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}

// @Override
// @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
func (de *DoubleType) Equals(other Type) bool {
	return basic.ObjectEqual(other, DOUBLE)
}

// 继承Type
// @Override
func (te *DoubleType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *DoubleType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *DoubleType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *DoubleType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *DoubleType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *DoubleType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *DoubleType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

func (te *DoubleType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *DoubleType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *DoubleType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *DoubleType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *DoubleType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *DoubleType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *DoubleType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *DoubleType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}
