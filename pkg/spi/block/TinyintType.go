package block

import (
	"fmt"
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TinyintType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType
}

func NewTinyintType() *TinyintType {
	te := new(TinyintType)
	te.signature = NewTypeSignature(ST_TINYINT)
	te.goKind = reflect.Int64
	te.AbstractType = *NewAbstractType(te.signature, te.goKind)
	return te
}

var TINYINT *TinyintType = NewTinyintType()

// @Override
func (te *TinyintType) GetFixedSize() int32 {
	// return Byte.BYTES
	return util.BYTE_BYTES
}

// @Override
func (te *TinyintType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewByteArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.BYTE_BYTES))
}

// @Override
func (te *TinyintType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return te.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.BYTE_BYTES)
}

// @Override
func (te *TinyintType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewByteArrayBlockBuilder(nil, positionCount)
}

// @Override
func (te *TinyintType) IsComparable() bool {
	return true
}

// @Override
func (te *TinyintType) IsOrderable() bool {
	return true
}

// @Override
func (te *TinyintType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteByte(block.GetByte(position, 0)).CloseEntry()
	}
}

// @Override
func (te *TinyintType) GetLong(block Block, position int32) int64 {
	return int64(block.GetByte(position, 0))
}

// @Override
func (te *TinyintType) WriteLong(blockBuilder BlockBuilder, value int64) {
	if value > math.MaxInt8 {
		panic(fmt.Sprintf("Value %d exceeds MAX_BYTE", value))
	}
	if value < math.MinInt8 {
		panic(fmt.Sprintf("Value %d is less than MIN_BYTE", value))
	}
	blockBuilder.WriteByte(byte(value)).CloseEntry()
}

// 继承Type
// @Override
func (te *TinyintType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *TinyintType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *TinyintType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *TinyintType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *TinyintType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *TinyintType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *TinyintType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *TinyintType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *TinyintType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *TinyintType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *TinyintType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *TinyintType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *TinyintType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *TinyintType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *TinyintType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *TinyintType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
