package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var VARBINARY *VarbinaryType = NewVarbinaryType()

type VarbinaryType struct {
	// 继承
	AbstractVariableWidthType
}

func NewVarbinaryType() *VarbinaryType {
	ve := new(VarbinaryType)
	ve.signature = NewTypeSignature(ST_VARBINARY)
	ve.goKind = slice.SLICE_KIND
	ve.AbstractType = *NewAbstractType(ve.signature, ve.goKind)
	return ve
}

// @Override
func (ve *VarbinaryType) IsComparable() bool {
	return true
}

// @Override
func (ve *VarbinaryType) IsOrderable() bool {
	return true
}

// @Override
func (ve *VarbinaryType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		block.WriteBytesTo(position, 0, block.GetSliceLength(position), blockBuilder)
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ve *VarbinaryType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

// @Override
func (ve *VarbinaryType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	ve.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (ve *VarbinaryType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}

// 继承Type
// @Override
func (te *VarbinaryType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *VarbinaryType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *VarbinaryType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *VarbinaryType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *VarbinaryType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *VarbinaryType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *VarbinaryType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *VarbinaryType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *VarbinaryType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *VarbinaryType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *VarbinaryType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *VarbinaryType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *VarbinaryType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *VarbinaryType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *VarbinaryType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *VarbinaryType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
