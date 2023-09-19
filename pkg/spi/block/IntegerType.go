package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var INTEGER *IntegerType = NewIntegerType()

type IntegerType struct {
	// 继承
	AbstractIntType
}

func NewIntegerType() *IntegerType {
	ie := new(IntegerType)
	ie.signature = NewTypeSignature(ST_INTEGER)
	ie.goKind = reflect.Int64

	ie.AbstractType = *NewAbstractType(ie.signature, ie.goKind)

	return ie
}

// @Override
// @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
func (ie *IntegerType) Equals(other Type) bool {
	return other == INTEGER
}

// 继承Type
// @Override
func (te *IntegerType) GetTypeSignature() *TypeSignature {
	return te.AbstractIntType.GetTypeSignature()
}

// @Override
func (te *IntegerType) GetTypeId() *TypeId {
	return te.AbstractIntType.GetTypeId()
}

// @Override
func (te *IntegerType) GetBaseName() string {
	return te.AbstractIntType.GetBaseName()
}

// @Override
func (te *IntegerType) GetDisplayName() string {
	return te.AbstractIntType.GetDisplayName()
}

// @Override
func (te *IntegerType) GetGoKind() reflect.Kind {
	return te.AbstractIntType.GetGoKind()
}

// @Override
func (te *IntegerType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractIntType.GetTypeParameters()
}

// @Override
func (te *IntegerType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractIntType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *IntegerType) GetBoolean(block Block, position int32) bool {
	return te.AbstractIntType.GetBoolean(block, position)
}

// @Override
func (te *IntegerType) GetLong(block Block, position int32) int64 {
	return te.AbstractIntType.GetLong(block, position)
}

// @Override
func (te *IntegerType) GetDouble(block Block, position int32) float64 {
	return te.AbstractIntType.GetDouble(block, position)
}

// @Override
func (te *IntegerType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractIntType.GetObject(block, position)
}

// @Override
func (te *IntegerType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractIntType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *IntegerType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractIntType.WriteLong(blockBuilder, value)
}

// @Override
func (te *IntegerType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractIntType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *IntegerType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractIntType.WriteObject(blockBuilder, value)
}

// @Override
func (te *IntegerType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractIntType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *IntegerType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractIntType.WriteSlice2(blockBuilder, value, offset, length)
}
