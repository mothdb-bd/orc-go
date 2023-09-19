package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ArrayType struct {
	// 继承
	AbstractType

	elementType Type
}

var ARRAY_NULL_ELEMENT_MSG string = "ARRAY comparison not supported for arrays with null elements"

func NewArrayType(elementType Type) *ArrayType {
	ae := new(ArrayType)
	ae.signature = NewTypeSignature(ST_ARRAY, TSP_TypeParameter(elementType.GetTypeSignature()))
	ae.goKind = reflect.TypeOf(new(Block)).Kind()
	ae.elementType = elementType
	return ae
}

func (ae *ArrayType) GetElementType() Type {
	return ae.elementType
}

// @Override
func (ae *ArrayType) IsComparable() bool {
	return ae.elementType.IsComparable()
}

// @Override
func (ae *ArrayType) IsOrderable() bool {
	return ae.elementType.IsOrderable()
}

// @Override
func (ae *ArrayType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		ae.WriteObject(blockBuilder, ae.GetObject(block, position))
	}
}

// @Override
func (ae *ArrayType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

// @Override
func (ae *ArrayType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	ae.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (ae *ArrayType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}

// @Override
func (ae *ArrayType) GetObject(block Block, position int32) basic.Object {
	return block.GetObject(position, reflect.TypeOf(new(Block)))
}

// @Override
func (ae *ArrayType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	arrayBlock := value.(Block)
	entryBuilder := blockBuilder.BeginBlockEntry()
	for i := int32(0); i < arrayBlock.GetPositionCount(); i++ {
		ae.elementType.AppendTo(arrayBlock, i, entryBuilder)
	}
	blockBuilder.CloseEntry()
}

// @Override
func (ae *ArrayType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return NewArrayBlockBuilder2(ae.elementType, blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (ae *ArrayType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, 100)
}

// @Override
func (ae *ArrayType) GetTypeParameters() *util.ArrayList[Type] {
	l := util.NewArrayList(ae.GetElementType())
	return l
}

// @Override
func (ae *ArrayType) GetDisplayName() string {
	return ST_ARRAY + "(" + ae.elementType.GetDisplayName() + ")"
}
