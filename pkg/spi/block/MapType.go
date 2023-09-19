package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MAP_EXPECTED_BYTES_PER_ENTRY int32 = 32

type MapType struct {
	// 继承
	AbstractType

	keyType   Type
	valueType Type
}

func NewMapType(keyType Type, valueType Type) *MapType {
	me := new(MapType)

	me.signature = NewTypeSignature(ST_MAP, TSP_TypeParameter(keyType.GetTypeSignature()), TSP_TypeParameter(valueType.GetTypeSignature()))
	me.goKind = BLOCK_KIND

	if !keyType.IsComparable() {
		panic(fmt.Sprintf("key type must be comparable, got %s", keyType))
	}
	me.keyType = keyType
	me.valueType = valueType

	me.AbstractType = *NewAbstractType(me.signature, me.goKind)
	return me
}

// @Override
func (me *MapType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return NewMapBlockBuilder(me, blockBuilderStatus, expectedEntries)
}

// @Override
func (me *MapType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return me.CreateBlockBuilder(blockBuilderStatus, expectedEntries, MAP_EXPECTED_BYTES_PER_ENTRY)
}

func (me *MapType) GetKeyType() Type {
	return me.keyType
}

func (me *MapType) GetValueType() Type {
	return me.valueType
}

// @Override
func (me *MapType) IsComparable() bool {
	return me.valueType.IsComparable()
}

// @Override
func (me *MapType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		me.WriteObject(blockBuilder, me.GetObject(block, position))
	}
}

// @Override
func (me *MapType) GetObject(block Block, position int32) basic.Object {
	return block.GetObject(position, BLOCK_TYPE)
}

// @Override
func (me *MapType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	singleMapBlock, flag := value.(*SingleMapBlock)
	if !flag {
		panic("Maps must be represented with SingleMapBlock")
	}
	entryBuilder := blockBuilder.BeginBlockEntry()
	var i int32
	for i = 0; i < singleMapBlock.GetPositionCount(); i += 2 {
		me.keyType.AppendTo(singleMapBlock, i, entryBuilder)
		me.valueType.AppendTo(singleMapBlock, i+1, entryBuilder)
	}
	blockBuilder.CloseEntry()
}

// @Override
func (me *MapType) GetTypeParameters() *util.ArrayList[Type] {
	return util.NewArrayList(me.GetKeyType(), me.GetValueType())
}

// @Override
func (me *MapType) GetDisplayName() string {
	return fmt.Sprintf("map(%s, %s)", me.keyType.GetDisplayName(), me.valueType.GetDisplayName())
}

func (me *MapType) CreateBlockFromKeyValue(mapIsNull *optional.Optional[[]bool], offsets []int32, keyBlock Block, valueBlock Block) Block {
	return FromKeyValueBlock(mapIsNull, offsets, keyBlock, valueBlock, me)
}
