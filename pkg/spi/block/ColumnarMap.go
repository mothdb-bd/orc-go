package block

import "reflect"

type ColumnarMap struct {
	nullCheckBlock Block
	offsetsOffset  int32
	offsets        []int32
	keysBlock      Block
	valuesBlock    Block
}

func ToColumnarMap(block Block) *ColumnarMap {
	b, flag := block.(*LazyBlock)
	if flag {
		block = b.GetBlock()
	}
	db, flag := block.(*DictionaryBlock)
	if flag {
		return toColumnarMap(db)
	}
	rb, flag := block.(*RunLengthEncodedBlock)
	if flag {
		return toColumnarMap2(rb)
	}
	ab, flag := block.(*AbstractMapBlock)
	if !flag {
		panic("Invalid array block: " + reflect.TypeOf(block).String())
	}
	mapBlock := ab
	offsetBase := mapBlock.getOffsetBase()
	offsets := mapBlock.getOffsets()
	firstEntryPosition := mapBlock.getOffset(0)
	totalEntryCount := mapBlock.getOffset(block.GetPositionCount()) - firstEntryPosition
	keysBlock := mapBlock.getRawKeyBlock().GetRegion(firstEntryPosition, totalEntryCount)
	valuesBlock := mapBlock.getRawValueBlock().GetRegion(firstEntryPosition, totalEntryCount)
	return NewColumnarMap(block, offsetBase, offsets, keysBlock, valuesBlock)
}

func toColumnarMap(dictionaryBlock *DictionaryBlock) *ColumnarMap {
	columnarMap := ToColumnarMap(dictionaryBlock.GetDictionary())
	offsets := make([]int32, dictionaryBlock.GetPositionCount()+1)
	var position int32
	for position = 0; position < dictionaryBlock.GetPositionCount(); position++ {
		dictionaryId := dictionaryBlock.GetId(position)
		offsets[position+1] = offsets[position] + columnarMap.GetEntryCount(dictionaryId)
	}
	dictionaryIds := make([]int32, offsets[dictionaryBlock.GetPositionCount()])
	nextDictionaryIndex := 0
	for position = 0; position < dictionaryBlock.GetPositionCount(); position++ {
		dictionaryId := dictionaryBlock.GetId(position)
		entryCount := columnarMap.GetEntryCount(dictionaryId)
		startOffset := columnarMap.GetOffset(dictionaryId)
		var entryIndex int32
		for entryIndex = 0; entryIndex < entryCount; entryIndex++ {
			dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex
			nextDictionaryIndex++
		}
	}
	return NewColumnarMap(dictionaryBlock, 0, offsets, NewDictionaryBlock2(int32(len(dictionaryIds)), columnarMap.GetKeysBlock(), dictionaryIds), NewDictionaryBlock2(int32(len(dictionaryIds)), columnarMap.GetValuesBlock(), dictionaryIds))
}

func toColumnarMap2(rleBlock *RunLengthEncodedBlock) *ColumnarMap {
	columnarMap := ToColumnarMap(rleBlock.GetValue())
	offsets := make([]int32, rleBlock.GetPositionCount()+1)
	entryCount := columnarMap.GetEntryCount(0)
	var i int32
	for i = 0; i < int32(len(offsets)); i++ {
		offsets[i] = i * entryCount
	}
	dictionaryIds := make([]int32, rleBlock.GetPositionCount()*entryCount)
	nextDictionaryIndex := 0
	var position int32
	for position = 0; position < rleBlock.GetPositionCount(); position++ {
		var entryIndex int32
		for entryIndex = 0; entryIndex < entryCount; entryIndex++ {
			dictionaryIds[nextDictionaryIndex] = entryIndex
			nextDictionaryIndex++
		}
	}
	return NewColumnarMap(rleBlock, 0, offsets, NewDictionaryBlock2(int32(len(dictionaryIds)), columnarMap.GetKeysBlock(), dictionaryIds), NewDictionaryBlock2(int32(len(dictionaryIds)), columnarMap.GetValuesBlock(), dictionaryIds))
}
func NewColumnarMap(nullCheckBlock Block, offsetsOffset int32, offsets []int32, keysBlock Block, valuesBlock Block) *ColumnarMap {
	cp := new(ColumnarMap)
	cp.nullCheckBlock = nullCheckBlock
	cp.offsetsOffset = offsetsOffset
	cp.offsets = offsets
	cp.keysBlock = keysBlock
	cp.valuesBlock = valuesBlock
	return cp
}

func (cp *ColumnarMap) GetPositionCount() int32 {
	return cp.nullCheckBlock.GetPositionCount()
}

func (cp *ColumnarMap) IsNull(position int32) bool {
	return cp.nullCheckBlock.IsNull(position)
}

func (cp *ColumnarMap) GetEntryCount(position int32) int32 {
	return (cp.offsets[position+1+cp.offsetsOffset] - cp.offsets[position+cp.offsetsOffset])
}

func (cp *ColumnarMap) GetOffset(position int32) int32 {
	return (cp.offsets[position+cp.offsetsOffset] - cp.offsets[cp.offsetsOffset])
}

func (cp *ColumnarMap) GetKeysBlock() Block {
	return cp.keysBlock
}

func (cp *ColumnarMap) GetValuesBlock() Block {
	return cp.valuesBlock
}
