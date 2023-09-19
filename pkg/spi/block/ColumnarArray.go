package block

import "reflect"

type ColumnarArray struct {
	nullCheckBlock Block
	offsetsOffset  int32
	offsets        []int32
	elementsBlock  Block
}

func ToColumnarArray(block Block) *ColumnarArray {
	b, flag := block.(*LazyBlock)
	if flag {
		block = b.GetBlock()
	}
	db, flag := block.(*DictionaryBlock)
	if flag {
		return toColumnarArray(db)
	}
	rb, flag := block.(*RunLengthEncodedBlock)
	if flag {
		return toColumnarArray2(rb)
	}
	ab, flag := block.(IArrayBlock)
	if !flag {
		panic("Invalid array block: " + reflect.TypeOf(block).String())
	}
	arrayBlock := ab
	elementsBlock := arrayBlock.getRawElementBlock()
	var elementsOffset int32 = 0
	var elementsLength int32 = 0
	if arrayBlock.GetPositionCount() > 0 {
		elementsOffset = arrayBlock.getOffset(0)
		elementsLength = arrayBlock.getOffset(arrayBlock.GetPositionCount()) - elementsOffset
	}
	elementsBlock = elementsBlock.GetRegion(elementsOffset, elementsLength)
	return NewColumnarArray(block, arrayBlock.GetOffsetBase(), arrayBlock.GetOffsets(), elementsBlock)
}

func toColumnarArray(dictionaryBlock *DictionaryBlock) *ColumnarArray {
	columnarArray := ToColumnarArray(dictionaryBlock.GetDictionary())
	offsets := make([]int32, dictionaryBlock.GetPositionCount()+1)
	var position int32
	for position = 0; position < dictionaryBlock.GetPositionCount(); position++ {
		dictionaryId := dictionaryBlock.GetId(position)
		offsets[position+1] = offsets[position] + columnarArray.GetLength(dictionaryId)
	}
	dictionaryIds := make([]int32, offsets[dictionaryBlock.GetPositionCount()])
	nextDictionaryIndex := 0

	for position = 0; position < dictionaryBlock.GetPositionCount(); position++ {
		dictionaryId := dictionaryBlock.GetId(position)
		length := columnarArray.GetLength(dictionaryId)
		startOffset := columnarArray.GetOffset(dictionaryId)
		var entryIndex int32
		for entryIndex = 0; entryIndex < length; entryIndex++ {
			dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex
			nextDictionaryIndex++
		}
	}
	return NewColumnarArray(dictionaryBlock, 0, offsets, NewDictionaryBlock2(int32(len(dictionaryIds)), columnarArray.GetElementsBlock(), dictionaryIds))
}

func toColumnarArray2(rleBlock *RunLengthEncodedBlock) *ColumnarArray {
	columnarArray := ToColumnarArray(rleBlock.GetValue())
	offsets := make([]int32, rleBlock.GetPositionCount()+1)
	valueLength := columnarArray.GetLength(0)
	for i := 0; i < len(offsets); i++ {
		offsets[i] = int32(i) * valueLength
	}
	dictionaryIds := make([]int32, rleBlock.GetPositionCount()*valueLength)
	nextDictionaryIndex := 0
	var position int32
	for position = 0; position < rleBlock.GetPositionCount(); position++ {
		var entryIndex int32
		for entryIndex = 0; entryIndex < valueLength; entryIndex++ {
			dictionaryIds[nextDictionaryIndex] = entryIndex
			nextDictionaryIndex++
		}
	}
	return NewColumnarArray(rleBlock, 0, offsets, NewDictionaryBlock2(int32(len(dictionaryIds)), columnarArray.GetElementsBlock(), dictionaryIds))
}
func NewColumnarArray(nullCheckBlock Block, offsetsOffset int32, offsets []int32, elementsBlock Block) *ColumnarArray {
	cy := new(ColumnarArray)
	cy.nullCheckBlock = nullCheckBlock
	cy.offsetsOffset = offsetsOffset
	cy.offsets = offsets
	cy.elementsBlock = elementsBlock
	return cy
}

func (cy *ColumnarArray) GetPositionCount() int32 {
	return cy.nullCheckBlock.GetPositionCount()
}

func (cy *ColumnarArray) IsNull(position int32) bool {
	return cy.nullCheckBlock.IsNull(position)
}

func (cy *ColumnarArray) GetLength(position int32) int32 {
	return (cy.offsets[position+1+cy.offsetsOffset] - cy.offsets[position+cy.offsetsOffset])
}

func (cy *ColumnarArray) GetOffset(position int32) int32 {
	return (cy.offsets[position+cy.offsetsOffset] - cy.offsets[cy.offsetsOffset])
}

func (cy *ColumnarArray) GetElementsBlock() Block {
	return cy.elementsBlock
}
