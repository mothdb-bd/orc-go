package block

import (
	"reflect"
)

type ColumnarRow struct {
	positionCount  int32 //@Nullable
	nullCheckBlock Block
	fields         []Block
}

func ToColumnarRow(block Block) *ColumnarRow {
	b, flag := block.(*LazyBlock)
	if flag {
		block = b.GetBlock()
	}
	db, flag := block.(*DictionaryBlock)
	if flag {
		return toColumnarRow(db)
	}
	rb, flag := block.(*RunLengthEncodedBlock)
	if flag {
		return toColumnarRow2(rb)
	}
	ab, flag := block.(*AbstractRowBlock)
	if !flag {
		panic("Invalid row block: " + reflect.TypeOf(block).String())
	}
	rowBlock := ab
	firstRowPosition := rowBlock.getFieldBlockOffset(0)
	totalRowCount := rowBlock.getFieldBlockOffset(block.GetPositionCount()) - firstRowPosition
	fieldBlocks := make([]Block, rowBlock.numFields)
	for i := 0; i < len(fieldBlocks); i++ {
		fieldBlocks[i] = rowBlock.getRawFieldBlocks()[i].GetRegion(firstRowPosition, totalRowCount)
	}
	return NewColumnarRow(block.GetPositionCount(), block, fieldBlocks)
}

func toColumnarRow(dictionaryBlock *DictionaryBlock) *ColumnarRow {
	if !dictionaryBlock.MayHaveNull() {
		return toColumnarRowFromDictionaryWithoutNulls(dictionaryBlock)
	}
	dictionary := dictionaryBlock.GetDictionary()
	newDictionaryIndex := make([]int32, dictionary.GetPositionCount())
	var nextNewDictionaryIndex int32 = 0

	var position int32
	for position = 0; position < dictionary.GetPositionCount(); position++ {
		if !dictionary.IsNull(position) {
			newDictionaryIndex[position] = nextNewDictionaryIndex
			nextNewDictionaryIndex++
		}
	}
	dictionaryIds := make([]int32, dictionaryBlock.GetPositionCount())
	var nonNullPositionCount int32 = 0
	for position = 0; position < dictionaryBlock.GetPositionCount(); position++ {
		if !dictionaryBlock.IsNull(position) {
			oldDictionaryId := dictionaryBlock.GetId(position)
			dictionaryIds[nonNullPositionCount] = newDictionaryIndex[oldDictionaryId]
			nonNullPositionCount++
		}
	}
	columnarRow := ToColumnarRow(dictionaryBlock.GetDictionary())
	fields := make([]Block, columnarRow.GetFieldCount())

	var i int32
	for i = 0; i < columnarRow.GetFieldCount(); i++ {
		fields[i] = NewDictionaryBlock2(nonNullPositionCount, columnarRow.GetField(i), dictionaryIds)
	}
	positionCount := dictionaryBlock.GetPositionCount()
	if nonNullPositionCount == positionCount {
		dictionaryBlock = nil
	}
	return NewColumnarRow(positionCount, dictionaryBlock, fields)
}

func toColumnarRowFromDictionaryWithoutNulls(dictionaryBlock *DictionaryBlock) *ColumnarRow {
	columnarRow := ToColumnarRow(dictionaryBlock.GetDictionary())
	fields := make([]Block, columnarRow.GetFieldCount())
	var i int32
	for i = 0; i < int32(len(fields)); i++ {
		fields[i] = NewDictionaryBlock6(dictionaryBlock.getRawIdsOffset(), dictionaryBlock.GetPositionCount(), columnarRow.GetField(i), dictionaryBlock.getRawIds(), false, RandomDictionaryId())
	}
	return NewColumnarRow(dictionaryBlock.GetPositionCount(), nil, fields)
}

func toColumnarRow2(rleBlock *RunLengthEncodedBlock) *ColumnarRow {
	rleValue := rleBlock.GetValue()
	columnarRow := ToColumnarRow(rleValue)
	fields := make([]Block, columnarRow.GetFieldCount())
	var i int32
	for i = 0; i < columnarRow.GetFieldCount(); i++ {
		nullSuppressedField := columnarRow.GetField(i)
		if rleValue.IsNull(0) {
			if nullSuppressedField.GetPositionCount() != 0 {
				panic("Invalid row block")
			}
			fields[i] = nullSuppressedField
		} else {
			fields[i] = NewRunLengthEncodedBlock(nullSuppressedField, rleBlock.GetPositionCount())
		}
	}
	return NewColumnarRow(rleBlock.GetPositionCount(), rleBlock, fields)
}
func NewColumnarRow(positionCount int32, nullCheckBlock Block, fields []Block) *ColumnarRow {
	cw := new(ColumnarRow)
	cw.positionCount = positionCount
	if nullCheckBlock != nil && nullCheckBlock.MayHaveNull() {
		cw.nullCheckBlock = nullCheckBlock
	} else {
		cw.nullCheckBlock = nil
	}
	cw.fields = fields
	return cw
}

func (cw *ColumnarRow) GetPositionCount() int32 {
	return cw.positionCount
}

func (cw *ColumnarRow) MayHaveNull() bool {
	return cw.nullCheckBlock != nil
}

func (cw *ColumnarRow) IsNull(position int32) bool {
	return cw.nullCheckBlock != nil && cw.nullCheckBlock.IsNull(position)
}

func (cw *ColumnarRow) GetFieldCount() int32 {
	return int32(len(cw.fields))
}

func (cw *ColumnarRow) GetField(index int32) Block {
	return cw.fields[index]
}
