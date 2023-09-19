package store

import (
	"fmt"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var LONG_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&LongColumnReader{})

type LongColumnReader struct {
	// 继承
	ColumnReader

	kind                  block.Type
	column                *MothColumn
	readOffset            int32
	nextBatchSize         int32
	presentStreamSource   InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream         *BooleanInputStream
	dataStreamSource      InputStreamSource // [LongInputStream] //@Nullable
	dataStream            LongInputStream
	rowGroupOpen          bool
	shortNonNullValueTemp []int16
	intNonNullValueTemp   []int32
	longNonNullValueTemp  []int64
	memoryContext         memory.LocalMemoryContext
}

func NewLongColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *LongColumnReader {
	lr := new(LongColumnReader)

	lr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	lr.dataStreamSource = MissingStreamSource()    //[LongInputStream]

	lr.shortNonNullValueTemp = make([]int16, 0)
	lr.intNonNullValueTemp = make([]int32, 0)
	lr.longNonNullValueTemp = make([]int64, 0)

	lr.kind = kind
	lr.column = column
	lr.memoryContext = memoryContext
	return lr
}

// @Override
func (lr *LongColumnReader) PrepareNextRead(batchSize int32) {
	lr.readOffset += lr.nextBatchSize
	lr.nextBatchSize = batchSize
}

// @Override
func (lr *LongColumnReader) ReadBlock() block.Block {
	if !lr.rowGroupOpen {
		lr.openRowGroup()
	}
	if lr.readOffset > 0 {
		if lr.presentStream != nil {
			lr.readOffset = lr.presentStream.CountBitsSet(lr.readOffset)
		}
		if lr.readOffset > 0 {
			if lr.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			lr.dataStream.Skip(int64(lr.readOffset))
		}
	}
	var b block.Block
	if lr.dataStream == nil {
		if lr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		lr.presentStream.Skip(int64(lr.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(lr.kind, nil, lr.nextBatchSize)
	} else if lr.presentStream == nil {
		b = lr.readNonNullBlock()
	} else {
		isNull := make([]bool, lr.nextBatchSize)
		nullCount := lr.presentStream.GetUnsetBits(lr.nextBatchSize, isNull)
		if nullCount == 0 {
			b = lr.readNonNullBlock()
		} else if nullCount != lr.nextBatchSize {
			b = lr.readNullBlock(isNull, lr.nextBatchSize-nullCount)
		} else {
			b = block.CreateRunLengthEncodedBlock(lr.kind, nil, lr.nextBatchSize)
		}
	}
	lr.readOffset = 0
	lr.nextBatchSize = 0
	return b
}

func (lr *LongColumnReader) readNonNullBlock() block.Block {

	_, flag := lr.kind.(*block.BigintType)
	if flag {
		values := make([]int64, lr.nextBatchSize)
		lr.dataStream.Next2(values, lr.nextBatchSize)
		return block.NewLongArrayBlock(lr.nextBatchSize, optional.Empty[[]bool](), values)
	}

	_, flag = lr.kind.(*block.TimeType)

	if flag {
		values := make([]int64, lr.nextBatchSize)
		lr.dataStream.Next2(values, lr.nextBatchSize)
		lr.maybeTransformValues(values, lr.nextBatchSize)
		return block.NewLongArrayBlock(lr.nextBatchSize, optional.Empty[[]bool](), values)
	}

	_, iflag := lr.kind.(*block.IntegerType)
	_, dflag := lr.kind.(*block.DateType)
	if iflag || dflag {
		values := make([]int32, lr.nextBatchSize)
		lr.dataStream.Next3(values, lr.nextBatchSize)
		return block.NewIntArrayBlock(lr.nextBatchSize, optional.Empty[[]bool](), values)
	}
	_, flag = lr.kind.(*block.SmallintType)
	if flag {
		values := make([]int16, lr.nextBatchSize)
		lr.dataStream.Next4(values, lr.nextBatchSize)
		return block.NewShortArrayBlock(lr.nextBatchSize, optional.Empty[[]bool](), values)
	}
	panic(fmt.Sprintf("Unsupported type %s", lr.kind))
}

func (lr *LongColumnReader) maybeTransformValues(values []int64, nextBatchSize int32) {
}

func (lr *LongColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {

	_, flag := lr.kind.(*block.BigintType)
	if flag {
		return lr.longReadNullBlock(isNull, nonNullCount)
	}

	_, iflag := lr.kind.(*block.IntegerType)
	_, dflag := lr.kind.(*block.DateType)
	if iflag || dflag {
		return lr.intReadNullBlock(isNull, nonNullCount)
	}
	_, flag = lr.kind.(*block.SmallintType)
	if flag {
		return lr.shortReadNullBlock(isNull, nonNullCount)
	}
	panic(fmt.Sprintf("Unsupported type %s", lr.kind))
}

func (lr *LongColumnReader) longReadNullBlock(isNull []bool, nonNullCount int32) block.Block {

	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(lr.longNonNullValueTemp) < minNonNullValueSize {
		lr.longNonNullValueTemp = make([]int64, minNonNullValueSize)
		lr.memoryContext.SetBytes(util.SizeOfInt64(lr.longNonNullValueTemp))
	}
	lr.dataStream.Next2(lr.longNonNullValueTemp, nonNullCount)
	result := UnpackLongNulls(lr.longNonNullValueTemp, isNull)
	return block.NewLongArrayBlock(lr.nextBatchSize, optional.Of(isNull), result)
}

func (lr *LongColumnReader) intReadNullBlock(isNull []bool, nonNullCount int32) block.Block {

	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(lr.intNonNullValueTemp) < minNonNullValueSize {
		lr.intNonNullValueTemp = make([]int32, minNonNullValueSize)
		lr.memoryContext.SetBytes(util.SizeOfInt64(lr.intNonNullValueTemp))
	}
	lr.dataStream.Next3(lr.intNonNullValueTemp, nonNullCount)
	result := UnpackIntNulls(lr.intNonNullValueTemp, isNull)
	return block.NewIntArrayBlock(lr.nextBatchSize, optional.Of(isNull), result)
}

func (lr *LongColumnReader) shortReadNullBlock(isNull []bool, nonNullCount int32) block.Block {

	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(lr.shortNonNullValueTemp) < minNonNullValueSize {
		lr.shortNonNullValueTemp = make([]int16, minNonNullValueSize)
		lr.memoryContext.SetBytes(util.SizeOfInt64(lr.shortNonNullValueTemp))
	}
	lr.dataStream.Next4(lr.shortNonNullValueTemp, nonNullCount)
	result := UnpackShortNulls(lr.shortNonNullValueTemp, isNull)
	return block.NewShortArrayBlock(lr.nextBatchSize, optional.Of(isNull), result)
}

func (lr *LongColumnReader) openRowGroup() {
	pe := lr.presentStreamSource.OpenStream()
	if pe != nil {
		lr.presentStream = pe.(*BooleanInputStream)
	} else {
		lr.presentStream = nil
	}

	de := lr.dataStreamSource.OpenStream()
	if de != nil {
		lr.dataStream = de.(*LongInputStreamV2)
	} else {
		lr.dataStream = nil
	}

	lr.rowGroupOpen = true
}

// @Override
func (lr *LongColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	lr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	lr.dataStreamSource = MissingStreamSource()    //[LongInputStream]
	lr.readOffset = 0
	lr.nextBatchSize = 0
	lr.presentStream = nil
	lr.dataStream = nil
	lr.rowGroupOpen = false
}

// @Override
func (lr *LongColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	lr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, lr.column, metadata.PRESENT)
	lr.dataStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, lr.column, metadata.DATA)
	lr.readOffset = 0
	lr.nextBatchSize = 0
	lr.presentStream = nil
	lr.dataStream = nil
	lr.rowGroupOpen = false
}

// @Override
func (lr *LongColumnReader) ToString() string {
	return util.NewSB().AppendString(lr.column.String()).String()
}

// @Override
func (lr *LongColumnReader) Close() {
	lr.memoryContext.Close()
}

// @Override
func (lr *LongColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(LONG_COLUMN_READER_INSTANCE_SIZE)
}
