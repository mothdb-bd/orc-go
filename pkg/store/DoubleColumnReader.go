package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DOUBLE_COLUMN_INSTANCE_SIZE int32 = util.SizeOf(&DoubleColumnReader{})

type DoubleColumnReader struct {
	// 继承
	ColumnReader

	column              *MothColumn
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	dataStreamSource    InputStreamSource // [*DoubleInputStream] //@Nullable
	dataStream          *DoubleInputStream
	rowGroupOpen        bool
	nonNullValueTemp    []int64
	memoryContext       memory.LocalMemoryContext
}

func NewDoubleColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *DoubleColumnReader {
	dr := new(DoubleColumnReader)

	// 初始化变量
	dr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	dr.dataStreamSource = MissingStreamSource()    //[*DoubleInputStream]
	dr.nonNullValueTemp = make([]int64, 0)

	dr.column = column
	dr.memoryContext = memoryContext
	return dr
}

// @Override
func (dr *DoubleColumnReader) PrepareNextRead(batchSize int32) {
	dr.readOffset += dr.nextBatchSize
	dr.nextBatchSize = batchSize
}

// @Override
func (dr *DoubleColumnReader) ReadBlock() block.Block {
	if !dr.rowGroupOpen {
		dr.openRowGroup()
	}
	if dr.readOffset > 0 {
		if dr.presentStream != nil {
			dr.readOffset = dr.presentStream.CountBitsSet(dr.readOffset)
		}
		if dr.readOffset > 0 {
			if dr.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			dr.dataStream.Skip(int64(dr.readOffset))
		}
	}
	var b block.Block
	if dr.dataStream == nil {
		if dr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		dr.presentStream.Skip(int64(dr.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(block.DOUBLE, nil, dr.nextBatchSize)
	} else if dr.presentStream == nil {
		b = dr.readNonNullBlock()
	} else {
		isNull := make([]bool, dr.nextBatchSize)
		nullCount := dr.presentStream.GetUnsetBits(dr.nextBatchSize, isNull)
		if nullCount == 0 {
			b = dr.readNonNullBlock()
		} else if nullCount != dr.nextBatchSize {
			b = dr.readNullBlock(isNull, dr.nextBatchSize-nullCount)
		} else {
			b = block.CreateRunLengthEncodedBlock(block.DOUBLE, nil, dr.nextBatchSize)
		}
	}
	dr.readOffset = 0
	dr.nextBatchSize = 0
	return b
}

func (dr *DoubleColumnReader) readNonNullBlock() block.Block {
	values := make([]int64, dr.nextBatchSize)
	dr.dataStream.Next2(values, dr.nextBatchSize)
	return block.NewLongArrayBlock(dr.nextBatchSize, optional.Empty[[]bool](), values)
}

func (dr *DoubleColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {
	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(dr.nonNullValueTemp) < minNonNullValueSize {
		dr.nonNullValueTemp = make([]int64, minNonNullValueSize)
		dr.memoryContext.SetBytes(util.SizeOfInt64(dr.nonNullValueTemp))
	}
	dr.dataStream.Next2(dr.nonNullValueTemp, nonNullCount)
	result := UnpackLongNulls(dr.nonNullValueTemp, isNull)
	return block.NewLongArrayBlock(util.Lens(isNull), optional.Of(isNull), result)
}

func (dr *DoubleColumnReader) openRowGroup() {
	pe := dr.presentStreamSource.OpenStream()
	if pe != nil {
		dr.presentStream = pe.(*BooleanInputStream)
	} else {
		dr.presentStream = nil
	}

	de := dr.dataStreamSource.OpenStream()
	if de != nil {
		dr.dataStream = de.(*DoubleInputStream)
	} else {
		dr.dataStream = nil
	}

	dr.rowGroupOpen = true
}

// @Override
func (dr *DoubleColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	dr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	dr.dataStreamSource = MissingStreamSource()    //[*DoubleInputStream]
	dr.readOffset = 0
	dr.nextBatchSize = 0
	dr.presentStream = nil
	dr.dataStream = nil
	dr.rowGroupOpen = false
}

// @Override
func (dr *DoubleColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	dr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, dr.column, metadata.PRESENT)
	dr.dataStreamSource = GetInputStreamSource[*DoubleInputStream](dataStreamSources, dr.column, metadata.DATA)
	dr.readOffset = 0
	dr.nextBatchSize = 0
	dr.presentStream = nil
	dr.dataStream = nil
	dr.rowGroupOpen = false
}

// @Override
func (dr *DoubleColumnReader) String() string {
	return util.NewSB().AppendString(dr.column.String()).String()
}

// @Override
func (dr *DoubleColumnReader) Close() {
	dr.memoryContext.Close()
}

// @Override
func (dr *DoubleColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(DOUBLE_COLUMN_INSTANCE_SIZE)
}
