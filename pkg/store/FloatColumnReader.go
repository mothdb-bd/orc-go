package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var FLOAT_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&FloatColumnReader{})

type FloatColumnReader struct {
	// 继承
	ColumnReader

	column              *MothColumn
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	dataStreamSource    InputStreamSource // [*FloatInputStream] //@Nullable
	dataStream          *FloatInputStream
	rowGroupOpen        bool
	nonNullValueTemp    []int32
	memoryContext       memory.LocalMemoryContext
}

func NewFloatColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *FloatColumnReader {
	fr := new(FloatColumnReader)

	fr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	fr.dataStreamSource = MissingStreamSource()    //[*FloatInputStream]
	fr.nonNullValueTemp = make([]int32, 0)

	fr.column = column
	fr.memoryContext = memoryContext
	return fr
}

// @Override
func (fr *FloatColumnReader) PrepareNextRead(batchSize int32) {
	fr.readOffset += fr.nextBatchSize
	fr.nextBatchSize = batchSize
}

// @Override
func (fr *FloatColumnReader) ReadBlock() block.Block {
	if !fr.rowGroupOpen {
		fr.openRowGroup()
	}
	if fr.readOffset > 0 {
		if fr.presentStream != nil {
			fr.readOffset = fr.presentStream.CountBitsSet(fr.readOffset)
		}
		if fr.readOffset > 0 {
			if fr.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			fr.dataStream.Skip(int64(fr.readOffset))
		}
	}
	var b block.Block
	if fr.dataStream == nil {
		if fr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		fr.presentStream.Skip(int64(fr.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(block.REAL, nil, fr.nextBatchSize)
	} else if fr.presentStream == nil {
		b = fr.readNonNullBlock()
	} else {
		isNull := make([]bool, fr.nextBatchSize)
		nullCount := fr.presentStream.GetUnsetBits(fr.nextBatchSize, isNull)
		if nullCount == 0 {
			b = fr.readNonNullBlock()
		} else if nullCount != fr.nextBatchSize {
			b = fr.readNullBlock(isNull, fr.nextBatchSize-nullCount)
		} else {
			b = block.CreateRunLengthEncodedBlock(block.REAL, nil, fr.nextBatchSize)
		}
	}
	fr.readOffset = 0
	fr.nextBatchSize = 0
	return b
}

func (fr *FloatColumnReader) readNonNullBlock() block.Block {
	values := make([]int32, fr.nextBatchSize)
	fr.dataStream.Next2(values, fr.nextBatchSize)
	return block.NewIntArrayBlock(fr.nextBatchSize, optional.Empty[[]bool](), values)
}

func (fr *FloatColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {
	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(fr.nonNullValueTemp) < minNonNullValueSize {
		fr.nonNullValueTemp = make([]int32, minNonNullValueSize)
		fr.memoryContext.SetBytes(util.SizeOfInt64(fr.nonNullValueTemp))
	}
	fr.dataStream.Next2(fr.nonNullValueTemp, nonNullCount)
	result := UnpackIntNulls(fr.nonNullValueTemp, isNull)
	return block.NewIntArrayBlock(util.Lens(isNull), optional.Of(isNull), result)
}

func (fr *FloatColumnReader) openRowGroup() {
	pe := fr.presentStreamSource.OpenStream()
	if pe != nil {
		fr.presentStream = pe.(*BooleanInputStream)
	} else {
		fr.presentStream = nil
	}

	de := fr.dataStreamSource.OpenStream()
	if de != nil {
		fr.dataStream = de.(*FloatInputStream)
	} else {
		fr.dataStream = nil
	}
	fr.rowGroupOpen = true
}

// @Override
func (fr *FloatColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	fr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	fr.dataStreamSource = MissingStreamSource()    //[*FloatInputStream]
	fr.readOffset = 0
	fr.nextBatchSize = 0
	fr.presentStream = nil
	fr.dataStream = nil
	fr.rowGroupOpen = false
}

// @Override
func (fr *FloatColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	fr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, fr.column, metadata.PRESENT)
	fr.dataStreamSource = GetInputStreamSource[*FloatInputStream](dataStreamSources, fr.column, metadata.DATA)
	fr.readOffset = 0
	fr.nextBatchSize = 0
	fr.presentStream = nil
	fr.dataStream = nil
	fr.rowGroupOpen = false
}

// @Override
func (fr *FloatColumnReader) ToString() string {
	return util.NewSB().AppendString(fr.column.String()).String()
}

// @Override
func (fr *FloatColumnReader) Close() {
	fr.memoryContext.Close()
}

// @Override
func (fr *FloatColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(FLOAT_COLUMN_READER_INSTANCE_SIZE)
}
