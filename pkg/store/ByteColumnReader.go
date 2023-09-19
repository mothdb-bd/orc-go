package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BYTE_COLUMN_INSTANCE_SIZE int32 = util.SizeOf(&ByteColumnReader{})

type ByteColumnReader struct {
	// 继承
	ColumnReader

	column              *MothColumn
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	dataStreamSource    InputStreamSource // [*ByteInputStream] //@Nullable
	dataStream          *ByteInputStream
	rowGroupOpen        bool
	nonNullValueTemp    []byte
	memoryContext       memory.LocalMemoryContext
}

func NewByteColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *ByteColumnReader {
	br := new(ByteColumnReader)

	br.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	br.dataStreamSource = MissingStreamSource()    // [*ByteInputStream]

	br.nonNullValueTemp = make([]byte, 0)

	br.column = column
	br.memoryContext = memoryContext
	return br
}

// @Override
func (br *ByteColumnReader) PrepareNextRead(batchSize int32) {
	br.readOffset += br.nextBatchSize
	br.nextBatchSize = batchSize
}

// @Override
func (br *ByteColumnReader) ReadBlock() block.Block {
	if !br.rowGroupOpen {
		br.openRowGroup()
	}
	if br.readOffset > 0 {
		if br.presentStream != nil {
			br.readOffset = br.presentStream.CountBitsSet(br.readOffset)
		}
		if br.readOffset > 0 {
			if br.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			br.dataStream.Skip(int64(br.readOffset))
		}
	}
	var b block.Block
	if br.dataStream == nil {
		if br.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		br.presentStream.Skip(int64(br.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(block.TINYINT, nil, br.nextBatchSize)
	} else if br.presentStream == nil {
		b = br.readNonNullBlock()
	} else {
		isNull := make([]bool, br.nextBatchSize)
		nullCount := br.presentStream.GetUnsetBits(br.nextBatchSize, isNull)
		if nullCount == 0 {
			b = br.readNonNullBlock()
		} else if nullCount != br.nextBatchSize {
			b = br.readNullBlock(isNull, br.nextBatchSize-nullCount)
		} else {
			b = block.CreateRunLengthEncodedBlock(block.TINYINT, nil, br.nextBatchSize)
		}
	}
	br.readOffset = 0
	br.nextBatchSize = 0
	return b
}

func (br *ByteColumnReader) readNonNullBlock() block.Block {
	values := br.dataStream.Next2(br.nextBatchSize)
	return block.NewByteArrayBlock(br.nextBatchSize, optional.Empty[[]bool](), values)
}

func (br *ByteColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {
	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(br.nonNullValueTemp) < minNonNullValueSize {
		br.nonNullValueTemp = make([]byte, minNonNullValueSize)
		br.memoryContext.SetBytes(util.SizeOfInt64(br.nonNullValueTemp))
	}
	br.dataStream.Next3(br.nonNullValueTemp, nonNullCount)
	result := UnpackByteNulls(br.nonNullValueTemp, isNull)
	return block.NewByteArrayBlock(br.nextBatchSize, optional.Of(isNull), result)
}

func (br *ByteColumnReader) openRowGroup() {
	pe := br.presentStreamSource.OpenStream()
	if pe != nil {
		br.presentStream = pe.(*BooleanInputStream)
	} else {
		br.presentStream = nil
	}

	de := br.dataStreamSource.OpenStream()
	if de != nil {
		br.dataStream = de.(*ByteInputStream)
	} else {
		br.dataStream = nil
	}
	br.rowGroupOpen = true
}

// @Override
func (br *ByteColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	br.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	br.dataStreamSource = MissingStreamSource()    //[*ByteInputStream]
	br.readOffset = 0
	br.nextBatchSize = 0
	br.presentStream = nil
	br.dataStream = nil
	br.rowGroupOpen = false
}

// @Override
func (br *ByteColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	br.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, br.column, metadata.PRESENT)
	br.dataStreamSource = GetInputStreamSource[*ByteInputStream](dataStreamSources, br.column, metadata.DATA)
	br.readOffset = 0
	br.nextBatchSize = 0
	br.presentStream = nil
	br.dataStream = nil
	br.rowGroupOpen = false
}

// @Override
func (br *ByteColumnReader) String() string {
	return util.NewSB().AppendString(br.column.String()).String()
}

// @Override
func (br *ByteColumnReader) Close() {
	br.memoryContext.Close()
}

// @Override
func (br *ByteColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(BYTE_COLUMN_INSTANCE_SIZE)
}
