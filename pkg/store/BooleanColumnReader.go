package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BOOL_COLUMN_INSTANCE_SIZE int32 = util.SizeOf(&BooleanColumnReader{})

type BooleanColumnReader struct {
	// 继承
	ColumnReader

	column        *MothColumn
	readOffset    int32
	nextBatchSize int32
	// private InputStreamSource[ ]<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
	presentStream       *BooleanInputStream
	presentStreamSource InputStreamSource // [*BooleanInputStream]
	// private InputStreamSource[ ]<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
	dataStreamSource InputStreamSource //[*BooleanInputStream] //@Nullable
	dataStream       *BooleanInputStream
	rowGroupOpen     bool
	nonNullValueTemp []byte
	memoryContext    memory.LocalMemoryContext
}

func NewBooleanColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *BooleanColumnReader {
	br := new(BooleanColumnReader)

	// 初始化变量
	br.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	br.dataStreamSource = MissingStreamSource()    //[*BooleanInputStream]
	br.nonNullValueTemp = make([]byte, 0)

	br.column = column
	br.memoryContext = memoryContext
	return br
}

// @Override
func (br *BooleanColumnReader) PrepareNextRead(batchSize int32) {
	br.readOffset += br.nextBatchSize
	br.nextBatchSize = batchSize
}

// @Override
func (br *BooleanColumnReader) ReadBlock() block.Block {
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
			br.dataStream.SkipInt32(br.readOffset)
		}
	}
	var b block.Block
	if br.dataStream == nil {
		if br.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		br.presentStream.SkipInt32(br.nextBatchSize)
		b = block.CreateRunLengthEncodedBlock(block.BOOLEAN, nil, br.nextBatchSize)
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
			b = block.CreateRunLengthEncodedBlock(block.BOOLEAN, nil, br.nextBatchSize)
		}
	}
	br.readOffset = 0
	br.nextBatchSize = 0
	return b
}

func (br *BooleanColumnReader) readNonNullBlock() block.Block {
	values := br.dataStream.GetSetBits(br.nextBatchSize)
	return block.NewByteArrayBlock(br.nextBatchSize, optional.Empty[[]bool](), values)
}

func (br *BooleanColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {
	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.BytesLenInt32(br.nonNullValueTemp) < minNonNullValueSize {
		br.nonNullValueTemp = make([]byte, minNonNullValueSize)
		br.memoryContext.SetBytes(int64(util.SizeOf(br.nonNullValueTemp)))
	}
	br.dataStream.GetSetBits2(br.nonNullValueTemp, nonNullCount)
	result := UnpackByteNulls(br.nonNullValueTemp, isNull)
	return block.NewByteArrayBlock(br.nextBatchSize, optional.Of(isNull), result)
}

func (br *BooleanColumnReader) openRowGroup() {
	pe := br.presentStreamSource.OpenStream()
	if pe != nil {
		br.presentStream = pe.(*BooleanInputStream)
	} else {
		br.presentStream = nil
	}
	de := br.dataStreamSource.OpenStream()
	if de != nil {
		br.dataStream = de.(*BooleanInputStream)
	} else {
		br.dataStream = nil
	}
	br.rowGroupOpen = true
}

// @Override
func (br *BooleanColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	// br.presentStreamSource = missingStreamSource(BooleanInputStream.class)
	br.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	// br.dataStreamSource = missingStreamSource(BooleanInputStream.class)
	br.dataStreamSource = MissingStreamSource() //[*BooleanInputStream]
	br.readOffset = 0
	br.nextBatchSize = 0
	br.presentStream = nil
	br.dataStream = nil
	br.rowGroupOpen = false
}

// @Override
func (br *BooleanColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	br.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, br.column, metadata.PRESENT)
	br.dataStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, br.column, metadata.DATA)
	br.readOffset = 0
	br.nextBatchSize = 0
	br.presentStream = nil
	br.dataStream = nil
	br.rowGroupOpen = false
}

// @Override
func (br *BooleanColumnReader) String() string {
	return util.NewSB().AppendString(br.column.String()).String()
}

// @Override
func (br *BooleanColumnReader) Close() {
	br.memoryContext.Close()
}

// @Override
func (br *BooleanColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(BOOL_COLUMN_INSTANCE_SIZE)
}
