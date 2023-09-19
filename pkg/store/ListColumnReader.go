package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var LIST_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&ListColumnReader{})

type ListColumnReader struct {
	// 继承
	ColumnReader

	elementType         block.Type
	column              *MothColumn
	blockFactory        *MothBlockFactory
	elementColumnReader ColumnReader
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	lengthStreamSource  InputStreamSource // [LongInputStream] //@Nullable
	lengthStream        LongInputStream
	rowGroupOpen        bool
}

func NewListColumnReader(kind block.Type, column *MothColumn, memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) *ListColumnReader {
	lr := new(ListColumnReader)

	lr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	lr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]

	lr.elementType = (kind.(*block.ArrayType)).GetElementType()
	lr.column = column
	lr.blockFactory = blockFactory
	lr.elementColumnReader = CreateColumnReader(lr.elementType, column.GetNestedColumns().Get(0), FullyProjectedLayout(), memoryContext, blockFactory, fieldMapperFactory)
	return lr
}

// @Override
func (lr *ListColumnReader) PrepareNextRead(batchSize int32) {
	lr.readOffset += lr.nextBatchSize
	lr.nextBatchSize = batchSize
}

// @Override
func (lr *ListColumnReader) ReadBlock() block.Block {
	if !lr.rowGroupOpen {
		lr.openRowGroup()
	}
	if lr.readOffset > 0 {
		if lr.presentStream != nil {
			lr.readOffset = lr.presentStream.CountBitsSet(lr.readOffset)
		}
		if lr.readOffset > 0 {
			if lr.lengthStream == nil {
				panic("Value is not null but data stream is not present")
			}
			elementSkipSize := lr.lengthStream.Sum(lr.readOffset)
			i, _ := util.ToInt32Exact(elementSkipSize)
			lr.elementColumnReader.PrepareNextRead(i)
		}
	}
	offsetVector := make([]int32, lr.nextBatchSize+1)
	var nullVector []bool = nil
	if lr.presentStream == nil {
		if lr.lengthStream == nil {
			panic("Value is not null but data stream is not present")
		}
		lr.lengthStream.Next3(offsetVector, lr.nextBatchSize)
	} else {
		nullVector = make([]bool, lr.nextBatchSize)
		nullValues := lr.presentStream.GetUnsetBits(lr.nextBatchSize, nullVector)
		if nullValues != lr.nextBatchSize {
			if lr.lengthStream == nil {
				panic("Value is not null but data stream is not present")
			}
			lr.lengthStream.Next3(offsetVector, lr.nextBatchSize-nullValues)
			UnpackLengthNulls(offsetVector, nullVector, lr.nextBatchSize-nullValues)
		}
	}
	ConvertLengthVectorToOffsetVector(offsetVector)
	elementCount := offsetVector[util.Lens(offsetVector)-1]
	var elements block.Block
	if elementCount > 0 {
		lr.elementColumnReader.PrepareNextRead(elementCount)
		elements = lr.blockFactory.CreateBlock(elementCount, lr.elementColumnReader, true)
	} else {
		elements = lr.elementType.CreateBlockBuilder2(nil, 0).Build()
	}
	arrayBlock := block.FromElementBlock(lr.nextBatchSize, optional.OfNullable(nullVector), offsetVector, elements)
	lr.readOffset = 0
	lr.nextBatchSize = 0
	return arrayBlock
}

func (lr *ListColumnReader) openRowGroup() {
	pe := lr.presentStreamSource.OpenStream()
	if pe != nil {
		lr.presentStream = pe.(*BooleanInputStream)
	} else {
		lr.presentStream = nil
	}

	le := lr.lengthStreamSource.OpenStream()
	if le != nil {
		lr.lengthStream = le.(LongInputStream)
	} else {
		lr.lengthStream = nil
	}

	lr.rowGroupOpen = true
}

// @Override
func (lr *ListColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	lr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	lr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]
	lr.readOffset = 0
	lr.nextBatchSize = 0
	lr.presentStream = nil
	lr.lengthStream = nil
	lr.rowGroupOpen = false
	lr.elementColumnReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
}

// @Override
func (lr *ListColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	lr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, lr.column, metadata.PRESENT)
	lr.lengthStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, lr.column, metadata.LENGTH)
	lr.readOffset = 0
	lr.nextBatchSize = 0
	lr.presentStream = nil
	lr.lengthStream = nil
	lr.rowGroupOpen = false
	lr.elementColumnReader.StartRowGroup(dataStreamSources)
}

// @Override
func (lr *ListColumnReader) String() string {
	return util.NewSB().AppendString(lr.column.String()).String()
}

// @Override
func (lr *ListColumnReader) Close() {
	// closer := Closer.create()
	// closer.register(elementColumnReader.close)

	// TODO FIX implement close method
	lr.elementColumnReader.Close()
}

// @Override
func (lr *ListColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(LIST_COLUMN_READER_INSTANCE_SIZE) + lr.elementColumnReader.GetRetainedSizeInBytes()
}
