package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MAP_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&MapColumnReader{})

type MapColumnReader struct {
	// 继承
	ColumnReader

	kind                *block.MapType
	column              *MothColumn
	blockFactory        *MothBlockFactory
	keyColumnReader     ColumnReader
	valueColumnReader   ColumnReader
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	lengthStreamSource  InputStreamSource // [LongInputStream] //@Nullable
	lengthStream        LongInputStream
	rowGroupOpen        bool
}

func NewMapColumnReader(kind block.Type, column *MothColumn, memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) *MapColumnReader {
	mr := new(MapColumnReader)

	mr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	mr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]

	mr.kind = kind.(*block.MapType)
	mr.column = column
	mr.blockFactory = blockFactory
	mr.keyColumnReader = CreateColumnReader(mr.kind.GetKeyType(), column.GetNestedColumns().Get(0), FullyProjectedLayout(), memoryContext, blockFactory, fieldMapperFactory)
	mr.valueColumnReader = CreateColumnReader(mr.kind.GetValueType(), column.GetNestedColumns().Get(1), FullyProjectedLayout(), memoryContext, blockFactory, fieldMapperFactory)
	return mr
}

// @Override
func (mr *MapColumnReader) PrepareNextRead(batchSize int32) {
	mr.readOffset += mr.nextBatchSize
	mr.nextBatchSize = batchSize
}

// @Override
func (mr *MapColumnReader) ReadBlock() block.Block {
	if !mr.rowGroupOpen {
		mr.openRowGroup()
	}
	if mr.readOffset > 0 {
		if mr.presentStream != nil {
			mr.readOffset = mr.presentStream.CountBitsSet(mr.readOffset)
		}
		if mr.readOffset > 0 {
			if mr.lengthStream == nil {
				panic("Value is not null but data stream is not present")
			}
			entrySkipSize := mr.lengthStream.Sum(mr.readOffset)
			kv, _ := util.ToInt32Exact(entrySkipSize)
			mr.keyColumnReader.PrepareNextRead(kv)
			mr.valueColumnReader.PrepareNextRead(kv)
		}
	}
	offsetVector := make([]int32, mr.nextBatchSize+1)
	var nullVector []bool = nil
	if mr.presentStream == nil {
		if mr.lengthStream == nil {
			panic("Value is not null but data stream is not present")
		}
		mr.lengthStream.Next3(offsetVector, mr.nextBatchSize)
	} else {
		nullVector = make([]bool, mr.nextBatchSize)
		nullValues := mr.presentStream.GetUnsetBits(mr.nextBatchSize, nullVector)
		if nullValues != mr.nextBatchSize {
			if mr.lengthStream == nil {
				panic("Value is not null but data stream is not present")
			}
			mr.lengthStream.Next3(offsetVector, mr.nextBatchSize-nullValues)
			UnpackLengthNulls(offsetVector, nullVector, mr.nextBatchSize-nullValues)
		}
	}
	entryCount := util.INT32_ZERO
	for i := util.INT32_ZERO; i < util.Lens(offsetVector)-1; i++ {
		entryCount += offsetVector[i]
	}
	var keys block.Block
	var values block.Block
	if entryCount > 0 {
		mr.keyColumnReader.PrepareNextRead(entryCount)
		mr.valueColumnReader.PrepareNextRead(entryCount)
		keys = mr.keyColumnReader.ReadBlock()
		values = mr.blockFactory.CreateBlock(entryCount, mr.valueColumnReader, true)
	} else {
		keys = mr.kind.GetKeyType().CreateBlockBuilder2(nil, 0).Build()
		values = mr.kind.GetValueType().CreateBlockBuilder2(nil, 1).Build()
	}
	keyValueBlock := createKeyValueBlock(mr.nextBatchSize, keys, values, offsetVector)
	ConvertLengthVectorToOffsetVector(offsetVector)
	mr.readOffset = 0
	mr.nextBatchSize = 0
	return mr.kind.CreateBlockFromKeyValue(optional.OfNullable(nullVector), offsetVector, keyValueBlock[0], keyValueBlock[1])
}

func createKeyValueBlock(positionCount int32, keys block.Block, values block.Block, lengths []int32) []block.Block {
	if !hasNull(keys) {
		return []block.Block{keys, values}
	}
	nonNullPositions := block.NewIntArrayList(keys.GetPositionCount())
	position := util.INT32_ZERO
	for mapIndex := util.INT32_ZERO; mapIndex < positionCount; mapIndex++ {
		length := lengths[mapIndex]
		for entryIndex := util.INT32_ZERO; entryIndex < length; entryIndex++ {
			if keys.IsNull(position) {
				lengths[mapIndex]--
			} else {
				nonNullPositions.Add(position)
			}
			position++
		}
	}
	newKeys := keys.CopyPositions(nonNullPositions.Elements(), 0, nonNullPositions.Size())
	newValues := values.CopyPositions(nonNullPositions.Elements(), 0, nonNullPositions.Size())
	return []block.Block{newKeys, newValues}
}

func hasNull(keys block.Block) bool {
	for position := util.INT32_ZERO; position < keys.GetPositionCount(); position++ {
		if keys.IsNull(position) {
			return true
		}
	}
	return false
}

func (mr *MapColumnReader) openRowGroup() {
	pe := mr.presentStreamSource.OpenStream()
	if pe != nil {
		mr.presentStream = pe.(*BooleanInputStream)
	} else {
		mr.presentStream = nil
	}

	le := mr.lengthStreamSource.OpenStream()
	if le != nil {
		mr.lengthStream = le.(LongInputStream)
	} else {
		mr.lengthStream = nil
	}

	mr.rowGroupOpen = true
}

// @Override
func (mr *MapColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	mr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	mr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]
	mr.readOffset = 0
	mr.nextBatchSize = 0
	mr.presentStream = nil
	mr.lengthStream = nil
	mr.rowGroupOpen = false
	mr.keyColumnReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
	mr.valueColumnReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
}

// @Override
func (mr *MapColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	mr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, mr.column, metadata.PRESENT)
	mr.lengthStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, mr.column, metadata.LENGTH)
	mr.readOffset = 0
	mr.nextBatchSize = 0
	mr.presentStream = nil
	mr.lengthStream = nil
	mr.rowGroupOpen = false
	mr.keyColumnReader.StartRowGroup(dataStreamSources)
	mr.valueColumnReader.StartRowGroup(dataStreamSources)
}

// @Override
func (mr *MapColumnReader) ToString() string {
	return util.NewSB().AppendString(mr.column.String()).String()
}

// @Override
func (mr *MapColumnReader) Close() {
	// closer := Closer.create()
	// closer.register(keyColumnReader.close)
	// closer.register(valueColumnReader.close)
	// TODO FIX
	mr.keyColumnReader.Close()
	mr.valueColumnReader.Close()
}

// @Override
func (mr *MapColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(MAP_COLUMN_READER_INSTANCE_SIZE) + mr.keyColumnReader.GetRetainedSizeInBytes() + mr.valueColumnReader.GetRetainedSizeInBytes()
}
