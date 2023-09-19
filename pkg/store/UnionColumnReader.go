package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var UNION_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&UnionColumnReader{})

type UnionColumnReader struct {
	column              *MothColumn
	blockFactory        *MothBlockFactory
	kind                *block.RowType
	fieldReaders        *util.ArrayList[ColumnReader]
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource   // [*BooleanInputStream]
	dataStreamSource    InputStreamSource   // [*ByteInputStream] //@Nullable
	presentStream       *BooleanInputStream //@Nullable
	dataStream          *ByteInputStream
	rowGroupOpen        bool
}

func NewUnionColumnReader(kind block.Type, column *MothColumn, memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) *UnionColumnReader {
	ur := new(UnionColumnReader)

	ur.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]()
	ur.dataStreamSource = MissingStreamSource()    // [*ByteInputStream]()

	ur.kind = kind.(*block.RowType)
	ur.column = column
	ur.blockFactory = blockFactory
	fieldReadersBuilder := util.NewArrayList[ColumnReader]()
	fields := column.GetNestedColumns()
	for i := 0; i < fields.Size(); i++ {
		fieldReadersBuilder.Add(CreateColumnReader(kind.GetTypeParameters().Get(i+1), fields.Get(i), FullyProjectedLayout(), memoryContext, blockFactory, fieldMapperFactory))
	}
	ur.fieldReaders = fieldReadersBuilder
	return ur
}

// @Override
func (ur *UnionColumnReader) PrepareNextRead(batchSize int32) {
	ur.readOffset += ur.nextBatchSize
	ur.nextBatchSize = batchSize
}

// @Override
func (ur *UnionColumnReader) ReadBlock() block.Block {
	if !ur.rowGroupOpen {
		ur.openRowGroup()
	}
	if ur.readOffset > 0 {
		if ur.presentStream != nil {
			ur.readOffset = ur.presentStream.CountBitsSet(ur.readOffset)
		}
		if ur.readOffset > 0 {
			if ur.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			readOffsets := make([]int32, ur.fieldReaders.Size())
			for _, tag := range ur.dataStream.Next2(ur.readOffset) {
				readOffsets[tag]++
			}
			for i := 0; i < ur.fieldReaders.Size(); i++ {
				ur.fieldReaders.Get(i).PrepareNextRead(readOffsets[i])
			}
		}
	}
	var nullVector []bool = nil
	var blocks []block.Block
	if ur.presentStream == nil {
		blocks = ur.getBlocks(ur.nextBatchSize)
	} else {
		nullVector = make([]bool, ur.nextBatchSize)
		nullValues := ur.presentStream.GetUnsetBits(ur.nextBatchSize, nullVector)
		if nullValues != ur.nextBatchSize {
			blocks = ur.getBlocks(ur.nextBatchSize - nullValues)
		} else {
			typeParameters := ur.kind.GetTypeParameters()
			blocks = make([]block.Block, typeParameters.Size()+1)
			blocks[0] = block.TINYINT.CreateBlockBuilder2(nil, 0).Build()
			for i := 0; i < typeParameters.Size(); i++ {
				blocks[i+1] = typeParameters.Get(i).CreateBlockBuilder2(nil, 0).Build()
			}
		}
	}
	// verify(Arrays.stream(blocks).mapToInt(Block.getPositionCount).distinct().count() == 1)
	rowBlock := block.FromFieldBlocks(ur.nextBatchSize, optional.OfNullable(nullVector), blocks)
	ur.readOffset = 0
	ur.nextBatchSize = 0
	return rowBlock
}

func (ur *UnionColumnReader) openRowGroup() {
	pe := ur.presentStreamSource.OpenStream()
	if pe != nil {
		ur.presentStream = pe.(*BooleanInputStream)
	} else {
		ur.presentStream = nil
	}

	de := ur.dataStreamSource.OpenStream()
	if de != nil {
		ur.dataStream = de.(*ByteInputStream)
	} else {
		ur.dataStream = nil
	}

	ur.rowGroupOpen = true
}

// @Override
func (ur *UnionColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	ur.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]()
	ur.dataStreamSource = MissingStreamSource()    //[*ByteInputStream]()
	ur.readOffset = 0
	ur.nextBatchSize = 0
	ur.presentStream = nil
	ur.dataStream = nil
	ur.rowGroupOpen = false
	for i := 0; i < ur.fieldReaders.Size(); i++ {
		fieldReader := ur.fieldReaders.Get(i)
		fieldReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
	}
}

// @Override
func (ur *UnionColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	ur.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, ur.column, metadata.PRESENT)
	ur.dataStreamSource = GetInputStreamSource[*ByteInputStream](dataStreamSources, ur.column, metadata.DATA)
	ur.readOffset = 0
	ur.nextBatchSize = 0
	ur.presentStream = nil
	ur.dataStream = nil
	ur.rowGroupOpen = false
	for i := 0; i < ur.fieldReaders.Size(); i++ {
		fieldReader := ur.fieldReaders.Get(i)
		fieldReader.StartRowGroup(dataStreamSources)
	}
}

// @Override
func (ur *UnionColumnReader) ToString() string {
	return util.NewSB().AppendString(ur.column.String()).String()
}

func (ur *UnionColumnReader) getBlocks(positionCount int32) []block.Block {
	if ur.dataStream == nil {
		panic("Value is not null but data stream is missing")
	}
	blocks := make([]block.Block, ur.fieldReaders.Size()+1)
	tags := ur.dataStream.Next2(positionCount)
	blocks[0] = block.NewByteArrayBlock(positionCount, optional.Empty[[]bool](), tags)
	valueIsNonNull := make([][]bool, ur.fieldReaders.Size(), positionCount)
	nonNullValueCount := make([]int32, ur.fieldReaders.Size())
	for i := util.INT32_ZERO; i < positionCount; i++ {
		valueIsNonNull[tags[i]][i] = true
		nonNullValueCount[tags[i]]++
	}
	for i := 0; i < ur.fieldReaders.Size(); i++ {
		fieldType := ur.kind.GetTypeParameters().Get(i + 1)
		if nonNullValueCount[i] > 0 {
			reader := ur.fieldReaders.Get(i)
			reader.PrepareNextRead(nonNullValueCount[i])
			rawBlock := ur.blockFactory.CreateBlock(nonNullValueCount[i], reader, true)
			blocks[i+1] = block.NewLazyBlock(positionCount, NewUnpackLazyBlockLoader(rawBlock, fieldType, valueIsNonNull[i]))
		} else {
			blocks[i+1] = block.NewRunLengthEncodedBlock(fieldType.CreateBlockBuilder2(nil, 1).AppendNull().Build(), positionCount)
		}
	}
	return blocks
}

// @Override
func (ur *UnionColumnReader) Close() {
	// closer := Closer.create()
	// for _, structField := range fieldReaders {
	// 	closer.register(structField.close)
	// }
	// TOFO FIX
	for i := 0; i < ur.fieldReaders.Size(); i++ {
		structField := ur.fieldReaders.Get(i)
		structField.Close()
	}
}

// @Override
func (ur *UnionColumnReader) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := int64(UNION_COLUMN_READER_INSTANCE_SIZE)
	for i := 0; i < ur.fieldReaders.Size(); i++ {
		structField := ur.fieldReaders.Get(i)
		retainedSizeInBytes += structField.GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}

type UnpackLazyBlockLoader struct {
	denseBlock     block.Block
	kind           block.Type
	valueIsNonNull []bool
}

func NewUnpackLazyBlockLoader(denseBlock block.Block, kind block.Type, valueIsNonNull []bool) *UnpackLazyBlockLoader {
	ur := new(UnpackLazyBlockLoader)
	ur.denseBlock = denseBlock
	ur.kind = kind
	ur.valueIsNonNull = valueIsNonNull
	return ur
}

// @Override
func (ur *UnpackLazyBlockLoader) Load() block.Block {
	loadedDenseBlock := ur.denseBlock.GetLoadedBlock()
	unpackedBlock := ur.kind.CreateBlockBuilder2(nil, util.Lens(ur.valueIsNonNull))
	denseBlockPosition := util.INT32_ZERO
	for _, isNonNull := range ur.valueIsNonNull {
		if isNonNull {
			ur.kind.AppendTo(loadedDenseBlock, denseBlockPosition, unpackedBlock)
			denseBlockPosition++
		} else {
			unpackedBlock.AppendNull()
		}
	}
	util.CheckState2(denseBlockPosition == loadedDenseBlock.GetPositionCount(), "inconsistency between denseBlock and valueIsNonNull")
	return unpackedBlock.Build()
}
