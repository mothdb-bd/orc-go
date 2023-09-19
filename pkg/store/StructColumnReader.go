package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var STRUCT_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&StructColumnReader{})

type StructColumnReader struct {
	// 继承
	ColumnReader

	column              *MothColumn
	blockFactory        *MothBlockFactory
	structFields        map[string]ColumnReader
	kind                *block.RowType
	fieldNames          *util.ArrayList[string]
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	rowGroupOpen        bool
}

func NewStructColumnReader(kind block.Type, column *MothColumn, readLayout ProjectedLayout, memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) *StructColumnReader {
	sr := new(StructColumnReader)

	sr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]()

	sr.kind = kind.(*block.RowType)
	sr.column = column
	sr.blockFactory = blockFactory
	fieldMapper := fieldMapperFactory.Create(column)
	fieldNames := util.NewArrayList[string]()
	structFields := make(map[string]ColumnReader)

	fs := sr.kind.GetFields()
	for i := 0; i < fs.Size(); i++ {
		field := fs.Get(i)

		fieldName := strings.ToLower(field.GetName().OrElseThrow(fmt.Sprintf("ROW type does not have field names declared: %s", kind)))
		fieldNames.Add(fieldName)
		fieldStream := fieldMapper.Get(fieldName)
		if fieldStream != nil {
			fieldLayout := readLayout.GetFieldLayout(fieldStream)
			if fieldLayout != nil {
				structFields[fieldName] = CreateColumnReader(field.GetType(), fieldStream, fieldLayout, memoryContext, blockFactory, fieldMapperFactory)
			}
		}
	}
	sr.fieldNames = fieldNames
	sr.structFields = structFields
	return sr
}

// @Override
func (sr *StructColumnReader) PrepareNextRead(batchSize int32) {
	sr.readOffset += sr.nextBatchSize
	sr.nextBatchSize = batchSize
}

// @Override
func (sr *StructColumnReader) ReadBlock() block.Block {
	if !sr.rowGroupOpen {
		sr.openRowGroup()
	}
	if sr.readOffset > 0 {
		if sr.presentStream != nil {
			sr.readOffset = sr.presentStream.CountBitsSet(sr.readOffset)
		}
		for _, structField := range sr.structFields {
			structField.PrepareNextRead(sr.readOffset)
		}
	}
	var nullVector []bool = nil
	var blocks []block.Block
	if sr.presentStream == nil {
		blocks = sr.getBlocksForType(sr.nextBatchSize)
	} else {
		nullVector = make([]bool, sr.nextBatchSize)
		nullValues := sr.presentStream.GetUnsetBits(sr.nextBatchSize, nullVector)
		if nullValues != sr.nextBatchSize {
			blocks = sr.getBlocksForType(sr.nextBatchSize - nullValues)
		} else {
			typeParameters := sr.kind.GetTypeParameters()
			blocks = make([]block.Block, typeParameters.Size())
			for i := 0; i < typeParameters.Size(); i++ {
				blocks[i] = typeParameters.Get(i).CreateBlockBuilder2(nil, 0).Build()
			}
		}
	}
	// util.Verify(Arrays.stream(blocks).mapToInt(Block.getPositionCount).distinct().count() == 1)
	rowBlock := block.FromFieldBlocks(sr.nextBatchSize, optional.OfNullable(nullVector), blocks)
	sr.readOffset = 0
	sr.nextBatchSize = 0
	return rowBlock
}

func (sr *StructColumnReader) openRowGroup() {
	pe := sr.presentStreamSource.OpenStream()
	if pe != nil {
		sr.presentStream = pe.(*BooleanInputStream)
	} else {
		sr.presentStream = nil
	}

	sr.rowGroupOpen = true
}

// @Override
func (sr *StructColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	sr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]()
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.rowGroupOpen = false
	for _, structField := range sr.structFields {
		structField.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
	}
}

// @Override
func (sr *StructColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	sr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, sr.column, metadata.PRESENT)
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.rowGroupOpen = false
	for _, structField := range sr.structFields {
		structField.StartRowGroup(dataStreamSources)
	}
}

// @Override
func (sr *StructColumnReader) ToString() string {
	return util.NewSB().AppendString(sr.column.String()).String()
}

func (sr *StructColumnReader) getBlocksForType(positionCount int32) []block.Block {
	blocks := make([]block.Block, sr.fieldNames.Size())
	for i := 0; i < sr.fieldNames.Size(); i++ {
		fieldName := sr.fieldNames.Get(i)
		columnReader := sr.structFields[fieldName]
		if columnReader != nil {
			columnReader.PrepareNextRead(positionCount)
			blocks[i] = sr.blockFactory.CreateBlock(positionCount, columnReader, true)
		} else {
			blocks[i] = block.CreateRunLengthEncodedBlock(sr.kind.GetFields().Get(i).GetType(), nil, positionCount)
		}
	}
	return blocks
}

// @Override
func (sr *StructColumnReader) Close() {
	// closer := Closer.create()
	for _, structField := range sr.structFields {
		// closer.register(structField.close)
		structField.Close()
	}
}

// @Override
func (sr *StructColumnReader) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := int64(STRUCT_COLUMN_READER_INSTANCE_SIZE)
	for _, structField := range sr.structFields {
		retainedSizeInBytes += structField.GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}
