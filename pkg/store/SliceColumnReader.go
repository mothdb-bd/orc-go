package store

import (
	"fmt"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var SLICE_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&SliceColumnReader{})

type SliceColumnReader struct {
	// 继承
	ColumnReader

	column           *MothColumn
	directReader     *SliceDirectColumnReader
	dictionaryReader *SliceDictionaryColumnReader
	currentReader    ColumnReader
}

func NewSliceColumnReader(kind block.Type, column *MothColumn, memoryContext memory.AggregatedMemoryContext) *SliceColumnReader {
	sr := new(SliceColumnReader)

	sr.column = column
	maxCodePointCount := getMaxCodePointCount(kind)
	_, charType := kind.(*block.CharType)
	sr.directReader = NewSliceDirectColumnReader(column, maxCodePointCount, charType)
	sr.dictionaryReader = NewSliceDictionaryColumnReader(column, memoryContext.NewLocalMemoryContext("SliceColumnReader"), maxCodePointCount, charType)
	return sr
}

// @Override
func (sr *SliceColumnReader) ReadBlock() block.Block {
	return sr.currentReader.ReadBlock()
}

// @Override
func (sr *SliceColumnReader) PrepareNextRead(batchSize int32) {
	sr.currentReader.PrepareNextRead(batchSize)
}

// @Override
func (sr *SliceColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	columnEncodingKind := encoding.Get(sr.column.GetColumnId()).GetColumnEncodingKind()
	if columnEncodingKind == metadata.DIRECT || columnEncodingKind == metadata.DIRECT_V2 {
		sr.currentReader = sr.directReader
	} else if columnEncodingKind == metadata.DICTIONARY || columnEncodingKind == metadata.DICTIONARY_V2 {
		sr.currentReader = sr.dictionaryReader
	} else {
		panic(fmt.Sprintf("Unsupported encoding %d", columnEncodingKind))
	}
	sr.currentReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
}

// @Override
func (sr *SliceColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	sr.currentReader.StartRowGroup(dataStreamSources)
}

// @Override
func (sr *SliceColumnReader) ToString() string {
	return util.NewSB().AppendString(sr.column.String()).String()
}

func getMaxCodePointCount(kind block.Type) int32 {
	varcharType, flag := kind.(*block.VarcharType)
	if flag {
		return util.Ternary(varcharType.IsUnbounded(), -1, varcharType.GetBoundedLength())
	}
	charType, cflag := kind.(*block.CharType)
	if cflag {
		return charType.GetLength()
	}
	_, flag = kind.(*block.VarbinaryType)
	if flag {
		return -1
	}
	panic(fmt.Sprintf("Unsupported encoding %s", kind.GetDisplayName()))
}

func ComputeTruncatedLength(slice *slice.Slice, offset int32, length int32, maxCodePointCount int32, isCharType bool) int32 {
	if isCharType {
		return block.ByteCountWithoutTrailingSpace(slice, offset, length, maxCodePointCount)
	}
	if maxCodePointCount >= 0 && length > maxCodePointCount {
		return block.ByteCount(slice, offset, length, maxCodePointCount)
	}
	return length
}

// @Override
func (sr *SliceColumnReader) Close() {
	// closer := Closer.create()
	// closer.register(directReader.close)
	// closer.register(dictionaryReader.close)
	// TODO FIX
	sr.directReader.Close()
	sr.dictionaryReader.Close()
}

// @Override
func (sr *SliceColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(SLICE_COLUMN_READER_INSTANCE_SIZE) + sr.directReader.GetRetainedSizeInBytes() + sr.dictionaryReader.GetRetainedSizeInBytes()
}
