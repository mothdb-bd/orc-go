package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TimeColumnReader struct {
	//继承
	LongColumnReader
}

func NewTimeColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *TimeColumnReader {
	tr := new(TimeColumnReader)

	tr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]()
	tr.dataStreamSource = MissingStreamSource()    // [LongInputStream]()

	tr.shortNonNullValueTemp = make([]int16, 0)
	tr.intNonNullValueTemp = make([]int32, 0)
	tr.longNonNullValueTemp = make([]int64, 0)

	tr.kind = kind
	tr.column = column
	tr.memoryContext = memoryContext
	return tr
}

// @Override
func (tr *TimeColumnReader) maybeTransformValues(values []int64, nextBatchSize int32) {
	for i := util.INT32_ZERO; i < nextBatchSize; i++ {
		values[i] *= int64(block.TTS_PICOSECONDS_PER_MICROSECOND)
	}
}

// 继承ColumnReader
func (tr *TimeColumnReader) ReadBlock() block.Block {
	return tr.LongColumnReader.ReadBlock()
}

func (tr *TimeColumnReader) PrepareNextRead(batchSize int32) {
	tr.LongColumnReader.PrepareNextRead(batchSize)
}

func (tr *TimeColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	tr.LongColumnReader.StartStripe(fileTimeZone, dictionaryStreamSources, encoding)
}

func (tr *TimeColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	tr.LongColumnReader.StartRowGroup(dataStreamSources)
}

func (tr *TimeColumnReader) GetRetainedSizeInBytes() int64 {
	return tr.LongColumnReader.GetRetainedSizeInBytes()
}

// @Override
func (tr *TimeColumnReader) Close() {
	tr.memoryContext.Close()
}
