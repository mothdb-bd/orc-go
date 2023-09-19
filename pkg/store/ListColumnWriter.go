package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var LIST_INSTANCE_SIZE int32 = util.SizeOf(&ListColumnWriter{})

type ListColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                 metadata.MothColumnId
	compressed               bool
	columnEncoding           *metadata.ColumnEncoding
	lengthStream             LongOutputStream
	presentStream            *PresentOutputStream
	elementWriter            ColumnWriter
	rowGroupColumnStatistics *util.ArrayList[*metadata.ColumnStatistics]
	nonNullValueCount        int32
	closed                   bool
}

func NewListColumnWriter(columnId metadata.MothColumnId, compression metadata.CompressionKind, bufferSize int32, elementWriter ColumnWriter) *ListColumnWriter {
	lr := new(ListColumnWriter)
	lr.columnId = columnId
	lr.compressed = compression != metadata.NONE
	lr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	lr.elementWriter = elementWriter
	lr.lengthStream = CreateLengthOutputStream(compression, bufferSize)
	lr.presentStream = NewPresentOutputStream(compression, bufferSize)
	lr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return lr
}

// @Override
func (lr *ListColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	list := util.NewArrayList[ColumnWriter]()
	list.Add(lr.elementWriter)
	list.AddAll(lr.elementWriter.GetNestedColumnWriters())
	return list
}

// @Override
func (lr *ListColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	// encodings.put(columnId, columnEncoding)
	encodings := util.NewMap(lr.columnId, lr.columnEncoding)

	// encodings.putAll(elementWriter.getColumnEncodings())
	util.PutAll(encodings, lr.elementWriter.GetColumnEncodings())
	return encodings
}

// @Override
func (lr *ListColumnWriter) BeginRowGroup() {
	lr.lengthStream.RecordCheckpoint()
	lr.presentStream.RecordCheckpoint()
	lr.elementWriter.BeginRowGroup()
}

// @Override
func (lr *ListColumnWriter) WriteBlock(b block.Block) {
	util.CheckState(!lr.closed)
	util.CheckArgument2(b.GetPositionCount() > 0, "Block is empty")
	columnarArray := block.ToColumnarArray(b)
	lr.writeColumnarArray(columnarArray)
}

func (lr *ListColumnWriter) writeColumnarArray(columnarArray *block.ColumnarArray) {
	for position := util.INT32_ZERO; position < columnarArray.GetPositionCount(); position++ {
		present := !columnarArray.IsNull(position)
		lr.presentStream.WriteBoolean(present)
		if present {
			lr.nonNullValueCount++
			lr.lengthStream.WriteLong(int64(columnarArray.GetLength(position)))
		}
	}
	elementsBlock := columnarArray.GetElementsBlock()
	if elementsBlock.GetPositionCount() > 0 {
		lr.elementWriter.WriteBlock(elementsBlock)
	}
}

// @Override
func (lr *ListColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!lr.closed)
	statistics := metadata.NewColumnStatistics(int64(lr.nonNullValueCount), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	lr.rowGroupColumnStatistics.Add(statistics)
	lr.nonNullValueCount = 0
	// columnStatistics.put(columnId, statistics)
	columnStatistics := util.NewMap(lr.columnId, statistics)

	// columnStatistics.putAll(lr.elementWriter.FinishRowGroup())
	util.PutAll(columnStatistics, lr.elementWriter.FinishRowGroup())
	return columnStatistics
}

// @Override
func (lr *ListColumnWriter) Close() {
	lr.closed = true
	lr.elementWriter.Close()
	lr.lengthStream.Close()
	lr.presentStream.Close()
}

// @Override
func (lr *ListColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(lr.closed)
	// columnStatistics.put(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics))
	columnStatistics := util.NewMap(lr.columnId, metadata.MergeColumnStatistics(lr.rowGroupColumnStatistics))
	// columnStatistics.putAll(elementWriter.getColumnStripeStatistics())
	util.PutAll(columnStatistics, lr.elementWriter.GetColumnStripeStatistics())
	return columnStatistics
}

// @Override
func (lr *ListColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(lr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	lengthCheckpoints := lr.lengthStream.GetCheckpoints()
	presentCheckpoints := lr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < lr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := lr.rowGroupColumnStatistics.GetByInt32(groupId)
		lengthCheckpoint := lengthCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := list_createArrayColumnPositionList(lr.compressed, lengthCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(lr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	indexStreams := util.NewArrayList[*StreamDataOutput]()
	indexStreams.Add(NewStreamDataOutput(slice, stream))
	indexStreams.AddAll(lr.elementWriter.GetIndexStreams(metadataWriter))
	indexStreams.AddAll(lr.elementWriter.GetBloomFilters(metadataWriter))
	return indexStreams
}

func list_createArrayColumnPositionList(compressed bool, lengthCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(lengthCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (lr *ListColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (lr *ListColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(lr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	lr.presentStream.GetStreamDataOutput(lr.columnId).IfPresent(func(st *StreamDataOutput) {
		outputDataStreams.Add(st)
	})
	outputDataStreams.Add(lr.lengthStream.GetStreamDataOutput(lr.columnId))
	outputDataStreams.AddAll(lr.elementWriter.GetDataStreams())
	return outputDataStreams
}

// @Override
func (lr *ListColumnWriter) GetBufferedBytes() int64 {
	return lr.lengthStream.GetBufferedBytes() + lr.presentStream.GetBufferedBytes() + lr.elementWriter.GetBufferedBytes()
}

// @Override
func (lr *ListColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(LIST_INSTANCE_SIZE) + lr.lengthStream.GetRetainedBytes() + lr.presentStream.GetRetainedBytes() + lr.elementWriter.GetRetainedBytes()
	for _, statistics := range lr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (lr *ListColumnWriter) Reset() {
	lr.closed = false
	lr.lengthStream.Reset()
	lr.presentStream.Reset()
	lr.elementWriter.Reset()
	lr.rowGroupColumnStatistics.Clear()
	lr.nonNullValueCount = 0
}
