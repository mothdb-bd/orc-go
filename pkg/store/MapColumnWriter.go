package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MAP_INSTANCE_SIZE int32 = util.SizeOf(&MapColumnWriter{})

type MapColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                 metadata.MothColumnId
	compressed               bool
	columnEncoding           *metadata.ColumnEncoding
	lengthStream             LongOutputStream
	presentStream            *PresentOutputStream
	keyWriter                ColumnWriter
	valueWriter              ColumnWriter
	rowGroupColumnStatistics *util.ArrayList[*metadata.ColumnStatistics]
	nonNullValueCount        int32
	closed                   bool
}

func NewMapColumnWriter(columnId metadata.MothColumnId, compression metadata.CompressionKind, bufferSize int32, keyWriter ColumnWriter, valueWriter ColumnWriter) *MapColumnWriter {
	mr := new(MapColumnWriter)
	mr.columnId = columnId
	mr.compressed = compression != metadata.NONE
	mr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	mr.keyWriter = keyWriter
	mr.valueWriter = valueWriter
	mr.lengthStream = CreateLengthOutputStream(compression, bufferSize)
	mr.presentStream = NewPresentOutputStream(compression, bufferSize)
	mr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return mr
}

// @Override
func (mr *MapColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	list := util.NewArrayList[ColumnWriter]()
	list.Add(mr.keyWriter).AddAll(mr.keyWriter.GetNestedColumnWriters()).Add(mr.valueWriter).AddAll(mr.valueWriter.GetNestedColumnWriters())
	return list
}

// @Override
func (mr *MapColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	// encodings.put(columnId,columnEncoding )
	encodings := util.NewMap(mr.columnId, mr.columnEncoding)
	// encodings.putAll(keyWriter.getColumnEncodings())
	// encodings.putAll(valueWriter.getColumnEncodings())
	util.PutAll(encodings, mr.keyWriter.GetColumnEncodings())
	util.PutAll(encodings, mr.valueWriter.GetColumnEncodings())
	return encodings
}

// @Override
func (mr *MapColumnWriter) BeginRowGroup() {
	mr.lengthStream.RecordCheckpoint()
	mr.presentStream.RecordCheckpoint()
	mr.keyWriter.BeginRowGroup()
	mr.valueWriter.BeginRowGroup()
}

// @Override
func (mr *MapColumnWriter) WriteBlock(b block.Block) {
	util.CheckState(!mr.closed)
	util.CheckArgument2(b.GetPositionCount() > 0, "Block is empty")
	columnarMap := block.ToColumnarMap(b)
	mr.writeColumnarMap(columnarMap)
}

func (mr *MapColumnWriter) writeColumnarMap(columnarMap *block.ColumnarMap) {
	for position := util.INT32_ZERO; position < columnarMap.GetPositionCount(); position++ {
		present := !columnarMap.IsNull(position)
		mr.presentStream.WriteBoolean(present)
		if present {
			mr.nonNullValueCount++
			mr.lengthStream.WriteLong(int64(columnarMap.GetEntryCount(position)))
		}
	}
	keysBlock := columnarMap.GetKeysBlock()
	if keysBlock.GetPositionCount() > 0 {
		mr.keyWriter.WriteBlock(keysBlock)
		mr.valueWriter.WriteBlock(columnarMap.GetValuesBlock())
	}
}

// @Override
func (mr *MapColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!mr.closed)
	statistics := metadata.NewColumnStatistics(int64(mr.nonNullValueCount), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mr.rowGroupColumnStatistics.Add(statistics)
	mr.nonNullValueCount = 0
	// columnStatistics.put(columnId, statistics)
	columnStatistics := util.NewMap(mr.columnId, statistics)

	// columnStatistics.putAll(keyWriter.finishRowGroup())
	// columnStatistics.putAll(valueWriter.finishRowGroup())
	util.PutAll(columnStatistics, mr.keyWriter.FinishRowGroup())
	util.PutAll(columnStatistics, mr.valueWriter.FinishRowGroup())
	return columnStatistics
}

// @Override
func (mr *MapColumnWriter) Close() {
	mr.closed = true
	mr.keyWriter.Close()
	mr.valueWriter.Close()
	mr.lengthStream.Close()
	mr.presentStream.Close()
}

// @Override
func (mr *MapColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(mr.closed)
	// columnStatistics.put(mr.columnId, metadata.MergeColumnStatistics(mr.rowGroupColumnStatistics))
	columnStatistics := util.NewMap(mr.columnId, metadata.MergeColumnStatistics(mr.rowGroupColumnStatistics))
	// columnStatistics.putAll(mr.keyWriter.GetColumnStripeStatistics())
	// columnStatistics.putAll(mr.valueWriter.GetColumnStripeStatistics())
	util.PutAll(columnStatistics, mr.keyWriter.GetColumnStripeStatistics())
	util.PutAll(columnStatistics, mr.valueWriter.GetColumnStripeStatistics())
	return columnStatistics
}

// @Override
func (mr *MapColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(mr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	lengthCheckpoints := mr.lengthStream.GetCheckpoints()
	presentCheckpoints := mr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < mr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := mr.rowGroupColumnStatistics.GetByInt32(groupId)
		lengthCheckpoint := lengthCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createArrayColumnPositionList(mr.compressed, lengthCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(mr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	indexStreams := util.NewArrayList[*StreamDataOutput]()
	indexStreams.Add(NewStreamDataOutput(slice, stream))
	indexStreams.AddAll(mr.keyWriter.GetIndexStreams(metadataWriter))
	indexStreams.AddAll(mr.keyWriter.GetBloomFilters(metadataWriter))
	indexStreams.AddAll(mr.valueWriter.GetIndexStreams(metadataWriter))
	indexStreams.AddAll(mr.valueWriter.GetBloomFilters(metadataWriter))
	return indexStreams
}

func createArrayColumnPositionList(compressed bool, lengthCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(lengthCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (mr *MapColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (mr *MapColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(mr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	mr.presentStream.GetStreamDataOutput(mr.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(mr.lengthStream.GetStreamDataOutput(mr.columnId))
	outputDataStreams.AddAll(mr.keyWriter.GetDataStreams())
	outputDataStreams.AddAll(mr.valueWriter.GetDataStreams())
	return outputDataStreams
}

// @Override
func (mr *MapColumnWriter) GetBufferedBytes() int64 {
	return mr.lengthStream.GetBufferedBytes() + mr.presentStream.GetBufferedBytes() + mr.keyWriter.GetBufferedBytes() + mr.valueWriter.GetBufferedBytes()
}

// @Override
func (mr *MapColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(MAP_INSTANCE_SIZE) + mr.lengthStream.GetRetainedBytes() + mr.presentStream.GetRetainedBytes() + mr.keyWriter.GetRetainedBytes() + mr.valueWriter.GetRetainedBytes()
	for _, statistics := range mr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (mr *MapColumnWriter) Reset() {
	mr.closed = false
	mr.lengthStream.Reset()
	mr.presentStream.Reset()
	mr.keyWriter.Reset()
	mr.valueWriter.Reset()
	mr.rowGroupColumnStatistics.Clear()
	mr.nonNullValueCount = 0
}
