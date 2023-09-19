package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	STRUCT_INSTANCE_SIZE   int32                    = util.SizeOf(&StructColumnWriter{})
	STRUCT_COLUMN_ENCODING *metadata.ColumnEncoding = metadata.NewColumnEncoding(metadata.DIRECT, 0)
)

type StructColumnWriter struct {
	//继承
	ColumnWriter

	columnId                 metadata.MothColumnId
	compressed               bool
	presentStream            *PresentOutputStream
	structFields             *util.ArrayList[ColumnWriter]
	rowGroupColumnStatistics *util.ArrayList[*metadata.ColumnStatistics]
	nonNullValueCount        int32
	closed                   bool
}

func NewStructColumnWriter(columnId metadata.MothColumnId, compression metadata.CompressionKind, bufferSize int32, structFields *util.ArrayList[ColumnWriter]) *StructColumnWriter {
	sr := new(StructColumnWriter)
	sr.columnId = columnId
	sr.compressed = compression != metadata.NONE
	sr.structFields = structFields
	sr.presentStream = NewPresentOutputStream(compression, bufferSize)
	sr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return sr
}

// @Override
func (sr *StructColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	nestedColumnWriters := util.NewArrayList[ColumnWriter]()
	for _, structField := range sr.structFields.ToArray() {
		nestedColumnWriters.Add(structField).AddAll(structField.GetNestedColumnWriters())
	}
	return nestedColumnWriters
}

// @Override
func (sr *StructColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	// encodings.put(columnId, COLUMN_ENCODING)
	encodings := util.NewMap(sr.columnId, STRUCT_COLUMN_ENCODING)

	// sr.structFields.Stream().Map(ColumnWriter.getColumnEncodings).forEach(encodings.putAll)
	for _, structField := range sr.structFields.ToArray() {
		util.PutAll(encodings, structField.GetColumnEncodings())
	}

	return encodings
}

// @Override
func (sr *StructColumnWriter) BeginRowGroup() {
	sr.presentStream.RecordCheckpoint()
	sr.structFields.ForEach(ColumnWriter.BeginRowGroup)
}

// @Override
func (sr *StructColumnWriter) WriteBlock(b block.Block) {
	util.CheckState(!sr.closed)
	util.CheckArgument2(b.GetPositionCount() > 0, "Block is empty")
	columnarRow := block.ToColumnarRow(b)
	sr.writeColumnarRow(columnarRow)
}

func (sr *StructColumnWriter) writeColumnarRow(columnarRow *block.ColumnarRow) {
	for position := util.INT32_ZERO; position < columnarRow.GetPositionCount(); position++ {
		present := !columnarRow.IsNull(position)
		sr.presentStream.WriteBoolean(present)
		if present {
			sr.nonNullValueCount++
		}
	}
	for i := util.INT32_ZERO; i < sr.structFields.SizeInt32(); i++ {
		columnWriter := sr.structFields.GetByInt32(i)
		fieldBlock := columnarRow.GetField(i)
		if fieldBlock.GetPositionCount() > 0 {
			columnWriter.WriteBlock(fieldBlock)
		}
	}
}

// @Override
func (sr *StructColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!sr.closed)
	statistics := metadata.NewColumnStatistics(int64(sr.nonNullValueCount), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	sr.rowGroupColumnStatistics.Add(statistics)
	sr.nonNullValueCount = 0
	// columnStatistics.put(columnId, statistics)
	columnStatistics := util.NewMap(sr.columnId, statistics)

	// sr.structFields.Stream().Map(ColumnWriter.FinishRowGroup).forEach(columnStatistics.putAll)
	for _, structField := range sr.structFields.ToArray() {
		util.PutAll(columnStatistics, structField.FinishRowGroup())
	}

	return columnStatistics
}

// @Override
func (sr *StructColumnWriter) Close() {
	sr.closed = true
	sr.structFields.ForEach(ColumnWriter.Close)
	sr.presentStream.Close()
}

// @Override
func (sr *StructColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(sr.closed)
	// columnStatistics.put(sr.columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics))
	columnStatistics := util.NewMap(sr.columnId, metadata.MergeColumnStatistics(sr.rowGroupColumnStatistics))

	// sr.structFields.stream().Map(ColumnWriter.getColumnStripeStatistics).forEach(columnStatistics.putAll)

	for _, structField := range sr.structFields.ToArray() {
		util.PutAll(columnStatistics, structField.GetColumnStripeStatistics())
	}

	return columnStatistics
}

// @Override
func (sr *StructColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	presentCheckpoints := sr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < sr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := sr.rowGroupColumnStatistics.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createStructColumnPositionList(sr.compressed, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(sr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)

	// indexStreams.Add(NewStreamDataOutput(slice, stream))
	indexStreams := util.NewArrayList(NewStreamDataOutput(slice, stream))

	for _, structField := range sr.structFields.ToArray() {
		indexStreams.AddAll(structField.GetIndexStreams(metadataWriter))
		indexStreams.AddAll(structField.GetBloomFilters(metadataWriter))
	}
	return indexStreams
}

// @Override
func (sr *StructColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.EMPTY_LIST[*StreamDataOutput]()
}

func createStructColumnPositionList(compressed bool, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	return positionList
}

// @Override
func (sr *StructColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	sr.presentStream.GetStreamDataOutput(sr.columnId).IfPresent(
		func(s *StreamDataOutput) {
			outputDataStreams.Add(s)
		})
	for _, structField := range sr.structFields.ToArray() {
		outputDataStreams.AddAll(structField.GetDataStreams())
	}
	return outputDataStreams
}

// @Override
func (sr *StructColumnWriter) GetBufferedBytes() int64 {
	bufferedBytes := sr.presentStream.GetBufferedBytes()
	for _, structField := range sr.structFields.ToArray() {
		bufferedBytes += structField.GetBufferedBytes()
	}
	return bufferedBytes
}

// @Override
func (sr *StructColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(STRUCT_INSTANCE_SIZE) + sr.presentStream.GetRetainedBytes()
	for _, structField := range sr.structFields.ToArray() {
		retainedBytes += structField.GetRetainedBytes()
	}
	for _, statistics := range sr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (sr *StructColumnWriter) Reset() {
	sr.closed = false
	sr.presentStream.Reset()
	sr.structFields.ForEach(ColumnWriter.Reset)
	sr.rowGroupColumnStatistics.Clear()
	sr.nonNullValueCount = 0
}
