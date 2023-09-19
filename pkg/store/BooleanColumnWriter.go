package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BooleanColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                 metadata.MothColumnId
	kind                     block.Type
	compressed               bool
	dataStream               *BooleanOutputStream
	presentStream            *PresentOutputStream
	rowGroupColumnStatistics *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilder        *metadata.BooleanStatisticsBuilder
	closed                   bool
}

// var (
// 	booleanColumnWriteriNSTANCE_SIZE	int32		= util.SizeOf(&BooleanColumnWriter{})
// 	booleanColumnWritercOLUMN_ENCODING	*metadata.ColumnEncoding	= NewColumnEncoding(DIRECT, 0)
// )

var (
	BOOLEAN_INSTANCE_SIZE   int32                    = util.SizeOf(&BooleanColumnWriter{})
	BOOLEAN_COLUMN_ENCODING *metadata.ColumnEncoding = metadata.NewColumnEncoding(metadata.DIRECT, 0)
)

func NewBooleanColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32) *BooleanColumnWriter {
	br := new(BooleanColumnWriter)

	br.statisticsBuilder = metadata.NewBooleanStatisticsBuilder()
	br.columnId = columnId
	br.kind = kind
	br.compressed = compression != metadata.NONE
	br.dataStream = NewBooleanOutputStream(compression, bufferSize)
	br.presentStream = NewPresentOutputStream(compression, bufferSize)

	br.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return br
}

// @Override
func (br *BooleanColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (br *BooleanColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(br.columnId, BOOLEAN_COLUMN_ENCODING)
}

// @Override
func (br *BooleanColumnWriter) BeginRowGroup() {
	br.presentStream.RecordCheckpoint()
	br.dataStream.RecordCheckpoint()
}

// @Override
func (br *BooleanColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!br.closed)
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		br.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			value := br.kind.GetBoolean(block, position)
			br.dataStream.WriteBoolean(value)
			br.statisticsBuilder.AddValue(value)
		}
	}
}

// @Override
func (br *BooleanColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!br.closed)
	statistics := br.statisticsBuilder.BuildColumnStatistics()
	br.rowGroupColumnStatistics.Add(statistics)
	br.statisticsBuilder = metadata.NewBooleanStatisticsBuilder()
	return util.NewMap(br.columnId, statistics)
}

// @Override
func (br *BooleanColumnWriter) Close() {
	br.closed = true
	br.dataStream.Close()
	br.presentStream.Close()
}

// @Override
func (br *BooleanColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(br.closed)
	return util.NewMap(br.columnId, metadata.MergeColumnStatistics(br.rowGroupColumnStatistics))
}

// @Override
func (br *BooleanColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(br.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := br.dataStream.GetCheckpoints()
	presentCheckpoints := br.presentStream.GetCheckpoints()
	for i := 0; i < br.rowGroupColumnStatistics.Size(); i++ {
		groupId := i
		columnStatistics := br.rowGroupColumnStatistics.Get(groupId)
		dataCheckpoint := dataCheckpoints.Get(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.Get(groupId)
		})
		positions := createBooleanColumnPositionList(br.compressed, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(br.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

// @Override
func (br *BooleanColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.NewArrayList[*StreamDataOutput]()
}

func createBooleanColumnPositionList(compressed bool, dataCheckpoint *BooleanStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (br *BooleanColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(br.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	br.presentStream.GetStreamDataOutput(br.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(br.dataStream.GetStreamDataOutput(br.columnId))
	return outputDataStreams
}

// @Override
func (br *BooleanColumnWriter) GetBufferedBytes() int64 {
	return br.dataStream.GetBufferedBytes() + br.presentStream.GetBufferedBytes()
}

// @Override
func (br *BooleanColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(BOOLEAN_INSTANCE_SIZE) + br.dataStream.GetRetainedBytes() + br.presentStream.GetRetainedBytes()
	for _, statistics := range br.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (br *BooleanColumnWriter) Reset() {
	br.closed = false
	br.dataStream.Reset()
	br.presentStream.Reset()
	br.rowGroupColumnStatistics.Clear()
	br.statisticsBuilder = metadata.NewBooleanStatisticsBuilder()
}
