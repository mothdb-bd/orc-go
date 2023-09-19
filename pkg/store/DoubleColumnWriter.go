package store

import (
	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DOUBLE_INSTANCE_SIZE   int32                    = util.SizeOf(&DoubleColumnWriter{})
	DOUBLE_COLUMN_ENCODING *metadata.ColumnEncoding = metadata.NewColumnEncoding(metadata.DIRECT, 0)
)

type DoubleColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                  metadata.MothColumnId
	kind                      block.Type
	compressed                bool
	dataStream                *DoubleOutputStream
	presentStream             *PresentOutputStream
	rowGroupColumnStatistics  *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilderSupplier function.Supplier[*metadata.DoubleStatisticsBuilder]
	statisticsBuilder         *metadata.DoubleStatisticsBuilder
	closed                    bool
}

func NewDoubleColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[*metadata.DoubleStatisticsBuilder]) *DoubleColumnWriter {
	dr := new(DoubleColumnWriter)
	dr.columnId = columnId
	dr.kind = kind
	dr.compressed = compression != metadata.NONE
	dr.dataStream = NewDoubleOutputStream(compression, bufferSize)
	dr.presentStream = NewPresentOutputStream(compression, bufferSize)
	dr.statisticsBuilderSupplier = statisticsBuilderSupplier
	dr.statisticsBuilder = statisticsBuilderSupplier.Get()
	dr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return dr
}

// @Override
func (dr *DoubleColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (dr *DoubleColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(dr.columnId, DOUBLE_COLUMN_ENCODING)
}

// @Override
func (dr *DoubleColumnWriter) BeginRowGroup() {
	dr.presentStream.RecordCheckpoint()
	dr.dataStream.RecordCheckpoint()
}

// @Override
func (dr *DoubleColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!dr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		dr.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			value := dr.kind.GetDouble(block, position)
			dr.statisticsBuilder.AddValue(value)
			dr.dataStream.WriteDouble(value)
		}
	}
}

// @Override
func (dr *DoubleColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!dr.closed)
	statistics := dr.statisticsBuilder.BuildColumnStatistics()
	dr.rowGroupColumnStatistics.Add(statistics)
	dr.statisticsBuilder = dr.statisticsBuilderSupplier.Get()
	return util.NewMap(dr.columnId, statistics)
}

// @Override
func (dr *DoubleColumnWriter) Close() {
	dr.closed = true
	dr.dataStream.Close()
	dr.presentStream.Close()
}

// @Override
func (dr *DoubleColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(dr.closed)
	return util.NewMap(dr.columnId, metadata.MergeColumnStatistics(dr.rowGroupColumnStatistics))
}

// @Override
func (dr *DoubleColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(dr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := dr.dataStream.GetCheckpoints()
	presentCheckpoints := dr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < dr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := dr.rowGroupColumnStatistics.GetByInt32(groupId)
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createDoubleColumnPositionList(dr.compressed, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(dr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createDoubleColumnPositionList(compressed bool, dataCheckpoint *DoubleStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (dr *DoubleColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	// bloomFilters := dr.rowGroupColumnStatistics.Stream().Map(ColumnStatistics.getBloomFilter).filter(Objects.nonNull).collect(toImmutableList())
	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, rs := range dr.rowGroupColumnStatistics.ToArray() {
		br := rs.GetBloomFilter()
		if br != nil {
			bloomFilters.Add(rs.GetBloomFilter())
		}
	}

	if !bloomFilters.IsEmpty() {
		slice := metadataWriter.WriteBloomFilters(bloomFilters)
		stream := metadata.NewStream(dr.columnId, metadata.BLOOM_FILTER_UTF8, slice.SizeInt32(), false)
		return util.NewArrayList(NewStreamDataOutput(slice, stream))
	}
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (dr *DoubleColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(dr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	st := dr.presentStream.GetStreamDataOutput(dr.columnId)
	if st.IsPresent() {
		outputDataStreams.Add(st.Get())
	}
	outputDataStreams.Add(dr.dataStream.GetStreamDataOutput(dr.columnId))
	return outputDataStreams
}

// @Override
func (dr *DoubleColumnWriter) GetBufferedBytes() int64 {
	return dr.dataStream.GetBufferedBytes() + dr.presentStream.GetBufferedBytes()
}

// @Override
func (dr *DoubleColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(DOUBLE_INSTANCE_SIZE) + dr.dataStream.GetRetainedBytes() + dr.presentStream.GetRetainedBytes()
	for _, statistics := range dr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (dr *DoubleColumnWriter) Reset() {
	dr.closed = false
	dr.dataStream.Reset()
	dr.presentStream.Reset()
	dr.rowGroupColumnStatistics.Clear()
	dr.statisticsBuilder = dr.statisticsBuilderSupplier.Get()
}
