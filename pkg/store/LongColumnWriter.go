package store

import (
	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var LONG_INSTANCE_SIZE int32 = util.SizeOf(&LongColumnWriter{})

type LongColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                  metadata.MothColumnId
	kind                      block.Type
	compressed                bool
	columnEncoding            *metadata.ColumnEncoding
	dataStream                LongOutputStream
	presentStream             *PresentOutputStream
	rowGroupColumnStatistics  *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilderSupplier function.Supplier[metadata.LongValueStatisticsBuilder]
	statisticsBuilder         metadata.LongValueStatisticsBuilder
	closed                    bool
}

func NewLongColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[metadata.LongValueStatisticsBuilder]) *LongColumnWriter {
	lr := new(LongColumnWriter)
	lr.columnId = columnId
	lr.kind = kind
	lr.compressed = compression != metadata.NONE
	lr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	lr.dataStream = NewLongOutputStreamV2(compression, bufferSize, true, metadata.DATA)
	lr.presentStream = NewPresentOutputStream(compression, bufferSize)
	lr.statisticsBuilderSupplier = statisticsBuilderSupplier
	lr.statisticsBuilder = statisticsBuilderSupplier.Get()
	lr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return lr
}

// @Override
func (lr *LongColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (lr *LongColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(lr.columnId, lr.columnEncoding)
}

// @Override
func (lr *LongColumnWriter) BeginRowGroup() {
	lr.presentStream.RecordCheckpoint()
	lr.dataStream.RecordCheckpoint()
}

// @Override
func (lr *LongColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!lr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		lr.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			value := lr.transformValue(lr.kind.GetLong(block, position))
			lr.dataStream.WriteLong(value)
			lr.statisticsBuilder.AddValue(value)
		}
	}
}

func (lr *LongColumnWriter) transformValue(value int64) int64 {
	return value
}

// @Override
func (lr *LongColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!lr.closed)
	statistics := lr.statisticsBuilder.BuildColumnStatistics()
	lr.rowGroupColumnStatistics.Add(statistics)
	lr.statisticsBuilder = lr.statisticsBuilderSupplier.Get()
	return util.NewMap(lr.columnId, statistics)
}

// @Override
func (lr *LongColumnWriter) Close() {
	lr.closed = true
	lr.dataStream.Close()
	lr.presentStream.Close()
}

// @Override
func (lr *LongColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(lr.closed)
	return util.NewMap(lr.columnId, metadata.MergeColumnStatistics(lr.rowGroupColumnStatistics))
}

// @Override
func (lr *LongColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(lr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := lr.dataStream.GetCheckpoints()
	presentCheckpoints := lr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < lr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := lr.rowGroupColumnStatistics.GetByInt32(groupId)
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createLongColumnPositionList(lr.compressed, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(lr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createLongColumnPositionList(compressed bool, dataCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (lr *LongColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	// bloomFilters := lr.rowGroupColumnStatistics.stream().Map(ColumnStatistics.getBloomFilter).filter(Objects.nonNull).collect(toImmutableList())
	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, rs := range lr.rowGroupColumnStatistics.ToArray() {
		br := rs.GetBloomFilter()
		if br != nil {
			bloomFilters.Add(rs.GetBloomFilter())
		}
	}

	if !bloomFilters.IsEmpty() {
		slice := metadataWriter.WriteBloomFilters(bloomFilters)
		stream := metadata.NewStream(lr.columnId, metadata.BLOOM_FILTER_UTF8, slice.SizeInt32(), false)
		return util.NewArrayList(NewStreamDataOutput(slice, stream))
	}
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (lr *LongColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(lr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	lr.presentStream.GetStreamDataOutput(lr.columnId).IfPresent(
		func(s *StreamDataOutput) {
			outputDataStreams.Add(s)
		})
	outputDataStreams.Add(lr.dataStream.GetStreamDataOutput(lr.columnId))
	return outputDataStreams
}

// @Override
func (lr *LongColumnWriter) GetBufferedBytes() int64 {
	return lr.dataStream.GetBufferedBytes() + lr.presentStream.GetBufferedBytes()
}

// @Override
func (lr *LongColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(LONG_INSTANCE_SIZE) + lr.dataStream.GetRetainedBytes() + lr.presentStream.GetRetainedBytes()
	for _, statistics := range lr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (lr *LongColumnWriter) Reset() {
	lr.closed = false
	lr.dataStream.Reset()
	lr.presentStream.Reset()
	lr.rowGroupColumnStatistics.Clear()
	lr.statisticsBuilder = lr.statisticsBuilderSupplier.Get()
}

// @Override
func (lr *LongColumnWriter) String() string {
	return util.NewSB().AddString("columnId", lr.columnId.String()).AddString("kind", lr.kind.GetBaseName()).String()
}
