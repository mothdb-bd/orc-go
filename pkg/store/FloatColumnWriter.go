package store

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	FLOAT_INSTANCE_SIZE   int32                    = util.SizeOf(&FloatColumnWriter{})
	FLOAT_COLUMN_ENCODING *metadata.ColumnEncoding = metadata.NewColumnEncoding(metadata.DIRECT, 0)
)

type FloatColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                  metadata.MothColumnId
	kind                      block.Type
	compressed                bool
	dataStream                *FloatOutputStream
	presentStream             *PresentOutputStream
	rowGroupColumnStatistics  *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilderSupplier function.Supplier[*metadata.DoubleStatisticsBuilder]
	statisticsBuilder         *metadata.DoubleStatisticsBuilder
	closed                    bool
}

func NewFloatColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[*metadata.DoubleStatisticsBuilder]) *FloatColumnWriter {
	fr := new(FloatColumnWriter)
	fr.columnId = columnId
	fr.kind = kind
	fr.compressed = compression != metadata.NONE
	fr.dataStream = NewFloatOutputStream(compression, bufferSize)
	fr.presentStream = NewPresentOutputStream(compression, bufferSize)
	fr.statisticsBuilderSupplier = statisticsBuilderSupplier
	fr.statisticsBuilder = statisticsBuilderSupplier.Get()
	fr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return fr
}

// @Override
func (fr *FloatColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (fr *FloatColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(fr.columnId, FLOAT_COLUMN_ENCODING)
}

// @Override
func (fr *FloatColumnWriter) BeginRowGroup() {
	fr.presentStream.RecordCheckpoint()
	fr.dataStream.RecordCheckpoint()
}

// @Override
func (fr *FloatColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!fr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		fr.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			intBits := int32(fr.kind.GetLong(block, position))
			value := math.Float32frombits(uint32(intBits))
			fr.dataStream.WriteFloat(value)
			fr.statisticsBuilder.AddValue(float64(value))
		}
	}
}

// @Override
func (fr *FloatColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!fr.closed)
	statistics := fr.statisticsBuilder.BuildColumnStatistics()
	fr.rowGroupColumnStatistics.Add(statistics)
	fr.statisticsBuilder = fr.statisticsBuilderSupplier.Get()
	return util.NewMap(fr.columnId, statistics)
}

// @Override
func (fr *FloatColumnWriter) Close() {
	fr.closed = true
	fr.dataStream.Close()
	fr.presentStream.Close()
}

// @Override
func (fr *FloatColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(fr.closed)
	return util.NewMap(fr.columnId, metadata.MergeColumnStatistics(fr.rowGroupColumnStatistics))
}

// @Override
func (fr *FloatColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(fr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := fr.dataStream.GetCheckpoints()
	presentCheckpoints := fr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < fr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := fr.rowGroupColumnStatistics.GetByInt32(groupId)
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createFloatColumnPositionList(fr.compressed, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(fr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createFloatColumnPositionList(compressed bool, dataCheckpoint *FloatStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (fr *FloatColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	// bloomFilters := rowGroupColumnStatistics.stream().Map(ColumnStatistics.getBloomFilter).filter(Objects.nonNull).collect(toImmutableList())
	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, rs := range fr.rowGroupColumnStatistics.ToArray() {
		br := rs.GetBloomFilter()
		if br != nil {
			bloomFilters.Add(rs.GetBloomFilter())
		}
	}

	if !bloomFilters.IsEmpty() {
		slice := metadataWriter.WriteBloomFilters(bloomFilters)
		stream := metadata.NewStream(fr.columnId, metadata.BLOOM_FILTER_UTF8, slice.SizeInt32(), false)
		return util.NewArrayList(NewStreamDataOutput(slice, stream))
	}
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (fr *FloatColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(fr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	fr.presentStream.GetStreamDataOutput(fr.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(fr.dataStream.GetStreamDataOutput(fr.columnId))
	return outputDataStreams
}

// @Override
func (fr *FloatColumnWriter) GetBufferedBytes() int64 {
	return fr.dataStream.GetBufferedBytes() + fr.presentStream.GetBufferedBytes()
}

// @Override
func (fr *FloatColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(FLOAT_INSTANCE_SIZE) + fr.dataStream.GetRetainedBytes() + fr.presentStream.GetRetainedBytes()
	for _, statistics := range fr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (fr *FloatColumnWriter) Reset() {
	fr.closed = false
	fr.dataStream.Reset()
	fr.presentStream.Reset()
	fr.rowGroupColumnStatistics.Clear()
	fr.statisticsBuilder = fr.statisticsBuilderSupplier.Get()
}
