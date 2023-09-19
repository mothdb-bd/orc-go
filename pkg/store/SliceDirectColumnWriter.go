package store

import (
	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var SLICE_DIRECT_INSTANCE_SIZE int32 = util.SizeOf(&SliceDirectColumnWriter{})

type SliceDirectColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                  metadata.MothColumnId
	kind                      block.Type
	compressed                bool
	columnEncoding            *metadata.ColumnEncoding
	lengthStream              LongOutputStream
	dataStream                *ByteArrayOutputStream
	presentStream             *PresentOutputStream
	rowGroupColumnStatistics  *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilderSupplier function.Supplier[metadata.SliceColumnStatisticsBuilder]
	statisticsBuilder         metadata.SliceColumnStatisticsBuilder
	closed                    bool
}

func NewSliceDirectColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[metadata.SliceColumnStatisticsBuilder]) *SliceDirectColumnWriter {
	sr := new(SliceDirectColumnWriter)
	sr.columnId = columnId
	sr.kind = kind
	sr.compressed = compression != metadata.NONE
	sr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	sr.lengthStream = CreateLengthOutputStream(compression, bufferSize)
	sr.dataStream = NewByteArrayOutputStream(compression, bufferSize)
	sr.presentStream = NewPresentOutputStream(compression, bufferSize)
	sr.statisticsBuilderSupplier = statisticsBuilderSupplier
	sr.statisticsBuilder = statisticsBuilderSupplier.Get()
	sr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return sr
}

func (sr *SliceDirectColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (sr *SliceDirectColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(sr.columnId, sr.columnEncoding)
}

// @Override
func (sr *SliceDirectColumnWriter) BeginRowGroup() {
	util.CheckState(!sr.closed)
	sr.presentStream.RecordCheckpoint()
	sr.lengthStream.RecordCheckpoint()
	sr.dataStream.RecordCheckpoint()
}

// @Override
func (sr *SliceDirectColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!sr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		sr.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			value := sr.kind.GetSlice(block, position)
			sr.lengthStream.WriteLong(int64(value.Size()))
			sr.dataStream.WriteSlice(value)
			sr.statisticsBuilder.AddValue(value)
		}
	}
}

// @Override
func (sr *SliceDirectColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!sr.closed)
	statistics := sr.statisticsBuilder.BuildColumnStatistics()
	sr.rowGroupColumnStatistics.Add(statistics)
	sr.statisticsBuilder = sr.statisticsBuilderSupplier.Get()
	return util.NewMap(sr.columnId, statistics)
}

// @Override
func (sr *SliceDirectColumnWriter) Close() {
	util.CheckState(!sr.closed)
	sr.closed = true
	sr.lengthStream.Close()
	sr.dataStream.Close()
	sr.presentStream.Close()
}

// @Override
func (sr *SliceDirectColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(sr.closed)
	return util.NewMap(sr.columnId, metadata.MergeColumnStatistics(sr.rowGroupColumnStatistics))
}

// @Override
func (sr *SliceDirectColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	lengthCheckpoints := sr.lengthStream.GetCheckpoints()
	dataCheckpoints := sr.dataStream.GetCheckpoints()
	presentCheckpoints := sr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < sr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := sr.rowGroupColumnStatistics.GetByInt32(groupId)
		lengthCheckpoint := lengthCheckpoints.GetByInt32(groupId)
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := slice_createSliceColumnPositionList(sr.compressed, lengthCheckpoint, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(sr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func slice_createSliceColumnPositionList(compressed bool, lengthCheckpoint LongStreamCheckpoint, dataCheckpoint *ByteArrayStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	positionList.AddAll(lengthCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (sr *SliceDirectColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	// bloomFilters := rowGroupColumnStatistics.stream().map(ColumnStatistics.getBloomFilter).filter(Objects.nonNull).collect(toImmutableList())
	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, rs := range sr.rowGroupColumnStatistics.ToArray() {
		br := rs.GetBloomFilter()
		if br != nil {
			bloomFilters.Add(rs.GetBloomFilter())
		}
	}
	if !bloomFilters.IsEmpty() {
		slice := metadataWriter.WriteBloomFilters(bloomFilters)
		stream := metadata.NewStream(sr.columnId, metadata.BLOOM_FILTER_UTF8, slice.SizeInt32(), false)
		return util.NewArrayList(NewStreamDataOutput(slice, stream))
	}
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (sr *SliceDirectColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	sr.presentStream.GetStreamDataOutput(sr.columnId).IfPresent(
		func(s *StreamDataOutput) {
			outputDataStreams.Add(s)
		})
	outputDataStreams.Add(sr.lengthStream.GetStreamDataOutput(sr.columnId))
	outputDataStreams.Add(sr.dataStream.GetStreamDataOutput(sr.columnId))
	return outputDataStreams
}

// @Override
func (sr *SliceDirectColumnWriter) GetBufferedBytes() int64 {
	return sr.lengthStream.GetBufferedBytes() + sr.dataStream.GetBufferedBytes() + sr.presentStream.GetBufferedBytes()
}

// @Override
func (sr *SliceDirectColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(SLICE_DIRECT_INSTANCE_SIZE) + sr.lengthStream.GetRetainedBytes() + sr.dataStream.GetRetainedBytes() + sr.presentStream.GetRetainedBytes()
	for _, statistics := range sr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (sr *SliceDirectColumnWriter) Reset() {
	util.CheckState(sr.closed)
	sr.closed = false
	sr.lengthStream.Reset()
	sr.dataStream.Reset()
	sr.presentStream.Reset()
	sr.rowGroupColumnStatistics.Clear()
	sr.statisticsBuilder = sr.statisticsBuilderSupplier.Get()
}
