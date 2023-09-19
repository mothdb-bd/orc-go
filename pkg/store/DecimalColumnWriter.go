package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
	"github.com/shopspring/decimal"
)

var DECIMAL_INSTANCE_SIZE int32 = util.SizeOf(&DecimalColumnWriter{})

type DecimalColumnWriter struct {
	// 继承
	ColumnWriter

	columnId                      metadata.MothColumnId
	kind                          block.IDecimalType
	columnEncoding                *metadata.ColumnEncoding
	compressed                    bool
	dataStream                    *DecimalOutputStream
	scaleStream                   LongOutputStream
	presentStream                 *PresentOutputStream
	rowGroupColumnStatistics      *util.ArrayList[*metadata.ColumnStatistics]
	shortDecimalStatisticsBuilder *metadata.ShortDecimalStatisticsBuilder
	longDecimalStatisticsBuilder  *metadata.LongDecimalStatisticsBuilder
	closed                        bool
}

func NewDecimalColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32) *DecimalColumnWriter {
	dr := new(DecimalColumnWriter)
	dr.columnId = columnId
	dr.kind = kind.(block.IDecimalType)
	dr.compressed = compression != metadata.NONE
	dr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	dr.dataStream = NewDecimalOutputStream(compression, bufferSize)
	dr.scaleStream = NewLongOutputStreamV2(compression, bufferSize, true, metadata.SECONDARY)
	dr.presentStream = NewPresentOutputStream(compression, bufferSize)
	if dr.kind.IsShort() {
		dr.shortDecimalStatisticsBuilder = metadata.NewShortDecimalStatisticsBuilder(dr.kind.GetScale())
	} else {
		dr.longDecimalStatisticsBuilder = metadata.NewLongDecimalStatisticsBuilder()
	}
	dr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return dr
}

// @Override
func (dr *DecimalColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (dr *DecimalColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(dr.columnId, dr.columnEncoding)
}

// @Override
func (dr *DecimalColumnWriter) BeginRowGroup() {
	dr.presentStream.RecordCheckpoint()
	dr.dataStream.RecordCheckpoint()
	dr.scaleStream.RecordCheckpoint()
}

// @Override
func (dr *DecimalColumnWriter) WriteBlock(b block.Block) {
	util.CheckState(!dr.closed)
	for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
		dr.presentStream.WriteBoolean(!b.IsNull(position))
	}
	if dr.kind.IsShort() {
		for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
			if !b.IsNull(position) {
				value := dr.kind.GetLong(b, position)
				dr.dataStream.WriteUnscaledValue2(value)
				dr.shortDecimalStatisticsBuilder.AddValue(value)
			}
		}
	} else {
		for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
			if !b.IsNull(position) {
				value := dr.kind.GetObject(b, position).(*block.Int128)
				dr.dataStream.WriteUnscaledValue(value)
				d := decimal.NewFromBigInt(value.AsBigInt(), dr.kind.GetScale())
				dr.longDecimalStatisticsBuilder.AddValue(&d)
			}
		}
	}
	for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
		if !b.IsNull(position) {
			dr.scaleStream.WriteLong(int64(dr.kind.GetScale()))
		}
	}
}

// @Override
func (dr *DecimalColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!dr.closed)
	var statistics *metadata.ColumnStatistics
	if dr.kind.IsShort() {
		statistics = dr.shortDecimalStatisticsBuilder.BuildColumnStatistics()
		dr.shortDecimalStatisticsBuilder = metadata.NewShortDecimalStatisticsBuilder(dr.kind.GetScale())
	} else {
		statistics = dr.longDecimalStatisticsBuilder.BuildColumnStatistics()
		dr.longDecimalStatisticsBuilder = metadata.NewLongDecimalStatisticsBuilder()
	}
	dr.rowGroupColumnStatistics.Add(statistics)
	return util.NewMap(dr.columnId, statistics)
}

// @Override
func (dr *DecimalColumnWriter) Close() {
	dr.closed = true
	dr.dataStream.Close()
	dr.scaleStream.Close()
	dr.presentStream.Close()
}

// @Override
func (dr *DecimalColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(dr.closed)
	return util.NewMap(dr.columnId, metadata.MergeColumnStatistics(dr.rowGroupColumnStatistics))
}

// @Override
func (dr *DecimalColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(dr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := dr.dataStream.GetCheckpoints()
	scaleCheckpoints := dr.scaleStream.GetCheckpoints()
	presentCheckpoints := dr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < dr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := dr.rowGroupColumnStatistics.GetByInt32(groupId)
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		scaleCheckpoint := scaleCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createDecimalColumnPositionList(dr.compressed, dataCheckpoint, scaleCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(dr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

// @Override
func (dr *DecimalColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.NewArrayList[*StreamDataOutput]()
}

func createDecimalColumnPositionList(compressed bool, dataCheckpoint *DecimalStreamCheckpoint, scaleCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	positionList.AddAll(scaleCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (dr *DecimalColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(dr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	dr.presentStream.GetStreamDataOutput(dr.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(dr.dataStream.GetStreamDataOutput(dr.columnId))
	outputDataStreams.Add(dr.scaleStream.GetStreamDataOutput(dr.columnId))
	return outputDataStreams
}

// @Override
func (dr *DecimalColumnWriter) GetBufferedBytes() int64 {
	return dr.dataStream.GetBufferedBytes() + dr.scaleStream.GetBufferedBytes() + dr.presentStream.GetBufferedBytes()
}

// @Override
func (dr *DecimalColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(DECIMAL_INSTANCE_SIZE) + dr.dataStream.GetRetainedBytes() + dr.scaleStream.GetRetainedBytes() + dr.presentStream.GetRetainedBytes()
	for _, statistics := range dr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (dr *DecimalColumnWriter) Reset() {
	dr.closed = false
	dr.dataStream.Reset()
	dr.scaleStream.Reset()
	dr.presentStream.Reset()
	dr.rowGroupColumnStatistics.Clear()
	dr.shortDecimalStatisticsBuilder = metadata.NewShortDecimalStatisticsBuilder(dr.kind.GetScale())
	dr.longDecimalStatisticsBuilder = metadata.NewLongDecimalStatisticsBuilder()
}
