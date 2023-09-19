package store

import (
	"fmt"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	TIMESTAMP_INSTANCE_SIZE         int32 = util.SizeOf(&TimestampColumnWriter{})
	TIMESTAMP_MOTH_EPOCH_IN_SECONDS int64 = int64(time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Second())
)

// type TimestampKind int8

// const (
// 	TIMESTAMP_MILLIS TimestampKind = iota
// 	TIMESTAMP_MICROS
// 	TIMESTAMP_NANOS
// 	INSTANT_MILLIS
// 	INSTANT_MICROS
// 	INSTANT_NANOS
// )

// The MOTH encoding erroneously uses normal integer division to compute seconds,
// rather than floor modulus, which produces the wrong result for negative values
// (those that are before the epoch). Readers must correct for this. It also makes
// it impossible to represent values less than one second before the epoch, which
// must also be handled in MothWriteValidation.
//
// The sub-second value (nanoseconds) typically has a large number of trailing zeroes,
// as many systems only record millisecond or microsecond precision. To optimize storage,
// if the value has at least two trailing zeros, the trailing decimal zero digits are
// removed, and the last three bits record how many zeros were removed, minus one:
//
//	# Trailing 0s   Last 3 Bits   Example nanos       Example encoding
//	      0            0b000        123456789     (123456789 << 3) | 0b000
//	      1            0b000        123456780     (123456780 << 3) | 0b000
//	      2            0b001        123456700       (1234567 << 3) | 0b001
//	      3            0b010        123456000        (123456 << 3) | 0b010
//	      4            0b011        123450000         (12345 << 3) | 0b011
//	      5            0b100        123400000          (1234 << 3) | 0b100
//	      6            0b101        123000000           (123 << 3) | 0b101
//	      7            0b110        120000000            (12 << 3) | 0b110
//	      8            0b111        100000000             (1 << 3) | 0b111
type TimestampColumnWriter struct {
	//继承
	ColumnWriter

	columnId                  metadata.MothColumnId
	kind                      block.Type
	timestampKind             TimestampKind
	compressed                bool
	columnEncoding            *metadata.ColumnEncoding
	secondsStream             LongOutputStream
	nanosStream               LongOutputStream
	presentStream             *PresentOutputStream
	rowGroupColumnStatistics  *util.ArrayList[*metadata.ColumnStatistics]
	statisticsBuilderSupplier function.Supplier[*metadata.TimestampStatisticsBuilder]
	statisticsBuilder         metadata.LongValueStatisticsBuilder
	closed                    bool
}

func NewTimestampColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[*metadata.TimestampStatisticsBuilder]) *TimestampColumnWriter {
	tr := new(TimestampColumnWriter)
	tr.columnId = columnId
	tr.kind = kind
	tr.timestampKind = timestampKindForType(kind)
	tr.compressed = compression != metadata.NONE
	tr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	tr.secondsStream = NewLongOutputStreamV2(compression, bufferSize, true, metadata.DATA)
	tr.nanosStream = NewLongOutputStreamV2(compression, bufferSize, false, metadata.SECONDARY)
	tr.presentStream = NewPresentOutputStream(compression, bufferSize)
	tr.statisticsBuilderSupplier = statisticsBuilderSupplier
	tr.statisticsBuilder = statisticsBuilderSupplier.Get()

	tr.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return tr
}

func timestampKindForType(kind block.Type) TimestampKind {
	if kind.Equals(block.TIMESTAMP_MILLIS) {
		return TIMESTAMP_MILLIS
	}
	if kind.Equals(block.TIMESTAMP_MICROS) {
		return TIMESTAMP_MICROS
	}
	if kind.Equals(block.TIMESTAMP_NANOS) {
		return TIMESTAMP_NANOS
	}
	if kind.Equals(block.TIMESTAMP_TZ_MILLIS) {
		return INSTANT_MILLIS
	}
	if kind.Equals(block.TIMESTAMP_TZ_MICROS) {
		return INSTANT_MICROS
	}
	if kind.Equals(block.TIMESTAMP_TZ_NANOS) {
		return INSTANT_NANOS
	}
	panic(fmt.Sprintf("Unsupported kind for MOTH timestamp writer: %s", kind))
}

// @Override
func (tr *TimestampColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (tr *TimestampColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(tr.columnId, tr.columnEncoding)
}

// @Override
func (tr *TimestampColumnWriter) BeginRowGroup() {
	tr.presentStream.RecordCheckpoint()
	tr.secondsStream.RecordCheckpoint()
	tr.nanosStream.RecordCheckpoint()
}

// @Override
func (tr *TimestampColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!tr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		tr.presentStream.WriteBoolean(!block.IsNull(position))
	}
	switch tr.timestampKind {
	case TIMESTAMP_MILLIS, TIMESTAMP_MICROS:
		tr.writeTimestampMicros(block)
	case TIMESTAMP_NANOS:
		tr.writeTimestampNanos(block)
	case INSTANT_MILLIS:
		tr.writeInstantShort(block)
	case INSTANT_MICROS, INSTANT_NANOS:
		tr.writeInstantLong(block)
	}
}

// @Override
func (tr *TimestampColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!tr.closed)
	statistics := tr.statisticsBuilder.BuildColumnStatistics()
	tr.rowGroupColumnStatistics.Add(statistics)
	tr.statisticsBuilder = tr.statisticsBuilderSupplier.Get()
	return util.NewMap(tr.columnId, statistics)
}

// @Override
func (tr *TimestampColumnWriter) Close() {
	tr.closed = true
	tr.secondsStream.Close()
	tr.nanosStream.Close()
	tr.presentStream.Close()
}

// @Override
func (tr *TimestampColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(tr.closed)
	return util.NewMap(tr.columnId, metadata.MergeColumnStatistics(tr.rowGroupColumnStatistics))
}

// @Override
func (tr *TimestampColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(tr.closed)
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	secondsCheckpoints := tr.secondsStream.GetCheckpoints()
	nanosCheckpoints := tr.nanosStream.GetCheckpoints()
	presentCheckpoints := tr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < tr.rowGroupColumnStatistics.SizeInt32(); i++ {
		groupId := i
		columnStatistics := tr.rowGroupColumnStatistics.GetByInt32(groupId)
		secondsCheckpoint := secondsCheckpoints.GetByInt32(groupId)
		nanosCheckpoint := nanosCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createTimestampColumnPositionList(tr.compressed, secondsCheckpoint, nanosCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(tr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createTimestampColumnPositionList(compressed bool, secondsCheckpoint LongStreamCheckpoint, nanosCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(secondsCheckpoint.ToPositionList(compressed))
	positionList.AddAll(nanosCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (tr *TimestampColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	return util.EMPTY_LIST[*StreamDataOutput]()
}

// @Override
func (tr *TimestampColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(tr.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	tr.presentStream.GetStreamDataOutput(tr.columnId).IfPresent(
		func(s *StreamDataOutput) {
			outputDataStreams.Add(s)
		})
	outputDataStreams.Add(tr.secondsStream.GetStreamDataOutput(tr.columnId))
	outputDataStreams.Add(tr.nanosStream.GetStreamDataOutput(tr.columnId))
	return outputDataStreams
}

// @Override
func (tr *TimestampColumnWriter) GetBufferedBytes() int64 {
	return tr.secondsStream.GetBufferedBytes() + tr.nanosStream.GetBufferedBytes() + tr.presentStream.GetBufferedBytes()
}

// @Override
func (tr *TimestampColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(TIMESTAMP_INSTANCE_SIZE) + tr.secondsStream.GetRetainedBytes() + tr.nanosStream.GetRetainedBytes() + tr.presentStream.GetRetainedBytes()
	for _, statistics := range tr.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (tr *TimestampColumnWriter) Reset() {
	tr.closed = false
	tr.secondsStream.Reset()
	tr.nanosStream.Reset()
	tr.presentStream.Reset()
	tr.rowGroupColumnStatistics.Clear()
	tr.statisticsBuilder = tr.statisticsBuilderSupplier.Get()
}

func (tr *TimestampColumnWriter) writeTimestampMicros(b block.Block) {
	for i := util.INT32_ZERO; i < b.GetPositionCount(); i++ {
		if !b.IsNull(i) {
			micros := tr.kind.GetLong(b, i)
			seconds := micros / int64(block.TTS_MICROSECONDS_PER_SECOND)
			microsFraction := maths.FloorMod(micros, int64(block.TTS_MICROSECONDS_PER_SECOND))
			nanosFraction := microsFraction * int64(block.TTS_NANOSECONDS_PER_MICROSECOND)
			millis := maths.FloorDiv(micros, int64(block.TTS_MICROSECONDS_PER_MILLISECOND))
			tr.writeValues(seconds, nanosFraction)
			tr.statisticsBuilder.AddValue(millis)
		}
	}
}

func (tr *TimestampColumnWriter) writeTimestampNanos(b block.Block) {
	for i := util.INT32_ZERO; i < b.GetPositionCount(); i++ {
		if !b.IsNull(i) {
			timestamp := tr.kind.GetObject(b, i).(*block.LongTimestamp)
			seconds := timestamp.GetEpochMicros() / int64(block.TTS_MICROSECONDS_PER_SECOND)
			microsFraction := maths.FloorMod(timestamp.GetEpochMicros(), int64(block.TTS_MICROSECONDS_PER_SECOND))
			nanosFraction := (microsFraction * int64(block.TTS_NANOSECONDS_PER_MICROSECOND)) + int64(timestamp.GetPicosOfMicro()/block.TTS_PICOSECONDS_PER_NANOSECOND) // no rounding since the data has nanosecond precision, at most
			millis := maths.FloorDiv(timestamp.GetEpochMicros(), int64(block.TTS_MICROSECONDS_PER_MILLISECOND))
			tr.writeValues(seconds, nanosFraction)
			tr.statisticsBuilder.AddValue(millis)
		}
	}
}

func (tr *TimestampColumnWriter) writeInstantShort(b block.Block) {
	for i := util.INT32_ZERO; i < b.GetPositionCount(); i++ {
		if !b.IsNull(i) {
			tr.writeMillis(block.UnpackMillisUtc(tr.kind.GetLong(b, i)))
		}
	}
}

func (tr *TimestampColumnWriter) writeInstantLong(b block.Block) {
	for i := util.INT32_ZERO; i < b.GetPositionCount(); i++ {
		if !b.IsNull(i) {
			timestamp := tr.kind.GetObject(b, i).(*block.LongTimestampWithTimeZone)
			millis := timestamp.GetEpochMillis()
			seconds := millis / int64(block.TTS_MILLISECONDS_PER_SECOND)
			millisFraction := maths.FloorMod(millis, int64(block.TTS_MILLISECONDS_PER_SECOND))
			nanosFraction := (millisFraction * int64(block.TTS_NANOSECONDS_PER_MILLISECOND)) + int64(timestamp.GetPicosOfMilli()/block.TTS_PICOSECONDS_PER_NANOSECOND)
			tr.writeValues(seconds, nanosFraction)
			tr.statisticsBuilder.AddValue(millis)
		}
	}
}

func (tr *TimestampColumnWriter) writeMillis(millis int64) {
	seconds := millis / int64(block.TTS_MILLISECONDS_PER_SECOND)
	millisFraction := maths.FloorMod(millis, int64(block.TTS_MILLISECONDS_PER_SECOND))
	nanosFraction := millisFraction * int64(block.TTS_NANOSECONDS_PER_MILLISECOND)
	tr.writeValues(seconds, nanosFraction)
	tr.statisticsBuilder.AddValue(millis)
}

func (tr *TimestampColumnWriter) writeValues(seconds int64, nanosFraction int64) {
	tr.secondsStream.WriteLong(seconds - TIMESTAMP_MOTH_EPOCH_IN_SECONDS)
	tr.nanosStream.WriteLong(encodeNanos(nanosFraction))
}

func encodeNanos(nanos int64) int64 {
	if nanos == 0 {
		return 0
	}
	if (nanos % 100) != 0 {
		return nanos << 3
	}
	nanos /= 100
	trailingZeros := 1
	for ((nanos % 10) == 0) && (trailingZeros < 7) {
		nanos /= 10
		trailingZeros++
	}
	return (nanos << 3) | int64(trailingZeros)
}
