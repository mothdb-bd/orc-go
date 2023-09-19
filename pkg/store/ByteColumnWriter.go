package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	W_BYTE_INSTANCE_SIZE int32                    = util.SizeOf(&ByteColumnWriter{})
	BYTE_COLUMN_ENCODING *metadata.ColumnEncoding = metadata.NewColumnEncoding(metadata.DIRECT, 0)
)

type ByteColumnWriter struct {

	//继承
	ColumnWriter

	columnId                 metadata.MothColumnId
	kind                     block.Type
	compressed               bool
	dataStream               *ByteOutputStream
	presentStream            *PresentOutputStream
	rowGroupColumnStatistics *util.ArrayList[*metadata.ColumnStatistics]
	nonNullValueCount        int32
	closed                   bool
}

func NewByteColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32) *ByteColumnWriter {
	br := new(ByteColumnWriter)
	br.columnId = columnId
	br.kind = kind
	br.compressed = compression != metadata.NONE
	br.dataStream = NewByteOutputStream(compression, bufferSize)
	br.presentStream = NewPresentOutputStream(compression, bufferSize)
	br.rowGroupColumnStatistics = util.NewArrayList[*metadata.ColumnStatistics]()
	return br
}

// @Override
func (br *ByteColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (br *ByteColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	return util.NewMap(br.columnId, BYTE_COLUMN_ENCODING)
}

// @Override
func (br *ByteColumnWriter) BeginRowGroup() {
	br.presentStream.RecordCheckpoint()
	br.dataStream.RecordCheckpoint()
}

// @Override
func (br *ByteColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!br.closed)
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		br.presentStream.WriteBoolean(!block.IsNull(position))
	}
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			br.dataStream.WriteByte(byte(br.kind.GetLong(block, position)))
			br.nonNullValueCount++
		}
	}
}

// @Override
func (br *ByteColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!br.closed)
	statistics := metadata.NewColumnStatistics(int64(br.nonNullValueCount), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	br.rowGroupColumnStatistics.Add(statistics)
	br.nonNullValueCount = 0
	return util.NewMap(br.columnId, statistics)
}

// @Override
func (br *ByteColumnWriter) Close() {
	br.closed = true
	br.dataStream.Close()
	br.presentStream.Close()
}

// @Override
func (br *ByteColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(br.closed)
	return util.NewMap(br.columnId, metadata.MergeColumnStatistics(br.rowGroupColumnStatistics))
}

// @Override
func (br *ByteColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
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
		positions := createByteColumnPositionList(br.compressed, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(br.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createByteColumnPositionList(compressed bool, dataCheckpoint *ByteStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (br *ByteColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, cs := range br.rowGroupColumnStatistics.ToArray() {
		bf := cs.GetBloomFilter()
		if bf != nil {
			bloomFilters.Add(bf)
		}
	}

	if !bloomFilters.IsEmpty() {
		slice := metadataWriter.WriteBloomFilters(bloomFilters)
		stream := metadata.NewStream(br.columnId, metadata.BLOOM_FILTER_UTF8, slice.SizeInt32(), false)
		return util.NewArrayList(NewStreamDataOutput(slice, stream))
	}
	return util.NewArrayList[*StreamDataOutput]()
}

// @Override
func (br *ByteColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(br.closed)
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	br.presentStream.GetStreamDataOutput(br.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(br.dataStream.GetStreamDataOutput(br.columnId))
	return outputDataStreams
}

// @Override
func (br *ByteColumnWriter) GetBufferedBytes() int64 {
	return br.dataStream.GetBufferedBytes() + br.presentStream.GetBufferedBytes()
}

// @Override
func (br *ByteColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(W_BYTE_INSTANCE_SIZE) + br.dataStream.GetRetainedBytes() + br.presentStream.GetRetainedBytes()
	for _, statistics := range br.rowGroupColumnStatistics.ToArray() {
		retainedBytes += statistics.GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (br *ByteColumnWriter) Reset() {
	br.closed = false
	br.dataStream.Reset()
	br.presentStream.Reset()
	br.rowGroupColumnStatistics.Clear()
	br.nonNullValueCount = 0
}
