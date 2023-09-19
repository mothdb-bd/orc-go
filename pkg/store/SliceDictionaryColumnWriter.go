package store

import (
	"sort"

	"github.com/mothdb-bd/orc-go/pkg/array"
	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	SLICEDICTIONARY_INSTANCE_SIZE                             int32 = util.SizeOf(&SliceDictionaryColumnWriter{})
	SLICEDICTIONARY_DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES int32 = util.Int32Exact(int64(32 * util.MB.Bytes()))
)

type SliceDictionaryColumnWriter struct {
	// 继承
	ColumnWriter

	// 继承
	DictionaryColumn

	columnId                  metadata.MothColumnId
	kind                      block.Type
	compression               metadata.CompressionKind
	bufferSize                int32
	dataStream                LongOutputStream
	presentStream             *PresentOutputStream
	dictionaryDataStream      *ByteArrayOutputStream
	dictionaryLengthStream    LongOutputStream
	dictionary                *DictionaryBuilder
	rowGroups                 *util.ArrayList[*DictionaryRowGroup]
	statisticsBuilderSupplier function.Supplier[metadata.SliceColumnStatisticsBuilder]
	values                    *array.IntBigArray
	rowGroupValueCount        int32
	statisticsBuilder         metadata.SliceColumnStatisticsBuilder
	rawBytes                  int64
	totalValueCount           int64
	totalNonNullValueCount    int64
	closed                    bool
	inRowGroup                bool
	columnEncoding            *metadata.ColumnEncoding
	directEncoded             bool
	directColumnWriter        *SliceDirectColumnWriter
}

func NewSliceDictionaryColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[metadata.SliceColumnStatisticsBuilder]) *SliceDictionaryColumnWriter {
	sr := new(SliceDictionaryColumnWriter)

	sr.dictionary = NewDictionaryBuilder(100)
	sr.rowGroups = util.NewArrayList[*DictionaryRowGroup]()

	sr.columnId = columnId
	sr.kind = kind
	sr.compression = compression
	sr.bufferSize = bufferSize
	sr.dataStream = NewLongOutputStreamV2(compression, bufferSize, false, metadata.DATA)
	sr.presentStream = NewPresentOutputStream(compression, bufferSize)
	sr.dictionaryDataStream = NewByteArrayOutputStream2(compression, bufferSize, metadata.DICTIONARY_DATA)
	sr.dictionaryLengthStream = CreateLengthOutputStream(compression, bufferSize)
	sr.values = array.NewIntBigArray()
	sr.statisticsBuilderSupplier = statisticsBuilderSupplier
	sr.statisticsBuilder = statisticsBuilderSupplier.Get()

	sr.rowGroups = util.NewArrayList[*DictionaryRowGroup]()
	return sr
}

func (mr *SliceDictionaryColumnWriter) GetNestedColumnWriters() *util.ArrayList[ColumnWriter] {
	return util.NewArrayList[ColumnWriter]()
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetRawBytes() int64 {
	util.CheckState(!sr.directEncoded)
	return sr.rawBytes
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetDictionaryBytes() int32 {
	util.CheckState(!sr.directEncoded)
	return util.Int32Exact(sr.dictionary.GetSizeInBytes())
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetIndexBytes() int32 {
	util.CheckState(!sr.directEncoded)
	return util.Int32Exact(int64(EstimateIndexBytesPerValue(sr.dictionary.GetEntryCount())) * sr.GetNonNullValueCount())
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetValueCount() int64 {
	util.CheckState(!sr.directEncoded)
	return sr.totalValueCount
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetNonNullValueCount() int64 {
	util.CheckState(!sr.directEncoded)
	return sr.totalNonNullValueCount
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetDictionaryEntries() int32 {
	util.CheckState(!sr.directEncoded)
	return sr.dictionary.GetEntryCount()
}

// @Override
func (sr *SliceDictionaryColumnWriter) TryConvertToDirect(maxDirectBytes int32) *optional.OptionalInt {
	util.CheckState(!sr.closed)
	util.CheckState(!sr.directEncoded)
	if sr.directColumnWriter == nil {
		sr.directColumnWriter = NewSliceDirectColumnWriter(sr.columnId, sr.kind, sr.compression, sr.bufferSize, sr.statisticsBuilderSupplier)
	}
	util.CheckState(sr.directColumnWriter.GetBufferedBytes() == 0)
	dictionaryValues := sr.dictionary.GetElementBlock()
	for _, rowGroup := range sr.rowGroups.ToArray() {
		sr.directColumnWriter.BeginRowGroup()
		success := sr.writeDictionaryRowGroup(dictionaryValues, rowGroup.GetValueCount(), rowGroup.GetDictionaryIndexes(), maxDirectBytes)
		sr.directColumnWriter.FinishRowGroup()
		if !success {
			sr.directColumnWriter.Close()
			sr.directColumnWriter.Reset()
			return optional.OptionalIntEmpty()
		}
	}
	if sr.inRowGroup {
		sr.directColumnWriter.BeginRowGroup()
		if !sr.writeDictionaryRowGroup(dictionaryValues, sr.rowGroupValueCount, sr.values, maxDirectBytes) {
			sr.directColumnWriter.Close()
			sr.directColumnWriter.Reset()
			return optional.OptionalIntEmpty()
		}
	} else {
		util.CheckState(sr.rowGroupValueCount == 0)
	}
	sr.rowGroups.Clear()
	sr.dictionary.Clear()
	sr.rawBytes = 0
	sr.totalValueCount = 0
	sr.totalNonNullValueCount = 0
	sr.rowGroupValueCount = 0
	sr.statisticsBuilder = sr.statisticsBuilderSupplier.Get()
	sr.directEncoded = true
	return optional.OptionalIntof(util.Int32Exact(sr.directColumnWriter.GetBufferedBytes()))
}

func (sr *SliceDictionaryColumnWriter) writeDictionaryRowGroup(dictionary block.Block, valueCount int32, dictionaryIndexes *array.IntBigArray, maxDirectBytes int32) bool {
	segments := dictionaryIndexes.GetSegments()
	for i := util.INT32_ZERO; valueCount > 0 && i < util.Lens(segments); i++ {
		segment := segments[i]
		positionCount := maths.MinInt32(valueCount, util.Lens(segment))
		var b block.Block = block.NewDictionaryBlock2(positionCount, dictionary, segment)
		for b != nil {
			chunkPositionCount := b.GetPositionCount()
			chunk := b.GetRegion(0, chunkPositionCount)
			for chunkPositionCount > 1 && chunk.GetLogicalSizeInBytes() > int64(SLICEDICTIONARY_DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
				chunkPositionCount /= 2
				chunk = chunk.GetRegion(0, chunkPositionCount)
			}
			sr.directColumnWriter.WriteBlock(chunk)
			if sr.directColumnWriter.GetBufferedBytes() > int64(maxDirectBytes) {
				return false
			}
			if chunkPositionCount < b.GetPositionCount() {
				b = b.GetRegion(chunkPositionCount, b.GetPositionCount()-chunkPositionCount)
			} else {
				b = nil
			}
		}
		valueCount -= positionCount
	}
	util.CheckState(valueCount == 0)
	return true
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding {
	util.CheckState(sr.closed)
	if sr.directEncoded {
		return sr.directColumnWriter.GetColumnEncodings()
	}
	return util.NewMap(sr.columnId, sr.columnEncoding)
}

// @Override
func (sr *SliceDictionaryColumnWriter) BeginRowGroup() {
	util.CheckState(!sr.inRowGroup)
	sr.inRowGroup = true
	if sr.directEncoded {
		sr.directColumnWriter.BeginRowGroup()
	}
}

// @Override
func (sr *SliceDictionaryColumnWriter) WriteBlock(block block.Block) {
	util.CheckState(!sr.closed)
	util.CheckArgument2(block.GetPositionCount() > 0, "Block is empty")
	if sr.directEncoded {
		sr.directColumnWriter.WriteBlock(block)
		return
	}
	sr.values.EnsureCapacity(int64(sr.rowGroupValueCount + block.GetPositionCount()))
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		index := sr.dictionary.PutIfAbsent(block, position)
		sr.values.Set(int64(sr.rowGroupValueCount), index)
		sr.rowGroupValueCount++
		sr.totalValueCount++
		if !block.IsNull(position) {
			sr.statisticsBuilder.AddValue(sr.kind.GetSlice(block, position))
			sr.rawBytes += int64(block.GetSliceLength(position))
			sr.totalNonNullValueCount++
		}
	}
}

// @Override
func (sr *SliceDictionaryColumnWriter) FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(!sr.closed)
	util.CheckState(sr.inRowGroup)
	sr.inRowGroup = false
	if sr.directEncoded {
		return sr.directColumnWriter.FinishRowGroup()
	}
	statistics := sr.statisticsBuilder.BuildColumnStatistics()
	sr.rowGroups.Add(NewDictionaryRowGroup(sr.values, sr.rowGroupValueCount, statistics))
	sr.rowGroupValueCount = 0
	sr.statisticsBuilder = sr.statisticsBuilderSupplier.Get()
	sr.values = array.NewIntBigArray()
	return util.NewMap(sr.columnId, statistics)
}

// @Override
func (sr *SliceDictionaryColumnWriter) Close() {
	util.CheckState(!sr.closed)
	util.CheckState(!sr.inRowGroup)
	sr.closed = true
	if sr.directEncoded {
		sr.directColumnWriter.Close()
	} else {
		sr.bufferOutputData()
	}
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics {
	util.CheckState(sr.closed)
	if sr.directEncoded {
		return sr.directColumnWriter.GetColumnStripeStatistics()
	}

	tmpList := util.NewArrayList[*metadata.ColumnStatistics]()
	for _, rs := range sr.rowGroups.ToArray() {
		tmpList.Add(rs.GetColumnStatistics())
	}

	return util.NewMap(sr.columnId, metadata.MergeColumnStatistics(tmpList))
}

func (sr *SliceDictionaryColumnWriter) bufferOutputData() {
	util.CheckState(sr.closed)
	util.CheckState(!sr.directEncoded)
	dictionaryElements := sr.dictionary.GetElementBlock()
	sortedDictionaryIndexes := getSortedDictionaryNullsLast(dictionaryElements)
	for _, sortedDictionaryIndex := range sortedDictionaryIndexes {
		if !dictionaryElements.IsNull(sortedDictionaryIndex) {
			length := dictionaryElements.GetSliceLength(sortedDictionaryIndex)
			sr.dictionaryLengthStream.WriteLong(int64(length))
			value := dictionaryElements.GetSlice(sortedDictionaryIndex, 0, length)
			sr.dictionaryDataStream.WriteSlice(value)
		}
	}
	sr.columnEncoding = metadata.NewColumnEncoding(metadata.DICTIONARY_V2, uint32(dictionaryElements.GetPositionCount())-1)
	originalDictionaryToSortedIndex := make([]int32, util.Lens(sortedDictionaryIndexes))
	for sortOrdinal := util.INT32_ZERO; sortOrdinal < util.Lens(sortedDictionaryIndexes); sortOrdinal++ {
		dictionaryIndex := sortedDictionaryIndexes[sortOrdinal]
		originalDictionaryToSortedIndex[dictionaryIndex] = sortOrdinal
	}
	if !sr.rowGroups.IsEmpty() {
		sr.presentStream.RecordCheckpoint()
		sr.dataStream.RecordCheckpoint()
	}
	for _, rowGroup := range sr.rowGroups.ToArray() {
		dictionaryIndexes := rowGroup.GetDictionaryIndexes()
		for position := util.INT32_ZERO; position < rowGroup.GetValueCount(); position++ {
			sr.presentStream.WriteBoolean(dictionaryIndexes.Get(int64(position)) != 0)
		}
		for position := util.INT32_ZERO; position < rowGroup.GetValueCount(); position++ {
			originalDictionaryIndex := dictionaryIndexes.Get(int64(position))
			if originalDictionaryIndex != 0 {
				sortedIndex := originalDictionaryToSortedIndex[originalDictionaryIndex]
				if sortedIndex < 0 {
					panic("illegal argument")
				}
				sr.dataStream.WriteLong(int64(sortedIndex))
			}
		}
		sr.presentStream.RecordCheckpoint()
		sr.dataStream.RecordCheckpoint()
	}
	sr.dictionary.Clear()
	sr.dictionaryDataStream.Close()
	sr.dictionaryLengthStream.Close()
	sr.dataStream.Close()
	sr.presentStream.Close()
}

func getSortedDictionaryNullsLast(elementBlock block.Block) []int32 {
	sortedPositions := make([]int32, elementBlock.GetPositionCount())
	for i := util.INT32_ZERO; i < util.Lens(sortedPositions); i++ {
		sortedPositions[i] = i
	}

	sort.Sort(NewInts(sortedPositions, elementBlock))

	// array.IntArrays.quickSort(sortedPositions, 0, sortedPositions.length, func(left int32, right int32) {
	// 	nullLeft := elementBlock.IsNull(left)
	// 	nullRight := elementBlock.IsNull(right)
	// 	if nullLeft && nullRight {
	// 		return 0
	// 	}
	// 	if nullLeft {
	// 		return 1
	// 	}
	// 	if nullRight {
	// 		return -1
	// 	}
	// 	return elementBlock.CompareTo(left, 0, elementBlock.getSliceLength(left), elementBlock, right, 0, elementBlock.getSliceLength(right))
	// })
	return sortedPositions
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	if sr.directEncoded {
		return sr.directColumnWriter.GetIndexStreams(metadataWriter)
	}
	rowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
	dataCheckpoints := sr.dataStream.GetCheckpoints()
	presentCheckpoints := sr.presentStream.GetCheckpoints()
	for i := util.INT32_ZERO; i < sr.rowGroups.SizeInt32(); i++ {
		groupId := i
		columnStatistics := sr.rowGroups.GetByInt32(groupId).GetColumnStatistics()
		dataCheckpoint := dataCheckpoints.GetByInt32(groupId)
		presentCheckpoint := optional.Map(presentCheckpoints, func(checkpoints *util.ArrayList[*BooleanStreamCheckpoint]) *BooleanStreamCheckpoint {
			return checkpoints.GetByInt32(groupId)
		})
		positions := createSliceColumnPositionList(sr.compression != metadata.NONE, dataCheckpoint, presentCheckpoint)
		rowGroupIndexes.Add(metadata.NewRowGroupIndex(positions, columnStatistics))
	}
	slice := metadataWriter.WriteRowIndexes(rowGroupIndexes)
	stream := metadata.NewStream(sr.columnId, metadata.ROW_INDEX, slice.SizeInt32(), false)
	return util.NewArrayList(NewStreamDataOutput(slice, stream))
}

func createSliceColumnPositionList(compressed bool, dataCheckpoint LongStreamCheckpoint, presentCheckpoint *optional.Optional[*BooleanStreamCheckpoint]) *util.ArrayList[int32] {
	positionList := util.NewArrayList[int32]()
	presentCheckpoint.IfPresent(func(booleanStreamCheckpoint *BooleanStreamCheckpoint) {
		positionList.AddAll(booleanStreamCheckpoint.ToPositionList(compressed))
	})
	positionList.AddAll(dataCheckpoint.ToPositionList(compressed))
	return positionList
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput] {
	// bloomFilters := sr.rowGroups.stream().Map(func(rowGroup interface{}) {
	// 	rowGroup.getColumnStatistics().getBloomFilter()
	// }).filter(Objects.nonNull).collect(toImmutableList())

	bloomFilters := util.NewArrayList[*metadata.BloomFilter]()
	for _, rs := range sr.rowGroups.ToArray() {
		br := rs.GetColumnStatistics().GetBloomFilter()
		if br != nil {
			bloomFilters.Add(br)
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
func (sr *SliceDictionaryColumnWriter) GetDataStreams() *util.ArrayList[*StreamDataOutput] {
	util.CheckState(sr.closed)
	if sr.directEncoded {
		return sr.directColumnWriter.GetDataStreams()
	}
	outputDataStreams := util.NewArrayList[*StreamDataOutput]()
	sr.presentStream.GetStreamDataOutput(sr.columnId).IfPresent(func(s *StreamDataOutput) {
		outputDataStreams.Add(s)
	})
	outputDataStreams.Add(sr.dataStream.GetStreamDataOutput(sr.columnId))
	outputDataStreams.Add(sr.dictionaryLengthStream.GetStreamDataOutput(sr.columnId))
	outputDataStreams.Add(sr.dictionaryDataStream.GetStreamDataOutput(sr.columnId))
	return outputDataStreams
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetBufferedBytes() int64 {
	util.CheckState(!sr.closed)
	if sr.directEncoded {
		return sr.directColumnWriter.GetBufferedBytes()
	}
	return int64(sr.GetIndexBytes() + sr.GetDictionaryBytes())
}

// @Override
func (sr *SliceDictionaryColumnWriter) GetRetainedBytes() int64 {
	retainedBytes := int64(0)
	if sr.directColumnWriter != nil {
		retainedBytes = int64(SLICEDICTIONARY_INSTANCE_SIZE) + sr.values.SizeOf() + sr.dataStream.GetRetainedBytes() + sr.presentStream.GetRetainedBytes() + sr.dictionaryDataStream.GetRetainedBytes() + sr.dictionaryLengthStream.GetRetainedBytes() + sr.dictionary.GetRetainedSizeInBytes() + sr.directColumnWriter.GetRetainedBytes()
	} else {
		retainedBytes = int64(SLICEDICTIONARY_INSTANCE_SIZE) + sr.values.SizeOf() + sr.dataStream.GetRetainedBytes() + sr.presentStream.GetRetainedBytes() + sr.dictionaryDataStream.GetRetainedBytes() + sr.dictionaryLengthStream.GetRetainedBytes() + sr.dictionary.GetRetainedSizeInBytes()
	}

	for _, rowGroup := range sr.rowGroups.ToArray() {
		retainedBytes += rowGroup.GetColumnStatistics().GetRetainedSizeInBytes()
	}
	return retainedBytes
}

// @Override
func (sr *SliceDictionaryColumnWriter) Reset() {
	util.CheckState(sr.closed)
	sr.closed = false
	sr.dataStream.Reset()
	sr.presentStream.Reset()
	sr.dictionaryDataStream.Reset()
	sr.dictionaryLengthStream.Reset()
	sr.rowGroups.Clear()
	sr.rowGroupValueCount = 0
	sr.statisticsBuilder = sr.statisticsBuilderSupplier.Get()
	sr.columnEncoding = nil
	sr.dictionary.Clear()
	sr.rawBytes = 0
	sr.totalValueCount = 0
	sr.totalNonNullValueCount = 0
	if sr.directEncoded {
		sr.directEncoded = false
		sr.directColumnWriter.Reset()
	}
}

type DictionaryRowGroup struct {
	dictionaryIndexes *array.IntBigArray
	valueCount        int32
	columnStatistics  *metadata.ColumnStatistics
}

func NewDictionaryRowGroup(dictionaryIndexes *array.IntBigArray, valueCount int32, columnStatistics *metadata.ColumnStatistics) *DictionaryRowGroup {
	dp := new(DictionaryRowGroup)
	dp.dictionaryIndexes = dictionaryIndexes
	dp.valueCount = valueCount
	dp.columnStatistics = columnStatistics
	return dp
}

func (dp *DictionaryRowGroup) GetDictionaryIndexes() *array.IntBigArray {
	return dp.dictionaryIndexes
}

func (dp *DictionaryRowGroup) GetValueCount() int32 {
	return dp.valueCount
}

func (dp *DictionaryRowGroup) GetColumnStatistics() *metadata.ColumnStatistics {
	return dp.columnStatistics
}

type Ints struct {
	data []int32
	b    block.Block
}

func NewInts(data []int32, b block.Block) *Ints {
	is := new(Ints)
	is.data = data
	is.b = b
	return is
}

func (is Ints) Len() int { return len(is.data) }

func (is Ints) Less(i, j int) bool {
	left := int32(i)
	right := int32(j)
	nullLeft := is.b.IsNull(left)
	nullRight := is.b.IsNull(right)
	if nullRight {
		return true
	}
	if nullLeft && nullRight {
		return false
	}
	if nullLeft {
		return false
	}
	return is.b.CompareTo(left, 0, is.b.GetSliceLength(left), is.b, right, 0, is.b.GetSliceLength(right)) < 0
}

func (is Ints) Swap(i, j int) {
	is.data[i], is.data[j] = is.data[j], is.data[i]
}
