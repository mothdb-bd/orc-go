package store

import (
	"sort"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StripeReader struct {
	mothDataSource        MothDataSource
	legacyFileTimeZone    *time.Location
	decompressor          *optional.Optional[MothDecompressor]
	types                 *metadata.ColumnMetadata[*metadata.MothType]
	hiveWriterVersion     metadata.HiveWriterVersion
	includedMothColumnIds util.SetInterface[metadata.MothColumnId]
	rowsInRowGroup        *optional.OptionalInt
	predicate             MothPredicate
	metadataReader        metadata.MetadataReader
}

func NewStripeReader(mothDataSource MothDataSource, legacyFileTimeZone *time.Location, decompressor *optional.Optional[MothDecompressor], types *metadata.ColumnMetadata[*metadata.MothType], readColumns util.SetInterface[*MothColumn], rowsInRowGroup *optional.OptionalInt, predicate MothPredicate, hiveWriterVersion metadata.HiveWriterVersion, metadataReader metadata.MetadataReader) *StripeReader {
	sr := new(StripeReader)
	sr.mothDataSource = mothDataSource
	sr.legacyFileTimeZone = legacyFileTimeZone
	sr.decompressor = decompressor
	sr.types = types
	sr.includedMothColumnIds = getIncludeColumns(readColumns)
	sr.rowsInRowGroup = rowsInRowGroup
	sr.predicate = predicate
	sr.hiveWriterVersion = hiveWriterVersion
	sr.metadataReader = metadataReader
	return sr
}

func (sr *StripeReader) ReadStripe(stripe *metadata.StripeInformation, memoryUsage memory.AggregatedMemoryContext) *Stripe {
	stripeFooter := sr.readStripeFooter(stripe, memoryUsage)
	columnEncodings := stripeFooter.GetColumnEncodings()
	fileTimeZone := stripeFooter.GetTimeZone()
	streams := util.EmptyMap[StreamId, *metadata.Stream]()
	for _, stream := range stripeFooter.GetStreams().ToArray() {
		if sr.includedMothColumnIds.Has(stream.GetColumnId()) && isSupportedStreamType(stream, sr.types.Get(stream.GetColumnId()).GetMothTypeKind()) {
			streams[NewSId(stream)] = stream
		}
	}
	invalidCheckPoint := false
	if sr.rowsInRowGroup.IsPresent() && stripe.GetNumberOfRows() > sr.rowsInRowGroup.Get() {
		diskRanges := getDiskRanges(stripeFooter.GetStreams())

		// diskRanges = Maps.filterKeys(, Predicates.in(streams.keySet()))
		diskRanges = util.FilterKeys(diskRanges, func(k StreamId) bool {
			_, ok := streams[k]
			return ok
		})

		streamsData := sr.readDiskRanges(int64(stripe.GetOffset()), diskRanges, memoryUsage)
		bloomFilterIndexes := sr.readBloomFilterIndexes(streams, streamsData)
		columnIndexes := sr.readColumnIndexes(streams, streamsData, bloomFilterIndexes)
		selectedRowGroups := sr.selectRowGroups(stripe, columnIndexes)
		if selectedRowGroups.IsEmpty() {
			memoryUsage.Close()
			return nil
		}
		valueStreams := sr.createValueStreams(streams, streamsData, columnEncodings)
		dictionaryStreamSources := sr.createDictionaryStreamSources(streams, valueStreams, columnEncodings)
		rowGroups := sr.createRowGroups(stripe.GetNumberOfRows(), streams, valueStreams, columnIndexes, selectedRowGroups, columnEncodings)
		return NewStripe(int64(stripe.GetNumberOfRows()), fileTimeZone, columnEncodings, rowGroups, dictionaryStreamSources)
	}
	diskRangesBuilder := util.EmptyMap[StreamId, *DiskRange]()
	for k, v := range getDiskRanges(stripeFooter.GetStreams()) {
		_, ok := streams[k]
		if ok {
			diskRangesBuilder[k] = v
		}
	}
	diskRanges := diskRangesBuilder
	streamsData := sr.readDiskRanges(int64(stripe.GetOffset()), diskRanges, memoryUsage)
	minAverageRowBytes := util.INT64_ZERO
	for k := range streams {
		if k.GetStreamKind() == metadata.ROW_INDEX {
			rowGroupIndexes := sr.metadataReader.ReadRowIndexes(sr.hiveWriterVersion, NewMothInputStream(streamsData[k]))
			util.CheckState2(rowGroupIndexes.Size() == 1 || invalidCheckPoint, "expect a single row group or an invalid check point")
			totalBytes := util.INT64_ZERO
			totalRows := util.INT64_ZERO
			for _, rowGroupIndex := range rowGroupIndexes.ToArray() {
				columnStatistics := rowGroupIndex.GetColumnStatistics()
				if columnStatistics.HasMinAverageValueSizeInBytes() {
					totalBytes += columnStatistics.GetMinAverageValueSizeInBytes() * columnStatistics.GetNumberOfValues()
					totalRows += columnStatistics.GetNumberOfValues()
				}
			}
			if totalRows > 0 {
				minAverageRowBytes += totalBytes / totalRows
			}
		}
	}
	valueStreams := sr.createValueStreams(streams, streamsData, columnEncodings)
	dictionaryStreamSources := sr.createDictionaryStreamSources(streams, valueStreams, columnEncodings)
	builder := util.EmptyMap[StreamId, InputStreamSource]() // InputStreamSource
	for k, v := range valueStreams {
		builder[k] = NewValueInputStreamSource(v)
	}
	rowGroup := NewRowGroup(0, 0, int64(stripe.GetNumberOfRows()), minAverageRowBytes, NewInputStreamSources(builder))
	return NewStripe(int64(stripe.GetNumberOfRows()), fileTimeZone, columnEncodings, util.NewArrayList(rowGroup), dictionaryStreamSources)
}

func isSupportedStreamType(stream *metadata.Stream, mothTypeKind metadata.MothTypeKind) bool {
	if stream.GetStreamKind() == metadata.BLOOM_FILTER {
		switch mothTypeKind {
		case metadata.STRING, metadata.VARCHAR, metadata.CHAR:
			return false
		case metadata.TIMESTAMP, metadata.TIMESTAMP_INSTANT:
			return false
		default:
			return true
		}
	}
	if stream.GetStreamKind() == metadata.BLOOM_FILTER_UTF8 {
		return mothTypeKind != metadata.CHAR
	}
	return true
}

func (sr *StripeReader) readDiskRanges(stripeOffset int64, diskRanges map[StreamId]*DiskRange, memoryUsage memory.AggregatedMemoryContext) map[StreamId]MothChunkLoader {
	diskRangesBuilder := util.EmptyMap[StreamId, *DiskRange]()
	for k, v := range diskRanges {
		diskRangesBuilder[k] = NewDiskRange(stripeOffset+v.GetOffset(), v.GetLength())
	}
	diskRanges = diskRangesBuilder
	streamsData := sr.mothDataSource.ReadFully2(diskRanges)
	dataBuilder := util.EmptyMap[StreamId, MothChunkLoader]()
	for k, v := range streamsData {
		dataBuilder[k] = CreateChunkLoader2(v, sr.decompressor, memoryUsage)
	}
	return dataBuilder
}

func (sr *StripeReader) createValueStreams(streams map[StreamId]*metadata.Stream, streamsData map[StreamId]MothChunkLoader, columnEncodings *metadata.ColumnMetadata[*metadata.ColumnEncoding]) map[StreamId]IValueInputStream {
	valueStreams := util.EmptyMap[StreamId, IValueInputStream]()
	for k, v := range streams {
		streamId := k
		stream := v
		columnEncoding := columnEncodings.Get(stream.GetColumnId()).GetColumnEncodingKind()
		if isIndexStream(stream) || stream.GetLength() == 0 {
			continue
		}
		chunkLoader := streamsData[streamId]
		columnType := sr.types.Get(stream.GetColumnId()).GetMothTypeKind()
		valueStreams[streamId] = CreateValueStreams(streamId, chunkLoader, columnType, columnEncoding)
	}
	return valueStreams
}

func (sr *StripeReader) createDictionaryStreamSources(streams map[StreamId]*metadata.Stream, valueStreams map[StreamId]IValueInputStream, columnEncodings *metadata.ColumnMetadata[*metadata.ColumnEncoding]) *InputStreamSources {
	// 继承
	// InputStreamSource
	dictionaryStreamBuilder := util.EmptyMap[StreamId, InputStreamSource]()
	for k, v := range streams {
		streamId := k
		stream := v
		column := stream.GetColumnId()
		columnEncoding := columnEncodings.Get(column).GetColumnEncodingKind()
		if !isDictionary(stream, columnEncoding) {
			continue
		}
		valueStream := valueStreams[streamId]
		if valueStream == nil {
			continue
		}
		columnType := sr.types.Get(stream.GetColumnId()).GetMothTypeKind()
		streamCheckpoint := GetDictionaryStreamCheckpoint(streamId, columnType, columnEncoding)
		streamSource := CreateCheckpointStreamSource(valueStream, streamCheckpoint)
		dictionaryStreamBuilder[streamId] = streamSource
	}
	return NewInputStreamSources(dictionaryStreamBuilder)
}

func (sr *StripeReader) createRowGroups(rowsInStripe int32, streams map[StreamId]*metadata.Stream, valueStreams map[StreamId]IValueInputStream, columnIndexes map[StreamId]*util.ArrayList[*metadata.RowGroupIndex], selectedRowGroups util.SetInterface[int32], encodings *metadata.ColumnMetadata[*metadata.ColumnEncoding]) *util.ArrayList[*RowGroup] {
	rowsInRowGroup := sr.rowsInRowGroup.Get()
	rowGroupBuilder := util.NewCmpList[*RowGroup](RowGroup_CMP)
	for _, rowGroupId := range selectedRowGroups.List() {
		checkpoints := GetStreamCheckpoints(sr.includedMothColumnIds, sr.types, sr.decompressor.IsPresent(), rowGroupId, encodings, streams, columnIndexes)
		rowOffset := rowGroupId * rowsInRowGroup
		rowsInGroup := maths.MinInt32(rowsInStripe-rowOffset, rowsInRowGroup)

		minAverageRowBytes := util.INT64_ZERO
		for _, v := range columnIndexes {
			minAverageRowBytes += v.GetByInt32(rowGroupId).GetColumnStatistics().GetMinAverageValueSizeInBytes()
		}
		rowGroupBuilder.Add(createRowGroup(rowGroupId, rowOffset, rowsInGroup, minAverageRowBytes, valueStreams, checkpoints))
	}

	// 排序
	sort.Sort(rowGroupBuilder)
	return rowGroupBuilder
}

func createRowGroup(groupId int32, rowOffset int32, rowCount int32, minAverageRowBytes int64, valueStreams map[StreamId]IValueInputStream, checkpoints map[StreamId]StreamCheckpoint) *RowGroup {
	builder := util.EmptyMap[StreamId, InputStreamSource]() // InputStreamSource
	for k, v := range checkpoints {
		streamId := k
		checkpoint := v
		valueStream := valueStreams[streamId]
		if valueStream == nil {
			continue
		}
		builder[streamId] = CreateCheckpointStreamSource(valueStream, checkpoint)
	}
	rowGroupStreams := NewInputStreamSources(builder)
	return NewRowGroup(groupId, int64(rowOffset), int64(rowCount), minAverageRowBytes, rowGroupStreams)
}

func (sr *StripeReader) readStripeFooter(stripe *metadata.StripeInformation, memoryUsage memory.AggregatedMemoryContext) *metadata.StripeFooter {
	offset := stripe.GetOffset() + stripe.GetIndexLength() + stripe.GetDataLength()
	tailLength := util.Int32Exact(int64(stripe.GetFooterLength()))
	tailBuffer := sr.mothDataSource.ReadFully(int64(offset), tailLength)
	inputStream := NewMothInputStream(CreateChunkLoader(sr.mothDataSource.GetId(), tailBuffer, sr.decompressor, memoryUsage))
	return sr.metadataReader.ReadStripeFooter(sr.types, inputStream, sr.legacyFileTimeZone)
}

func isIndexStream(stream *metadata.Stream) bool {
	return stream.GetStreamKind() == metadata.ROW_INDEX || stream.GetStreamKind() == metadata.DICTIONARY_COUNT || stream.GetStreamKind() == metadata.BLOOM_FILTER || stream.GetStreamKind() == metadata.BLOOM_FILTER_UTF8
}

func (sr *StripeReader) readBloomFilterIndexes(streams map[StreamId]*metadata.Stream, streamsData map[StreamId]MothChunkLoader) map[metadata.MothColumnId]*util.ArrayList[*metadata.BloomFilter] {
	bloomFilters := make(map[metadata.MothColumnId]*util.ArrayList[*metadata.BloomFilter])
	for k, v := range streams {
		stream := v
		if stream.GetStreamKind() == metadata.BLOOM_FILTER_UTF8 {
			inputStream := NewMothInputStream(streamsData[k])
			bloomFilters[stream.GetColumnId()] = sr.metadataReader.ReadBloomFilterIndexes(inputStream)
		}
	}
	for k, v := range streams {
		stream := v
		_, ok := bloomFilters[stream.GetColumnId()]
		if stream.GetStreamKind() == metadata.BLOOM_FILTER && !ok {
			inputStream := NewMothInputStream(streamsData[k])
			bloomFilters[k.GetColumnId()] = sr.metadataReader.ReadBloomFilterIndexes(inputStream)
		}
	}
	return bloomFilters
}

func (sr *StripeReader) readColumnIndexes(streams map[StreamId]*metadata.Stream, streamsData map[StreamId]MothChunkLoader, bloomFilterIndexes map[metadata.MothColumnId]*util.ArrayList[*metadata.BloomFilter]) map[StreamId]*util.ArrayList[*metadata.RowGroupIndex] {
	columnIndexes := util.EmptyMap[StreamId, *util.ArrayList[*metadata.RowGroupIndex]]()
	for k, v := range streams {
		stream := v
		if stream.GetStreamKind() == metadata.ROW_INDEX {
			inputStream := NewMothInputStream(streamsData[k])
			bloomFilters := bloomFilterIndexes[k.GetColumnId()]
			rowGroupIndexes := sr.metadataReader.ReadRowIndexes(sr.hiveWriterVersion, inputStream)
			if bloomFilters != nil && !bloomFilters.IsEmpty() {
				newRowGroupIndexes := util.NewArrayList[*metadata.RowGroupIndex]()
				for i := 0; i < rowGroupIndexes.Size(); i++ {
					rowGroupIndex := rowGroupIndexes.Get(i)
					columnStatistics := rowGroupIndex.GetColumnStatistics().WithBloomFilter(bloomFilters.Get(i))
					newRowGroupIndexes.Add(metadata.NewRowGroupIndex(rowGroupIndex.GetPositions(), columnStatistics))
				}
				rowGroupIndexes = newRowGroupIndexes
			}
			columnIndexes[k] = rowGroupIndexes
		}
	}
	return columnIndexes
}

func (sr *StripeReader) selectRowGroups(stripe *metadata.StripeInformation, columnIndexes map[StreamId]*util.ArrayList[*metadata.RowGroupIndex]) util.SetInterface[int32] {
	rowsInRowGroup := util.INT32_ZERO
	if sr.rowsInRowGroup.IsPresent() {
		rowsInRowGroup = sr.rowsInRowGroup.Get()
	} else {
		panic("Cannot create row groups if row group info is missing")
	}

	rowsInStripe := stripe.GetNumberOfRows()
	groupsInStripe := ceil(rowsInStripe, rowsInRowGroup)
	selectedRowGroups := util.NewSet[int32](util.SET_NonThreadSafe)
	remainingRows := rowsInStripe
	for i := util.INT32_ZERO; i < groupsInStripe; i++ {
		rows := maths.MinInt32(remainingRows, rowsInRowGroup)
		statistics := getRowGroupStatistics(sr.types, columnIndexes, i)
		if sr.predicate.Matches(int64(rows), statistics) {
			selectedRowGroups.Add(i)
		}
		remainingRows -= rows
	}
	return selectedRowGroups
}

func getRowGroupStatistics(types *metadata.ColumnMetadata[*metadata.MothType], columnIndexes map[StreamId]*util.ArrayList[*metadata.RowGroupIndex], rowGroup int32) *metadata.ColumnMetadata[*metadata.ColumnStatistics] {

	// rowGroupIndexesByColumn := columnIndexes.entrySet().stream().collect(toImmutableMap(func(entry interface{}) {
	// 	entry.getKey().getColumnId().getId()
	// }, Entry.getValue))
	rowGroupIndexesByColumn := make(map[int32]*util.ArrayList[*metadata.RowGroupIndex], len(columnIndexes))
	for k, v := range columnIndexes {
		rowGroupIndexesByColumn[int32(k.GetColumnId().GetId())] = v
	}

	statistics := util.NewArrayList[*metadata.ColumnStatistics]()
	for i := util.INT32_ZERO; i < types.Size(); i++ {
		rowGroupIndexes := rowGroupIndexesByColumn[i]
		if rowGroupIndexes != nil {
			statistics.Add(rowGroupIndexes.GetByInt32(rowGroup).GetColumnStatistics())
		} else {
			statistics.Add(nil)
		}
	}
	return metadata.NewColumnMetadata(statistics)
}

func isDictionary(stream *metadata.Stream, columnEncoding metadata.ColumnEncodingKind) bool {
	return stream.GetStreamKind() == metadata.DICTIONARY_DATA || (stream.GetStreamKind() == metadata.LENGTH && (columnEncoding == metadata.DICTIONARY || columnEncoding == metadata.DICTIONARY_V2))
}

func getDiskRanges(streams *util.ArrayList[*metadata.Stream]) map[StreamId]*DiskRange {
	streamDiskRanges := make(map[StreamId]*DiskRange, streams.Size())
	stripeOffset := util.INT64_ZERO
	for _, stream := range streams.ToArray() {
		streamLength := stream.GetLength()
		if streamLength > 0 {
			streamDiskRanges[NewSId(stream)] = NewDiskRange(stripeOffset, streamLength)
		}
		stripeOffset += int64(streamLength)
	}
	return streamDiskRanges
}

func getIncludeColumns(includedColumns util.SetInterface[*MothColumn]) util.SetInterface[metadata.MothColumnId] {
	result := util.NewSet[metadata.MothColumnId](util.SET_NonThreadSafe)
	includeColumnsRecursive(result, includedColumns.List())
	return result
}

func includeColumnsRecursive(result util.SetInterface[metadata.MothColumnId], readColumns []*MothColumn) {
	for _, column := range readColumns {
		result.Add(column.GetColumnId())
		includeColumnsRecursive(result, column.GetNestedColumns().ToArray())
	}
}

func ceil(dividend int32, divisor int32) int32 {
	return ((dividend + divisor) - 1) / divisor
}
