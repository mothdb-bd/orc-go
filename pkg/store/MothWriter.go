package store

import (
	"sort"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	MOTHWRITER_INSTANCE_SIZE                int32  = util.SizeOf(&MothWriter{})
	MOTHDB_MOTH_WRITER_VERSION_METADATA_KEY string = "moth.writer.version"
	MOTHDB_MOTH_WRITER_VERSION              string
)

type MothWriter struct {
	stats                          *MothWriterStats
	mothDataSink                   MothDataSink
	types                          *util.ArrayList[block.Type]
	compression                    metadata.CompressionKind
	stripeMaxBytes                 int32
	chunkMaxLogicalBytes           int32
	stripeMaxRowCount              int32
	rowGroupMaxRowCount            int32
	maxCompressionBufferSize       int32
	userMetadata                   map[string]string
	metadataWriter                 *CompressedMetadataWriter
	closedStripes                  *util.ArrayList[*ClosedStripe]
	mothTypes                      *metadata.ColumnMetadata[*metadata.MothType]
	columnWriters                  *util.ArrayList[ColumnWriter]
	dictionaryCompressionOptimizer *DictionaryCompressionOptimizer
	stripeRowCount                 int32
	rowGroupRowCount               int32
	bufferedBytes                  int32
	columnWritersRetainedBytes     int64
	closedStripesRetainedBytes     int64
	previouslyRecordedSizeInBytes  int64
	closed                         bool
	fileRowCount                   int64
	fileStats                      *optional.Optional[*metadata.ColumnMetadata[*metadata.ColumnStatistics]]
	fileStatsRetainedBytes         int64
}

func init() {
	version := "1.0.0"
	MOTHDB_MOTH_WRITER_VERSION = version
}
func NewMothWriter(mothDataSink MothDataSink, columnNames *util.ArrayList[string], types *util.ArrayList[block.Type], mothTypes *metadata.ColumnMetadata[*metadata.MothType], compression metadata.CompressionKind, options *MothWriterOptions, userMetadata map[string]string, stats *MothWriterStats) *MothWriter {
	mr := new(MothWriter)

	mr.closedStripes = util.NewArrayList[*ClosedStripe]()

	mr.mothDataSink = mothDataSink
	mr.types = types
	mr.compression = compression
	stripeMinBytes := util.Int32Exact(int64(options.GetStripeMinSize().Bytes()))
	mr.stripeMaxBytes = util.Int32Exact(int64(options.GetStripeMaxSize().Bytes()))
	mr.chunkMaxLogicalBytes = maths.MaxInt32(1, mr.stripeMaxBytes/2)
	mr.stripeMaxRowCount = options.GetStripeMaxRowCount()
	mr.rowGroupMaxRowCount = options.GetRowGroupMaxRowCount()
	mr.maxCompressionBufferSize = util.Int32Exact(int64(options.GetMaxCompressionBufferSize().Bytes()))

	mr.userMetadata = make(map[string]string)
	util.PutAll(mr.userMetadata, userMetadata)
	mr.userMetadata[MOTHDB_MOTH_WRITER_VERSION_METADATA_KEY] = MOTHDB_MOTH_WRITER_VERSION
	mr.metadataWriter = NewCompressedMetadataWriter(metadata.NewMothMetadataWriter(options.GetWriterIdentification()), compression, mr.maxCompressionBufferSize)

	mr.stats = stats
	mr.mothTypes = mothTypes
	rootType := mothTypes.Get(metadata.ROOT_COLUMN)

	columnWriters := util.NewArrayList[ColumnWriter]()
	sliceColumnWriters := util.NewSet[*SliceDictionaryColumnWriter](util.SET_NonThreadSafe)
	for fieldId := util.INT32_ZERO; fieldId < types.SizeInt32(); fieldId++ {
		fieldColumnIndex := rootType.GetFieldTypeIndex(fieldId)
		fieldType := types.GetByInt32(fieldId)
		columnWriter := CreateColumnWriter(fieldColumnIndex, mothTypes, fieldType, compression, mr.maxCompressionBufferSize, options.GetMaxStringStatisticsLimit(), getBloomFilterBuilder(options, columnNames.GetByInt32(fieldId)))
		columnWriters.Add(columnWriter)

		sr, flag := columnWriter.(*SliceDictionaryColumnWriter)
		if flag {
			sliceColumnWriters.Add(sr)
		} else {
			for _, nestedColumnWriter := range columnWriter.GetNestedColumnWriters().ToArray() {
				nr, flag := nestedColumnWriter.(*SliceDictionaryColumnWriter)
				if flag {
					sliceColumnWriters.Add(nr)
				}
			}
		}
	}
	mr.columnWriters = columnWriters
	mr.dictionaryCompressionOptimizer = NewDictionaryCompressionOptimizer(sliceColumnWriters, stripeMinBytes, mr.stripeMaxBytes, mr.stripeMaxRowCount, util.Int32ExactU(options.GetDictionaryMaxMemory().Bytes()))
	mr.previouslyRecordedSizeInBytes = mr.GetRetainedBytes()
	stats.UpdateSizeInBytes(mr.previouslyRecordedSizeInBytes)
	return mr
}

func (mr *MothWriter) GetWrittenBytes() int64 {
	return mr.mothDataSink.Size()
}

func (mr *MothWriter) GetBufferedBytes() int32 {
	return mr.bufferedBytes
}

func (mr *MothWriter) GetStripeRowCount() int32 {
	return mr.stripeRowCount
}

func (mr *MothWriter) GetRetainedBytes() int64 {
	return int64(MOTHWRITER_INSTANCE_SIZE) + mr.columnWritersRetainedBytes + mr.closedStripesRetainedBytes + mr.mothDataSink.GetRetainedSizeInBytes() + mr.fileStatsRetainedBytes
}

func (mr *MothWriter) Write(page *spi.Page) {
	if page.GetPositionCount() == 0 {
		return
	}
	for page != nil {
		chunkRows := maths.MinInt32(page.GetPositionCount(), maths.MinInt32(mr.rowGroupMaxRowCount-mr.rowGroupRowCount, mr.stripeMaxRowCount-mr.stripeRowCount))
		chunk := page.GetRegion(0, chunkRows)
		for chunkRows > 1 && chunk.GetLogicalSizeInBytes() > int64(mr.chunkMaxLogicalBytes) {
			chunkRows /= 2
			chunk = chunk.GetRegion(0, chunkRows)
		}
		if chunkRows < page.GetPositionCount() {
			page = page.GetRegion(chunkRows, page.GetPositionCount()-chunkRows)
		} else {
			page = nil
		}
		mr.writeChunk(chunk)
		mr.fileRowCount += int64(chunkRows)
	}
	recordedSizeInBytes := mr.GetRetainedBytes()
	mr.stats.UpdateSizeInBytes(recordedSizeInBytes - mr.previouslyRecordedSizeInBytes)
	mr.previouslyRecordedSizeInBytes = recordedSizeInBytes
}

func (mr *MothWriter) writeChunk(chunk *spi.Page) {
	if mr.rowGroupRowCount == 0 {
		mr.columnWriters.ForEach(ColumnWriter.BeginRowGroup)
	}
	mr.bufferedBytes = 0
	for channel := util.INT32_ZERO; channel < chunk.GetChannelCount(); channel++ {
		writer := mr.columnWriters.GetByInt32(channel)
		writer.WriteBlock(chunk.GetBlock(channel))
		mr.bufferedBytes += int32(writer.GetBufferedBytes())
	}
	mr.rowGroupRowCount += chunk.GetPositionCount()
	util.CheckState(mr.rowGroupRowCount <= mr.rowGroupMaxRowCount)
	mr.stripeRowCount += chunk.GetPositionCount()
	if mr.rowGroupRowCount == mr.rowGroupMaxRowCount {
		mr.finishRowGroup()
	}
	mr.dictionaryCompressionOptimizer.Optimize(mr.bufferedBytes, mr.stripeRowCount)
	mr.bufferedBytes = util.Int32Exact(mr.columnWriters.Stream().MapToLong(ColumnWriter.GetBufferedBytes).Sum())
	if mr.stripeRowCount == mr.stripeMaxRowCount {
		mr.flushStripe(MAX_ROWS)
	} else if mr.bufferedBytes > mr.stripeMaxBytes {
		mr.flushStripe(MAX_BYTES)
	} else if mr.dictionaryCompressionOptimizer.IsFull(int64(mr.bufferedBytes)) {
		mr.flushStripe(DICTIONARY_FULL)
	}

	for i := 0; i < mr.columnWriters.Size(); i++ {
		cw := mr.columnWriters.Get(i)
		mr.columnWritersRetainedBytes += cw.GetRetainedBytes()
	}
	// mr.columnWritersRetainedBytes = mr.columnWriters.Stream().MapToLong(ColumnWriter.GetRetainedBytes).Sum()
}

func (mr *MothWriter) finishRowGroup() {
	columnStatistics := util.EmptyMap[metadata.MothColumnId, *metadata.ColumnStatistics]()
	mr.columnWriters.ForEach(func(columnWriter ColumnWriter) {
		util.PutAll(columnStatistics, columnWriter.FinishRowGroup())
	})
	mr.rowGroupRowCount = 0
}

func (mr *MothWriter) flushStripe(flushReason FlushReason) {
	outputData := util.NewArrayList[MothDataOutput]()
	stripeStartOffset := mr.mothDataSink.Size()
	if mr.closedStripes.IsEmpty() {
		outputData.Add(CreateDataOutput(metadata.MAGIC_SLICE))
		stripeStartOffset += metadata.MAGIC_SLICE.LenInt64()
	}
	outputData.AddAll(mr.bufferStripeData(stripeStartOffset, flushReason))
	if flushReason == CLOSED {
		outputData.AddAll(mr.bufferFileFooter())
	}
	mr.mothDataSink.Write(outputData)
	mr.columnWriters.ForEach(ColumnWriter.Reset)
	mr.dictionaryCompressionOptimizer.Reset()
	mr.rowGroupRowCount = 0
	mr.stripeRowCount = 0
	mr.bufferedBytes = util.Int32Exact(mr.columnWriters.Stream().MapToLong(ColumnWriter.GetBufferedBytes).Sum())
}

func (mr *MothWriter) bufferStripeData(stripeStartOffset int64, flushReason FlushReason) *util.ArrayList[MothDataOutput] {
	if mr.stripeRowCount == 0 {
		util.Verify2(flushReason == CLOSED, "An empty stripe is not allowed")
		mr.columnWriters.ForEach(ColumnWriter.Close)
		return util.EMPTY_LIST[MothDataOutput]()
	}
	if mr.rowGroupRowCount > 0 {
		mr.finishRowGroup()
	}
	mr.dictionaryCompressionOptimizer.FinalOptimize(mr.bufferedBytes)
	mr.columnWriters.ForEach(ColumnWriter.Close)
	outputData := util.NewArrayList[MothDataOutput]()
	allStreams := util.NewArrayList[*metadata.Stream]() // mr.columnWriters.Size() * 3
	indexLength := util.INT64_ZERO
	for _, columnWriter := range mr.columnWriters.ToArray() {
		for _, indexStream := range columnWriter.GetIndexStreams(mr.metadataWriter).ToArray() {
			outputData.Add(indexStream)
			allStreams.Add(indexStream.GetStream())
			indexLength += indexStream.Size()
		}
		for _, bloomFilter := range columnWriter.GetBloomFilters(mr.metadataWriter).ToArray() {
			outputData.Add(bloomFilter)
			allStreams.Add(bloomFilter.GetStream())
			indexLength += bloomFilter.Size()
		}
	}
	dataLength := util.INT64_ZERO
	dataStreams := util.NewCmpList[*StreamDataOutput](NewStreamDataOutputCmp()) //. columnWriters.size() * 2
	for _, columnWriter := range mr.columnWriters.ToArray() {
		streams := columnWriter.GetDataStreams()
		dataStreams.AddAll(streams)
		dataLength += streams.Stream().MapToLong((*StreamDataOutput).Size).Sum()
	}

	sort.Sort(dataStreams)

	for _, dataStream := range dataStreams.ToArray() {
		outputData.Add(dataStream)
		allStreams.Add(dataStream.GetStream())
	}
	columnEncodings := util.EmptyMap[metadata.MothColumnId, *metadata.ColumnEncoding]()
	mr.columnWriters.ForEach(func(columnWriter ColumnWriter) {
		util.PutAll(columnEncodings, columnWriter.GetColumnEncodings())
	})
	columnStatistics := util.EmptyMap[metadata.MothColumnId, *metadata.ColumnStatistics]()
	mr.columnWriters.ForEach(func(columnWriter ColumnWriter) {
		util.PutAll(columnStatistics, columnWriter.GetColumnStripeStatistics())
	})
	columnEncodings[metadata.ROOT_COLUMN] = metadata.NewColumnEncoding(metadata.DIRECT, 0)
	columnStatistics[metadata.ROOT_COLUMN] = metadata.NewColumnStatistics(int64(mr.stripeRowCount), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	stripeFooter := metadata.NewStripeFooter(allStreams, toColumnMetadata(columnEncodings, mr.mothTypes.Size()), time.UTC)
	footer := mr.metadataWriter.WriteStripeFooter(stripeFooter)
	outputData.Add(CreateDataOutput(footer))
	statistics := metadata.NewStripeStatistics(toColumnMetadata(columnStatistics, mr.mothTypes.Size()))
	stripeInformation := metadata.NewStripeInformation(mr.stripeRowCount, uint64(stripeStartOffset), uint64(indexLength), uint64(dataLength), uint64(footer.Size()))
	closedStripe := NewClosedStripe(stripeInformation, statistics)
	mr.closedStripes.Add(closedStripe)
	mr.closedStripesRetainedBytes += closedStripe.GetRetainedSizeInBytes()
	mr.stats.RecordStripeWritten(flushReason, int64(stripeInformation.GetTotalLength()), stripeInformation.GetNumberOfRows(), mr.dictionaryCompressionOptimizer.GetDictionaryMemoryBytes())
	return outputData
}

// @Override
func (mr *MothWriter) Close() {
	if mr.closed {
		return
	}
	mr.closed = true
	mr.stats.UpdateSizeInBytes(-mr.previouslyRecordedSizeInBytes)
	mr.previouslyRecordedSizeInBytes = 0
	mr.flushStripe(CLOSED)
	mr.mothDataSink.Close()
	mr.bufferedBytes = 0
}

func (mr *MothWriter) UpdateUserMetadata(updatedProperties map[string]string) {
	util.PutAll(mr.userMetadata, updatedProperties)
}

func (mr *MothWriter) bufferFileFooter() *util.ArrayList[MothDataOutput] {
	outputData := util.NewArrayList[MothDataOutput]()
	ma := metadata.NewMetadata(util.MapStream(util.MapStream(mr.closedStripes.Stream(), (*ClosedStripe).GetStatistics), optional.Of[*metadata.StripeStatistics]).ToList())
	metadataSlice := mr.metadataWriter.WriteMetadata(ma)
	outputData.Add(CreateDataOutput(metadataSlice))
	mr.fileStats = toFileStats(util.MapStream(util.MapStream(mr.closedStripes.Stream(), (*ClosedStripe).GetStatistics), (*metadata.StripeStatistics).GetColumnStatistics).ToList())
	mr.fileStatsRetainedBytes = optional.Map(mr.fileStats, func(stats *metadata.ColumnMetadata[*metadata.ColumnStatistics]) int64 {
		return stats.Stream().MapToLong((*metadata.ColumnStatistics).GetRetainedSizeInBytes).Sum()
	}).OrElse(0)

	userMetadata := util.EmptyMap[string, *slice.Slice]()
	for k, v := range mr.userMetadata {
		userMetadata[k], _ = slice.NewByString(v)
	}

	footer := metadata.NewFooter(uint64(mr.fileRowCount), util.Ternary(mr.rowGroupMaxRowCount == 0, optional.OptionalIntEmpty(), optional.OptionalIntof(mr.rowGroupMaxRowCount)), util.MapStream(mr.closedStripes.Stream(), (*ClosedStripe).GetStripeInformation).ToList(), mr.mothTypes, mr.fileStats, userMetadata, optional.Empty[uint32]())
	mr.closedStripes.Clear()
	mr.closedStripesRetainedBytes = 0
	footerSlice := mr.metadataWriter.WriteFooter(footer)
	outputData.Add(CreateDataOutput(footerSlice))
	postscriptSlice := mr.metadataWriter.WritePostscript(footerSlice.Length(), metadataSlice.Length(), mr.compression, mr.maxCompressionBufferSize)
	outputData.Add(CreateDataOutput(postscriptSlice))

	s := slice.NewBaseBuf(make([]byte, 1))
	s.WriteUInt8(uint8(postscriptSlice.Length()))
	outputData.Add(CreateDataOutput(s))
	return outputData
}

func (mr *MothWriter) GetFileRowCount() int64 {
	return mr.fileRowCount
}

func (mr *MothWriter) GetFileStats() *optional.Optional[*metadata.ColumnMetadata[*metadata.ColumnStatistics]] {
	return mr.fileStats
}

func getBloomFilterBuilder(options *MothWriterOptions, columnName string) func() metadata.BloomFilterBuilder {
	if options.IsBloomFilterColumn(columnName) {
		return func() metadata.BloomFilterBuilder {
			return metadata.NewUtf8BloomFilterBuilder(options.GetRowGroupMaxRowCount(), options.GetBloomFilterFpp())
		}
	}
	return metadata.NewNoOpBloomFilterBuilder
}

func toColumnMetadata[T basic.Object](data map[metadata.MothColumnId]T, expectedSize int32) *metadata.ColumnMetadata[T] {
	list := util.NewArrayList[T]()
	for i := util.INT32_ZERO; i < expectedSize; i++ {
		list.Add(data[metadata.NewMothColumnId(uint32(i))])
	}
	return metadata.NewColumnMetadata(list)
}

func toFileStats(stripes *util.ArrayList[*metadata.ColumnMetadata[*metadata.ColumnStatistics]]) *optional.Optional[*metadata.ColumnMetadata[*metadata.ColumnStatistics]] {
	if stripes.IsEmpty() {
		return optional.Empty[*metadata.ColumnMetadata[*metadata.ColumnStatistics]]()
	}
	columnCount := stripes.Get(0).Size()

	fileStats := util.NewArrayList[*metadata.ColumnStatistics]()
	for i := util.INT32_ZERO; i < columnCount; i++ {
		columnId := metadata.NewMothColumnId(uint32(i))
		fileStats.Add(metadata.MergeColumnStatistics(util.MapStream(stripes.Stream(), func(stripe *metadata.ColumnMetadata[*metadata.ColumnStatistics]) *metadata.ColumnStatistics {
			return stripe.Get(columnId)
		}).ToList()))
	}
	return optional.Of(metadata.NewColumnMetadata(fileStats))
}

var CLOSED_STRIPE_INSTANCE_SIZE int32 = util.SizeOf(&ClosedStripe{}) + util.SizeOf(&metadata.StripeInformation{})

type ClosedStripe struct {
	stripeInformation *metadata.StripeInformation
	statistics        *metadata.StripeStatistics
}

func NewClosedStripe(stripeInformation *metadata.StripeInformation, statistics *metadata.StripeStatistics) *ClosedStripe {
	ce := new(ClosedStripe)
	ce.stripeInformation = stripeInformation
	ce.statistics = statistics
	return ce
}

func (ce *ClosedStripe) GetStripeInformation() *metadata.StripeInformation {
	return ce.stripeInformation
}

func (ce *ClosedStripe) GetStatistics() *metadata.StripeStatistics {
	return ce.statistics
}

func (ce *ClosedStripe) GetRetainedSizeInBytes() int64 {
	return int64(MOTHWRITER_INSTANCE_SIZE) + ce.statistics.GetRetainedSizeInBytes()
}
