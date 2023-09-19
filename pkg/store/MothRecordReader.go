package store

import (
	"fmt"
	"sort"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MR_INSTANCE_SIZE int32 = util.SizeOf(&MothRecordReader{})

type MothRecordReader struct {
	mothDataSource             MothDataSource
	columnReaders              []ColumnReader
	currentBytesPerCell        []int64
	maxBytesPerCell            []int64
	maxCombinedBytesPerRow     int64
	totalRowCount              int64
	splitLength                int64
	maxBlockBytes              int64
	mothTypes                  *metadata.ColumnMetadata[*metadata.MothType]
	currentPosition            int64
	currentStripePosition      int64
	currentBatchSize           int32
	nextBatchSize              int32
	maxBatchSize               int32
	stripes                    *util.ArrayList[*metadata.StripeInformation]
	stripeReader               *StripeReader
	currentStripe              int32
	currentStripeMemoryContext memory.AggregatedMemoryContext
	fileRowCount               int64
	stripeFilePositions        *util.ArrayList[int64]
	filePosition               int64
	rowGroups                  util.Iterator[*RowGroup]
	currentRowGroup            int32
	currentGroupRowCount       int64
	nextRowInGroup             int64
	userMetadata               map[string]*slice.Slice
	memoryUsage                memory.AggregatedMemoryContext
	mothDataSourceMemoryUsage  memory.LocalMemoryContext
	blockFactory               *MothBlockFactory
}

type StripeInfoCmp struct {
	util.Compare[*StripeInfo]
}

func (sp *StripeInfoCmp) Cmp(i, j *StripeInfo) int {
	return int(i.GetStripe().GetOffset() - i.GetStripe().GetOffset())
}

func NewMothRecordReader(readColumns *util.ArrayList[*MothColumn], readTypes *util.ArrayList[block.Type], readLayouts util.List[ProjectedLayout], predicate MothPredicate, numberOfRows int64, fileStripes *util.ArrayList[*metadata.StripeInformation], fileStats *optional.Optional[*metadata.ColumnMetadata[*metadata.ColumnStatistics]], stripeStats *util.ArrayList[*optional.Optional[*metadata.StripeStatistics]], mothDataSource MothDataSource, splitOffset int64, splitLength int64, mothTypes *metadata.ColumnMetadata[*metadata.MothType], decompressor *optional.Optional[MothDecompressor], rowsInRowGroup *optional.OptionalInt, legacyFileTimeZone *time.Location, hiveWriterVersion metadata.HiveWriterVersion, metadataReader metadata.MetadataReader, options *MothReaderOptions, userMetadata map[string]*slice.Slice, memoryUsage memory.AggregatedMemoryContext, initialBatchSize int32, fieldMapperFactory FieldMapperFactory) *MothRecordReader {
	mr := new(MothRecordReader)
	mr.rowGroups = util.NewArrayList[*RowGroup]().Iter()
	mr.maxBatchSize = MAX_BATCH_SIZE
	mr.currentStripe = -1
	mr.currentRowGroup = -1

	mr.mothTypes = mothTypes
	mr.memoryUsage = memoryUsage.NewAggregatedMemoryContext()
	mr.blockFactory = NewMothBlockFactory(options.IsNestedLazy())
	mr.maxBlockBytes = int64(options.GetMaxBlockSize().Bytes())
	stripeInfos := util.NewCmpListWithValues[*StripeInfo](new(StripeInfoCmp))
	for i := util.INT32_ZERO; i < fileStripes.SizeInt32(); i++ {
		stats := optional.Empty[*metadata.StripeStatistics]()
		if stripeStats.Size() == fileStripes.Size() {
			stats = stripeStats.GetByInt32(i)
		}
		stripeInfos.Add(NewStripeInfo(fileStripes.GetByInt32(i), stats))
	}
	sort.Sort(stripeInfos)

	totalRowCount := util.INT64_ZERO
	fileRowCount := util.INT64_ZERO
	stripes := util.NewArrayList[*metadata.StripeInformation]()
	stripeFilePositions := util.NewArrayList[int64]()
	if fileStats.IsEmpty() || predicate.Matches(numberOfRows, fileStats.Get()) {
		for _, info := range stripeInfos.ToArray() {
			stripe := info.GetStripe()
			if splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(stripe, info.GetStats(), predicate) {
				stripes.Add(stripe)
				stripeFilePositions.Add(fileRowCount)
				totalRowCount += int64(stripe.GetNumberOfRows())
			}
			fileRowCount += int64(stripe.GetNumberOfRows())
		}
	}
	mr.totalRowCount = totalRowCount
	mr.stripes = stripes
	mr.stripeFilePositions = stripeFilePositions
	mothDataSource = wrapWithCacheIfTinyStripes(mothDataSource, mr.stripes, options.GetMaxMergeDistance(), options.GetTinyStripeThreshold())
	mr.mothDataSource = mothDataSource
	mr.mothDataSourceMemoryUsage = memoryUsage.NewLocalMemoryContext("MothDataSource")
	mr.mothDataSourceMemoryUsage.SetBytes(mothDataSource.GetRetainedSize())
	mr.splitLength = splitLength
	mr.fileRowCount = int64(util.MapStream(stripeInfos.Stream(), (*StripeInfo).GetStripe).MapToInt32((*metadata.StripeInformation).GetNumberOfRows).Sum())
	mr.userMetadata = userMetadata
	mr.currentStripeMemoryContext = mr.memoryUsage.NewAggregatedMemoryContext()
	streamReadersMemoryContext := mr.memoryUsage.NewAggregatedMemoryContext()
	mr.stripeReader = NewStripeReader(mothDataSource, legacyFileTimeZone, decompressor, mothTypes, util.NewSetWithItems(util.SET_NonThreadSafe, readColumns.ToArray()...), rowsInRowGroup, predicate, hiveWriterVersion, metadataReader)
	mr.columnReaders = createColumnReaders(readColumns, readTypes, readLayouts, streamReadersMemoryContext, mr.blockFactory, fieldMapperFactory)
	mr.currentBytesPerCell = make([]int64, len(mr.columnReaders))
	mr.maxBytesPerCell = make([]int64, len(mr.columnReaders))
	mr.nextBatchSize = initialBatchSize
	return mr
}

func splitContainsStripe(splitOffset int64, splitLength int64, stripe *metadata.StripeInformation) bool {
	splitEndOffset := splitOffset + splitLength
	return uint64(splitOffset) <= stripe.GetOffset() && stripe.GetOffset() < uint64(splitEndOffset)
}

func isStripeIncluded(stripe *metadata.StripeInformation, stripeStats *optional.Optional[*metadata.StripeStatistics], predicate MothPredicate) bool {
	return optional.Map(optional.Map(stripeStats, (*metadata.StripeStatistics).GetColumnStatistics), func(columnStats *metadata.ColumnMetadata[*metadata.ColumnStatistics]) bool {
		return predicate.Matches(int64(stripe.GetNumberOfRows()), columnStats)
	}).OrElse(true)
}

// @VisibleForTesting
func wrapWithCacheIfTinyStripes(dataSource MothDataSource, stripes *util.ArrayList[*metadata.StripeInformation], maxMergeDistance util.DataSize, tinyStripeThreshold util.DataSize) MothDataSource {

	_, f1 := dataSource.(*MemoryMothDataSource)
	_, f2 := dataSource.(*CachingMothDataSource)
	if f1 || f2 {
		return dataSource
	}
	for _, stripe := range stripes.ToArray() {
		if stripe.GetTotalLength() > tinyStripeThreshold.Bytes() {
			return dataSource
		}
	}
	return NewCachingMothDataSource(dataSource, CreateTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold))
}

func (mr *MothRecordReader) GetFilePosition() int64 {
	return mr.filePosition
}

func (mr *MothRecordReader) GetFileRowCount() int64 {
	return mr.fileRowCount
}

func (mr *MothRecordReader) GetReaderPosition() int64 {
	return mr.currentPosition
}

func (mr *MothRecordReader) GetReaderRowCount() int64 {
	return mr.totalRowCount
}

func (mr *MothRecordReader) GetSplitLength() int64 {
	return mr.splitLength
}

func (mr *MothRecordReader) GetMaxCombinedBytesPerRow() int64 {
	return mr.maxCombinedBytesPerRow
}

func (mr *MothRecordReader) GetColumnTypes() *metadata.ColumnMetadata[*metadata.MothType] {
	return mr.mothTypes
}

// @Override
func (mr *MothRecordReader) Close() {
	// closer := Closer.create()
	// closer.register(mr.mothDataSource)
	mr.mothDataSource.Close()
	for _, column := range mr.columnReaders {
		if column != nil {
			// closer.register(column.Close)
			column.Close()
		}
	}
}

func (mr *MothRecordReader) NextPage() *spi.Page {
	mr.filePosition += int64(mr.currentBatchSize)
	mr.currentPosition += int64(mr.currentBatchSize)
	mr.currentBatchSize = 0
	if mr.nextRowInGroup >= mr.currentGroupRowCount {
		if !mr.advanceToNextRowGroup() {
			mr.filePosition = mr.fileRowCount
			mr.currentPosition = mr.totalRowCount
			return nil
		}
	}
	mr.currentBatchSize = maths.MinInt32(mr.nextBatchSize, mr.maxBatchSize)
	mr.nextBatchSize = maths.MinInt32(mr.currentBatchSize*BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE)
	mr.currentBatchSize = util.Int32Exact(maths.MinInt64s(int64(mr.currentBatchSize), mr.currentGroupRowCount-mr.nextRowInGroup))
	for _, column := range mr.columnReaders {
		if column != nil {
			column.PrepareNextRead(mr.currentBatchSize)
		}
	}
	mr.nextRowInGroup += int64(mr.currentBatchSize)
	mr.blockFactory.NextPage()
	util.FillInt64s(mr.currentBytesPerCell, 0)
	blocks := make([]block.Block, len(mr.columnReaders))
	for i := util.INT32_ZERO; i < util.Lens(mr.columnReaders); i++ {
		columnIndex := i
		blocks[columnIndex] = mr.blockFactory.CreateBlock(mr.currentBatchSize, mr.columnReaders[columnIndex], false)
		block.ListenForLoads(blocks[columnIndex], func(block block.Block) {
			mr.blockLoaded(columnIndex, block)
		})
	}
	page := spi.NewPage3(mr.currentBatchSize, blocks...)
	return page
}

func (mr *MothRecordReader) blockLoaded(columnIndex int32, block block.Block) {
	if block.GetPositionCount() <= 0 {
		return
	}
	mr.currentBytesPerCell[columnIndex] += block.GetSizeInBytes() / int64(mr.currentBatchSize)
	if mr.maxBytesPerCell[columnIndex] < mr.currentBytesPerCell[columnIndex] {
		delta := mr.currentBytesPerCell[columnIndex] - mr.maxBytesPerCell[columnIndex]
		mr.maxCombinedBytesPerRow += delta
		mr.maxBytesPerCell[columnIndex] = mr.currentBytesPerCell[columnIndex]
		mr.maxBatchSize = util.Int32Exact(maths.Min(int64(mr.maxBatchSize), maths.Max(1, mr.maxBlockBytes/mr.maxCombinedBytesPerRow)))
	}
}

func (mr *MothRecordReader) GetUserMetadata() map[string]*slice.Slice {
	return mr.userMetadata
}

func (mr *MothRecordReader) advanceToNextRowGroup() bool {
	mr.nextRowInGroup = 0
	for !mr.rowGroups.HasNext() && mr.currentStripe < mr.stripes.SizeInt32() {
		mr.advanceToNextStripe()
		mr.currentRowGroup = -1
	}
	if !mr.rowGroups.HasNext() {
		mr.currentGroupRowCount = 0
		return false
	}
	mr.currentRowGroup++
	currentRowGroup := mr.rowGroups.Next()
	mr.currentGroupRowCount = currentRowGroup.GetRowCount()
	if currentRowGroup.GetMinAverageRowBytes() > 0 {
		mr.maxBatchSize = util.Int32Exact(maths.Min(int64(mr.maxBatchSize), maths.Max(1, mr.maxBlockBytes/currentRowGroup.GetMinAverageRowBytes())))
	}
	mr.currentPosition = mr.currentStripePosition + currentRowGroup.GetRowOffset()
	mr.filePosition = mr.stripeFilePositions.GetByInt32(mr.currentStripe) + currentRowGroup.GetRowOffset()
	rowGroupStreamSources := currentRowGroup.GetStreamSources()
	for _, column := range mr.columnReaders {
		if column != nil {
			column.StartRowGroup(rowGroupStreamSources)
		}
	}
	return true
}

func (mr *MothRecordReader) advanceToNextStripe() {
	mr.currentStripeMemoryContext.Close()
	mr.currentStripeMemoryContext = mr.memoryUsage.NewAggregatedMemoryContext()
	mr.rowGroups = util.NewArrayList[*RowGroup]().Iter()
	mr.currentStripe++
	if mr.currentStripe >= mr.stripes.SizeInt32() {
		return
	}
	if mr.currentStripe > 0 {
		mr.currentStripePosition += int64(mr.stripes.GetByInt32(mr.currentStripe - 1).GetNumberOfRows())
	}
	stripeInformation := mr.stripes.GetByInt32(mr.currentStripe)
	stripe := mr.stripeReader.ReadStripe(stripeInformation, mr.currentStripeMemoryContext)
	if stripe != nil {
		dictionaryStreamSources := stripe.GetDictionaryStreamSources()
		columnEncodings := stripe.GetColumnEncodings()
		fileTimeZone := stripe.GetFileTimeZone()
		for _, column := range mr.columnReaders {
			if column != nil {
				column.StartStripe(fileTimeZone, dictionaryStreamSources, columnEncodings)
			}
		}
		mr.rowGroups = stripe.GetRowGroups().Iter()
	}
	mr.mothDataSourceMemoryUsage.SetBytes(mr.mothDataSource.GetRetainedSize())
}

func createColumnReaders(columns *util.ArrayList[*MothColumn], readTypes *util.ArrayList[block.Type], readLayouts util.List[ProjectedLayout], memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) []ColumnReader {
	columnReaders := make([]ColumnReader, columns.Size())
	for columnIndex := 0; columnIndex < columns.Size(); columnIndex++ {
		readType := readTypes.Get(columnIndex)
		column := columns.Get(columnIndex)
		projectedLayout := readLayouts.Get(columnIndex)
		columnReaders[columnIndex] = CreateColumnReader(readType, column, projectedLayout, memoryContext, blockFactory, fieldMapperFactory)
	}
	return columnReaders
}

// @VisibleForTesting
func (mr *MothRecordReader) getStreamReaderRetainedSizeInBytes() int64 {
	totalRetainedSizeInBytes := util.INT64_ZERO
	for _, column := range mr.columnReaders {
		if column != nil {
			totalRetainedSizeInBytes += column.GetRetainedSizeInBytes()
		}
	}
	return totalRetainedSizeInBytes
}

// @VisibleForTesting
func (mr *MothRecordReader) getCurrentStripeRetainedSizeInBytes() int64 {
	return mr.currentStripeMemoryContext.GetBytes()
}

// @VisibleForTesting
func (mr *MothRecordReader) getRetainedSizeInBytes() int64 {
	return int64(MR_INSTANCE_SIZE) + mr.getStreamReaderRetainedSizeInBytes() + mr.getCurrentStripeRetainedSizeInBytes()
}

// @VisibleForTesting
func (mr *MothRecordReader) getMemoryUsage() int64 {
	return mr.memoryUsage.GetBytes()
}

type StripeInfo struct {
	stripe *metadata.StripeInformation
	stats  *optional.Optional[*metadata.StripeStatistics]
}

func NewStripeInfo(stripe *metadata.StripeInformation, stats *optional.Optional[*metadata.StripeStatistics]) *StripeInfo {
	so := new(StripeInfo)
	so.stripe = stripe
	so.stats = stats
	return so
}

func (so *StripeInfo) GetStripe() *metadata.StripeInformation {
	return so.stripe
}

func (so *StripeInfo) GetStats() *optional.Optional[*metadata.StripeStatistics] {
	return so.stats
}

type LinearProbeRangeFinder struct {
	diskRanges *util.ArrayList[*DiskRange]
	index      int32
}

func NewLinearProbeRangeFinder(diskRanges *util.ArrayList[*DiskRange]) *LinearProbeRangeFinder {
	lr := new(LinearProbeRangeFinder)
	lr.diskRanges = diskRanges
	return lr
}

// @Override
func (lr *LinearProbeRangeFinder) GetRangeFor(desiredOffset int64) *DiskRange {
	for lr.index < lr.diskRanges.SizeInt32() {
		r := lr.diskRanges.GetByInt32(lr.index)
		if r.GetEnd() > desiredOffset {
			util.CheckArgument(r.GetOffset() <= desiredOffset)
			return r
		}
		lr.index++
	}
	panic(fmt.Sprintf("Invalid desiredOffset %d", desiredOffset))
}

func CreateTinyStripesRangeFinder(stripes *util.ArrayList[*metadata.StripeInformation], maxMergeDistance util.DataSize, tinyStripeThreshold util.DataSize) *LinearProbeRangeFinder {
	if stripes.IsEmpty() {
		return NewLinearProbeRangeFinder(util.EMPTY_LIST[*DiskRange]())
	}
	scratchDiskRanges := util.NewArrayList[*DiskRange]()
	util.MapStream(stripes.Stream(), func(stripe *metadata.StripeInformation) *DiskRange {
		return NewDiskRange(int64(stripe.GetOffset()), util.Int32ExactU(stripe.GetTotalLength()))
	}).ForEach(func(de *DiskRange) {
		scratchDiskRanges.Add(de)
	})
	diskRanges := MergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, tinyStripeThreshold)
	return NewLinearProbeRangeFinder(diskRanges)
}
