package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DICTIONARY_MIN_COMPRESSION_RATIO float64       = 1.25
	DICTIONARY_MEMORY_MAX_RANGE      util.DataSize = util.Ofds(4, util.MB)
	DIRECT_COLUMN_SIZE_RANGE         util.DataSize = util.Ofds(4, util.MB)
)

type DictionaryCompressionOptimizer struct {
	allWriters                   util.SetInterface[*DictionaryColumnManager]
	directConversionCandidates   util.SetInterface[*DictionaryColumnManager]
	stripeMinBytes               int32
	stripeMaxBytes               int32
	stripeMaxRowCount            int32
	dictionaryMemoryMaxBytesLow  int32
	dictionaryMemoryMaxBytesHigh int32
	dictionaryMemoryBytes        int32
}

func NewDictionaryCompressionOptimizer(writers util.SetInterface[*SliceDictionaryColumnWriter], stripeMinBytes int32, stripeMaxBytes int32, stripeMaxRowCount int32, dictionaryMemoryMaxBytes int32) *DictionaryCompressionOptimizer {
	dr := new(DictionaryCompressionOptimizer)

	dr.allWriters = util.NewSet[*DictionaryColumnManager](util.SET_NonThreadSafe)
	dr.directConversionCandidates = util.NewSet[*DictionaryColumnManager](util.SET_NonThreadSafe)

	util.MapStream(writers.Stream(), func(t *SliceDictionaryColumnWriter) *DictionaryColumnManager {
		return NewDictionaryColumnManager(t)
	}).ForEach(func(t *DictionaryColumnManager) {
		dr.allWriters.Add(t)
	})

	util.CheckArgument2(stripeMinBytes >= 0, "stripeMinBytes is negative")
	dr.stripeMinBytes = stripeMinBytes
	util.CheckArgument2(stripeMaxBytes >= stripeMinBytes, "stripeMaxBytes is less than stripeMinBytes")
	dr.stripeMaxBytes = stripeMaxBytes
	util.CheckArgument2(stripeMaxRowCount >= 0, "stripeMaxRowCount is negative")
	dr.stripeMaxRowCount = stripeMaxRowCount
	util.CheckArgument2(dictionaryMemoryMaxBytes >= 0, "dictionaryMemoryMaxBytes is negative")
	dr.dictionaryMemoryMaxBytesHigh = dictionaryMemoryMaxBytes
	dr.dictionaryMemoryMaxBytesLow = maths.MaxInt32(dictionaryMemoryMaxBytes-int32(DICTIONARY_MEMORY_MAX_RANGE.Bytes()), 0)
	dr.directConversionCandidates.Add(dr.allWriters.List()...)
	return dr
}

func (dr *DictionaryCompressionOptimizer) GetDictionaryMemoryBytes() int32 {
	return dr.dictionaryMemoryBytes
}

func (dr *DictionaryCompressionOptimizer) IsFull(bufferedBytes int64) bool {
	if bufferedBytes > int64(dr.stripeMinBytes) {
		return dr.dictionaryMemoryBytes > dr.dictionaryMemoryMaxBytesLow
	}
	return dr.dictionaryMemoryBytes > dr.dictionaryMemoryMaxBytesHigh
}

func (dr *DictionaryCompressionOptimizer) Reset() {
	dr.directConversionCandidates.Clear()
	dr.directConversionCandidates.Add(dr.allWriters.List()...)
	dr.dictionaryMemoryBytes = 0
	dr.allWriters.ForEach(func(t *DictionaryColumnManager) {
		t.reset()
	})
}

func (dr *DictionaryCompressionOptimizer) FinalOptimize(bufferedBytes int32) {
	dr.convertLowCompressionStreams(bufferedBytes)
}

func (dr *DictionaryCompressionOptimizer) Optimize(bufferedBytes int32, stripeRowCount int32) {
	dr.dictionaryMemoryBytes = dr.allWriters.Stream().Filter(func(writer *DictionaryColumnManager) bool {
		return !writer.IsDirectEncoded()
	}).MapToInt32((*DictionaryColumnManager).GetDictionaryBytes).Sum()

	dr.allWriters.Stream().Filter(func(writer *DictionaryColumnManager) bool {
		return !writer.IsDirectEncoded()
	}).ForEach(func(column *DictionaryColumnManager) {
		column.UpdateHistory(stripeRowCount)
	})

	if dr.dictionaryMemoryBytes <= dr.dictionaryMemoryMaxBytesLow {
		return
	}
	bufferedBytes = dr.convertLowCompressionStreams(bufferedBytes)
	if dr.dictionaryMemoryBytes <= dr.dictionaryMemoryMaxBytesLow || bufferedBytes >= dr.stripeMaxBytes {
		return
	}
	nonDictionaryBufferedBytes := bufferedBytes
	for _, dictionaryWriter := range dr.allWriters.List() {
		if !dictionaryWriter.IsDirectEncoded() {
			nonDictionaryBufferedBytes -= int32(dictionaryWriter.GetBufferedBytes())
		}
	}
	for !dr.directConversionCandidates.IsEmpty() && dr.dictionaryMemoryBytes > dr.dictionaryMemoryMaxBytesHigh && bufferedBytes < dr.stripeMaxBytes {
		projection := dr.selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount)
		selectDictionaryColumnBufferedBytes := util.Int32Exact(projection.GetColumnToConvert().GetBufferedBytes())
		directBytes := dr.TryConvertToDirect(projection.GetColumnToConvert(), dr.getMaxDirectBytes(bufferedBytes))
		if directBytes.IsPresent() {
			bufferedBytes = bufferedBytes + directBytes.Get() - selectDictionaryColumnBufferedBytes
			nonDictionaryBufferedBytes += directBytes.Get()
		}
	}
	if bufferedBytes >= dr.stripeMaxBytes {
		return
	}
	if bufferedBytes >= dr.stripeMinBytes {
		currentCompressionRatio := dr.currentCompressionRatio(nonDictionaryBufferedBytes)
		for !dr.directConversionCandidates.IsEmpty() && bufferedBytes < dr.stripeMaxBytes {
			projection := dr.selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount)
			if projection.GetPredictedFileCompressionRatio() < currentCompressionRatio {
				return
			}
			selectDictionaryColumnBufferedBytes := util.Int32Exact(projection.GetColumnToConvert().GetBufferedBytes())
			directBytes := dr.TryConvertToDirect(projection.GetColumnToConvert(), dr.getMaxDirectBytes(bufferedBytes))
			if directBytes.IsPresent() {
				bufferedBytes = bufferedBytes + directBytes.Get() - selectDictionaryColumnBufferedBytes
				nonDictionaryBufferedBytes += directBytes.Get()
			}
		}
	}
}

func (dr *DictionaryCompressionOptimizer) convertLowCompressionStreams(bufferedBytes int32) int32 {
	for _, dictionaryWriter := range dr.directConversionCandidates.List() {
		if dictionaryWriter.GetCompressionRatio() < DICTIONARY_MIN_COMPRESSION_RATIO {
			columnBufferedBytes := util.Int32Exact(dictionaryWriter.GetBufferedBytes())
			directBytes := dr.TryConvertToDirect(dictionaryWriter, dr.getMaxDirectBytes(bufferedBytes))
			if directBytes.IsPresent() {
				bufferedBytes = bufferedBytes + directBytes.Get() - columnBufferedBytes
				if bufferedBytes >= dr.stripeMaxBytes {
					return bufferedBytes
				}
			}
		}
	}
	return bufferedBytes
}

func (dr *DictionaryCompressionOptimizer) TryConvertToDirect(dictionaryWriter *DictionaryColumnManager, maxDirectBytes int32) *optional.OptionalInt {
	dictionaryBytes := dictionaryWriter.GetDictionaryBytes()
	directBytes := dictionaryWriter.TryConvertToDirect(maxDirectBytes)
	if directBytes.IsPresent() {
		dr.dictionaryMemoryBytes -= dictionaryBytes
	}
	dr.directConversionCandidates.Remove(dictionaryWriter)
	return directBytes
}

func (dr *DictionaryCompressionOptimizer) currentCompressionRatio(totalNonDictionaryBytes int32) float64 {
	uncompressedBytes := totalNonDictionaryBytes
	compressedBytes := totalNonDictionaryBytes
	for _, column := range dr.allWriters.List() {
		if !column.IsDirectEncoded() {
			uncompressedBytes += int32(column.GetRawBytes())
			compressedBytes += column.GetDictionaryBytes()
		}
	}
	return 1.0 * float64(uncompressedBytes) / float64(compressedBytes)
}

func (dr *DictionaryCompressionOptimizer) selectDictionaryColumnToConvert(totalNonDictionaryBytes int32, stripeRowCount int32) *DictionaryCompressionProjection {
	util.CheckState(!dr.directConversionCandidates.IsEmpty())
	totalNonDictionaryBytesPerRow := float64(totalNonDictionaryBytes / stripeRowCount)
	totalDictionaryRawBytes := util.INT64_ZERO
	totalDictionaryBytes := util.INT32_ZERO
	totalDictionaryIndexBytes := util.INT32_ZERO
	totalDictionaryRawBytesPerRow := util.FLOAT64_ZERO
	totalDictionaryBytesPerNewRow := util.FLOAT64_ZERO
	totalDictionaryIndexBytesPerRow := util.FLOAT64_ZERO
	for _, column := range dr.allWriters.List() {
		if !column.IsDirectEncoded() {
			totalDictionaryRawBytes += column.GetRawBytes()
			totalDictionaryBytes += column.GetDictionaryBytes()
			totalDictionaryIndexBytes += column.GetIndexBytes()
			totalDictionaryRawBytesPerRow += column.GetRawBytesPerRow()
			totalDictionaryBytesPerNewRow += column.GetDictionaryBytesPerFutureRow()
			totalDictionaryIndexBytesPerRow += column.GetIndexBytesPerRow()
		}
	}
	totalUncompressedBytesPerRow := totalNonDictionaryBytesPerRow + totalDictionaryRawBytesPerRow
	var maxProjectedCompression *DictionaryCompressionProjection = nil
	for _, column := range dr.directConversionCandidates.List() {
		currentRawBytes := totalNonDictionaryBytes + int32(column.GetRawBytes())
		currentDictionaryBytes := totalDictionaryBytes - column.GetDictionaryBytes()
		currentIndexBytes := totalDictionaryIndexBytes - column.GetIndexBytes()
		currentTotalBytes := currentRawBytes + currentDictionaryBytes + currentIndexBytes
		rawBytesPerFutureRow := totalNonDictionaryBytesPerRow + column.GetRawBytesPerRow()
		dictionaryBytesPerFutureRow := totalDictionaryBytesPerNewRow - column.GetDictionaryBytesPerFutureRow()
		indexBytesPerFutureRow := totalDictionaryIndexBytesPerRow - column.GetIndexBytesPerRow()
		totalBytesPerFutureRow := rawBytesPerFutureRow + dictionaryBytesPerFutureRow + indexBytesPerFutureRow
		rowsToDictionaryMemoryLimit := int64(float64(dr.dictionaryMemoryMaxBytesLow-currentDictionaryBytes) / dictionaryBytesPerFutureRow)
		rowsToStripeMemoryLimit := int64(float64(dr.stripeMaxBytes-currentTotalBytes) / totalBytesPerFutureRow)
		rowsToStripeRowLimit := dr.stripeMaxRowCount - stripeRowCount
		rowsToLimit := maths.MinInt64s(rowsToDictionaryMemoryLimit, rowsToStripeMemoryLimit, int64(rowsToStripeRowLimit))
		predictedUncompressedSizeAtLimit := int64(totalNonDictionaryBytes) + totalDictionaryRawBytes + (int64(totalUncompressedBytesPerRow) * rowsToLimit)
		predictedCompressedSizeAtLimit := int64(currentTotalBytes) + (int64(totalBytesPerFutureRow) * rowsToLimit)
		predictedCompressionRatioAtLimit := 1.0 * float64(predictedUncompressedSizeAtLimit/predictedCompressedSizeAtLimit)
		if maxProjectedCompression == nil || maxProjectedCompression.GetPredictedFileCompressionRatio() < predictedCompressionRatioAtLimit {
			maxProjectedCompression = NewDictionaryCompressionProjection(column, predictedCompressionRatioAtLimit)
		}
	}
	return maxProjectedCompression
}

func (dr *DictionaryCompressionOptimizer) getMaxDirectBytes(bufferedBytes int32) int32 {
	return maths.MinInt32(dr.stripeMaxBytes, dr.stripeMaxBytes-bufferedBytes+int32(DIRECT_COLUMN_SIZE_RANGE.Bytes()))
}

func EstimateIndexBytesPerValue(dictionaryEntries int32) int32 {
	if dictionaryEntries <= 256 {
		return 1
	}
	if dictionaryEntries <= 65_536 {
		return 2
	}
	if dictionaryEntries <= 16_777_216 {
		return 3
	}
	return 4
}

type DictionaryColumn interface {
	GetValueCount() int64
	GetNonNullValueCount() int64
	GetRawBytes() int64
	GetDictionaryEntries() int32
	GetDictionaryBytes() int32
	GetIndexBytes() int32
	TryConvertToDirect(maxDirectBytes int32) *optional.OptionalInt
	GetBufferedBytes() int64
}

type DictionaryColumnManager struct {
	dictionaryColumn             DictionaryColumn
	directEncoded                bool
	rowCount                     int32
	pastValueCount               int64
	pastDictionaryEntries        int32
	pendingPastValueCount        int64
	pendingPastDictionaryEntries int32
}

func NewDictionaryColumnManager(dictionaryColumn DictionaryColumn) *DictionaryColumnManager {
	dr := new(DictionaryColumnManager)
	dr.dictionaryColumn = dictionaryColumn
	return dr
}

func (dr *DictionaryColumnManager) TryConvertToDirect(maxDirectBytes int32) *optional.OptionalInt {
	directBytes := dr.dictionaryColumn.TryConvertToDirect(maxDirectBytes)
	if directBytes.IsPresent() {
		dr.directEncoded = true
	}
	return directBytes
}

func (dr *DictionaryColumnManager) reset() {
	dr.directEncoded = false
	dr.pastValueCount = 0
	dr.pastDictionaryEntries = 0
	dr.pendingPastValueCount = 0
	dr.pendingPastDictionaryEntries = 0
}

func (dr *DictionaryColumnManager) UpdateHistory(rowCount int32) {
	dr.rowCount = rowCount
	currentValueCount := dr.dictionaryColumn.GetValueCount()
	if currentValueCount-dr.pendingPastValueCount >= 1024 {
		dr.pastValueCount = dr.pendingPastValueCount
		dr.pastDictionaryEntries = dr.pendingPastDictionaryEntries
		dr.pendingPastValueCount = currentValueCount
		dr.pendingPastDictionaryEntries = dr.dictionaryColumn.GetDictionaryEntries()
	}
}

func (dr *DictionaryColumnManager) GetRawBytes() int64 {
	util.CheckState(!dr.directEncoded)
	return dr.dictionaryColumn.GetRawBytes()
}

func (dr *DictionaryColumnManager) GetRawBytesPerRow() float64 {
	util.CheckState(!dr.directEncoded)
	return 1.0 * float64(dr.GetRawBytes()) / float64(dr.rowCount)
}

func (dr *DictionaryColumnManager) GetDictionaryBytes() int32 {
	util.CheckState(!dr.directEncoded)
	return dr.dictionaryColumn.GetDictionaryBytes()
}

func (dr *DictionaryColumnManager) GetDictionaryBytesPerFutureRow() float64 {
	util.CheckState(!dr.directEncoded)
	currentDictionaryEntries := dr.dictionaryColumn.GetDictionaryEntries()
	currentValueCount := dr.dictionaryColumn.GetValueCount()
	dictionaryBytesPerEntry := 1.0 * dr.dictionaryColumn.GetDictionaryBytes() / currentDictionaryEntries
	dictionaryEntriesPerFutureValue := 1.0 * float64(currentDictionaryEntries-dr.pastDictionaryEntries) / float64(currentValueCount-dr.pastValueCount)
	return float64(dictionaryBytesPerEntry) * dictionaryEntriesPerFutureValue
}

func (dr *DictionaryColumnManager) GetIndexBytes() int32 {
	util.CheckState(!dr.directEncoded)
	return dr.dictionaryColumn.GetIndexBytes()
}

func (dr *DictionaryColumnManager) GetIndexBytesPerRow() float64 {
	util.CheckState(!dr.directEncoded)
	return 1.0 * float64(dr.GetIndexBytes()) / float64(dr.rowCount)
}

func (dr *DictionaryColumnManager) GetCompressionRatio() float64 {
	util.CheckState(!dr.directEncoded)
	return 1.0 * float64(dr.GetRawBytes()) / float64(dr.GetBufferedBytes())
}

func (dr *DictionaryColumnManager) GetBufferedBytes() int64 {
	return dr.dictionaryColumn.GetBufferedBytes()
}

func (dr *DictionaryColumnManager) IsDirectEncoded() bool {
	return dr.directEncoded
}

type DictionaryCompressionProjection struct {
	columnToConvert               *DictionaryColumnManager
	predictedFileCompressionRatio float64
}

func NewDictionaryCompressionProjection(columnToConvert *DictionaryColumnManager, predictedFileCompressionRatio float64) *DictionaryCompressionProjection {
	dn := new(DictionaryCompressionProjection)
	dn.columnToConvert = columnToConvert
	dn.predictedFileCompressionRatio = predictedFileCompressionRatio
	return dn
}

func (dn *DictionaryCompressionProjection) GetColumnToConvert() *DictionaryColumnManager {
	return dn.columnToConvert
}

func (dn *DictionaryCompressionProjection) GetPredictedFileCompressionRatio() float64 {
	return dn.predictedFileCompressionRatio
}
