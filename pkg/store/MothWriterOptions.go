package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var ( //@VisibleForTesting
	DEFAULT_MAX_STRING_STATISTICS_LIMIT util.DataSize = util.Ofds(64, util.B) //@VisibleForTesting
	DEFAULT_MAX_COMPRESSION_BUFFER_SIZE util.DataSize = util.Ofds(256, util.KB)
	DEFAULT_BLOOM_FILTER_FPP            float64       = 0.05
	DEFAULT_STRIPE_MIN_SIZE             util.DataSize = util.Ofds(32, util.MB)
	DEFAULT_STRIPE_MAX_SIZE             util.DataSize = util.Ofds(64, util.MB)
	DEFAULT_STRIPE_MAX_ROW_COUNT        int32         = 10_000_000
	DEFAULT_ROW_GROUP_MAX_ROW_COUNT     int32         = 10_000
	DEFAULT_DICTIONARY_MAX_MEMORY       util.DataSize = util.Ofds(16, util.MB)
)

type MothWriterOptions struct {
	writerIdentification     metadata.WriterIdentification
	stripeMinSize            util.DataSize
	stripeMaxSize            util.DataSize
	stripeMaxRowCount        int32
	rowGroupMaxRowCount      int32
	dictionaryMaxMemory      util.DataSize
	maxStringStatisticsLimit util.DataSize
	maxCompressionBufferSize util.DataSize
	bloomFilterColumns       util.SetInterface[string]
	bloomFilterFpp           float64
}

func NewMothWriterOptions() *MothWriterOptions {
	return NewMothWriterOptions2(metadata.MOTH, DEFAULT_STRIPE_MIN_SIZE, DEFAULT_STRIPE_MAX_SIZE, DEFAULT_STRIPE_MAX_ROW_COUNT, DEFAULT_ROW_GROUP_MAX_ROW_COUNT, DEFAULT_DICTIONARY_MAX_MEMORY, DEFAULT_MAX_STRING_STATISTICS_LIMIT, DEFAULT_MAX_COMPRESSION_BUFFER_SIZE, util.EmptySet[string](), DEFAULT_BLOOM_FILTER_FPP)
}
func NewMothWriterOptions2(writerIdentification metadata.WriterIdentification, stripeMinSize util.DataSize, stripeMaxSize util.DataSize, stripeMaxRowCount int32, rowGroupMaxRowCount int32, dictionaryMaxMemory util.DataSize, maxStringStatisticsLimit util.DataSize, maxCompressionBufferSize util.DataSize, bloomFilterColumns util.SetInterface[string], bloomFilterFpp float64) *MothWriterOptions {
	ms := new(MothWriterOptions)
	ms.writerIdentification = writerIdentification
	ms.stripeMinSize = stripeMinSize
	ms.stripeMaxSize = stripeMaxSize
	ms.stripeMaxRowCount = stripeMaxRowCount
	ms.rowGroupMaxRowCount = rowGroupMaxRowCount
	ms.dictionaryMaxMemory = dictionaryMaxMemory
	ms.maxStringStatisticsLimit = maxStringStatisticsLimit
	ms.maxCompressionBufferSize = maxCompressionBufferSize
	ms.bloomFilterColumns = bloomFilterColumns
	ms.bloomFilterFpp = bloomFilterFpp
	return ms
}

func (ms *MothWriterOptions) GetWriterIdentification() metadata.WriterIdentification {
	return ms.writerIdentification
}

func (ms *MothWriterOptions) WithWriterIdentification(writerIdentification metadata.WriterIdentification) *MothWriterOptions {
	return BuilderFrom(ms).SetWriterIdentification(writerIdentification).Build()
}

func (ms *MothWriterOptions) GetStripeMinSize() util.DataSize {
	return ms.stripeMinSize
}

func (ms *MothWriterOptions) WithStripeMinSize(stripeMinSize util.DataSize) *MothWriterOptions {
	return BuilderFrom(ms).SetStripeMinSize(stripeMinSize).Build()
}

func (ms *MothWriterOptions) GetStripeMaxSize() util.DataSize {
	return ms.stripeMaxSize
}

func (ms *MothWriterOptions) WithStripeMaxSize(stripeMaxSize util.DataSize) *MothWriterOptions {
	return BuilderFrom(ms).SetStripeMaxSize(stripeMaxSize).Build()
}

func (ms *MothWriterOptions) GetStripeMaxRowCount() int32 {
	return ms.stripeMaxRowCount
}

func (ms *MothWriterOptions) WithStripeMaxRowCount(stripeMaxRowCount int32) *MothWriterOptions {
	return BuilderFrom(ms).SetStripeMaxRowCount(stripeMaxRowCount).Build()
}

func (ms *MothWriterOptions) GetRowGroupMaxRowCount() int32 {
	return ms.rowGroupMaxRowCount
}

func (ms *MothWriterOptions) WithRowGroupMaxRowCount(rowGroupMaxRowCount int32) *MothWriterOptions {
	return BuilderFrom(ms).SetRowGroupMaxRowCount(rowGroupMaxRowCount).Build()
}

func (ms *MothWriterOptions) GetDictionaryMaxMemory() util.DataSize {
	return ms.dictionaryMaxMemory
}

func (ms *MothWriterOptions) WithDictionaryMaxMemory(dictionaryMaxMemory util.DataSize) *MothWriterOptions {
	return BuilderFrom(ms).SetDictionaryMaxMemory(dictionaryMaxMemory).Build()
}

func (ms *MothWriterOptions) GetMaxStringStatisticsLimit() util.DataSize {
	return ms.maxStringStatisticsLimit
}

func (ms *MothWriterOptions) WithMaxStringStatisticsLimit(maxStringStatisticsLimit util.DataSize) *MothWriterOptions {
	return BuilderFrom(ms).SetMaxStringStatisticsLimit(maxStringStatisticsLimit).Build()
}

func (ms *MothWriterOptions) GetMaxCompressionBufferSize() util.DataSize {
	return ms.maxCompressionBufferSize
}

func (ms *MothWriterOptions) WithMaxCompressionBufferSize(maxCompressionBufferSize util.DataSize) *MothWriterOptions {
	return BuilderFrom(ms).SetMaxCompressionBufferSize(maxCompressionBufferSize).Build()
}

func (ms *MothWriterOptions) IsBloomFilterColumn(columnName string) bool {
	return ms.bloomFilterColumns.Has(columnName)
}

func (ms *MothWriterOptions) WithBloomFilterColumns(bloomFilterColumns util.SetInterface[string]) *MothWriterOptions {
	return BuilderFrom(ms).SetBloomFilterColumns(bloomFilterColumns).Build()
}

func (ms *MothWriterOptions) GetBloomFilterFpp() float64 {
	return ms.bloomFilterFpp
}

func (ms *MothWriterOptions) WithBloomFilterFpp(bloomFilterFpp float64) *MothWriterOptions {
	return BuilderFrom(ms).SetBloomFilterFpp(bloomFilterFpp).Build()
}

// @Override
func (ms *MothWriterOptions) String() string {
	return util.NewSB().AddUInt64("stripeMinSize", uint64(ms.stripeMinSize)).AddUInt64("stripeMaxSize", uint64(ms.stripeMaxSize)).AddInt32("stripeMaxRowCount", ms.stripeMaxRowCount).AddInt32("rowGroupMaxRowCount", ms.rowGroupMaxRowCount).AddUInt64("dictionaryMaxMemory", uint64(ms.dictionaryMaxMemory)).AddUInt64("maxStringStatisticsLimit", uint64(ms.maxStringStatisticsLimit)).AddUInt64("maxCompressionBufferSize", uint64(ms.maxCompressionBufferSize)).AddString("bloomFilterColumns", ms.bloomFilterColumns.String()).AddFloat64("bloomFilterFpp", ms.bloomFilterFpp).String()
}

func Build() *Builder {
	return BuilderFrom(NewMothWriterOptions())
}

func BuilderFrom(options *MothWriterOptions) *Builder {
	return NewBuilder(options)
}

type Builder struct {
	writerIdentification     metadata.WriterIdentification
	stripeMinSize            util.DataSize
	stripeMaxSize            util.DataSize
	stripeMaxRowCount        int32
	rowGroupMaxRowCount      int32
	dictionaryMaxMemory      util.DataSize
	maxStringStatisticsLimit util.DataSize
	maxCompressionBufferSize util.DataSize
	bloomFilterColumns       util.SetInterface[string]
	bloomFilterFpp           float64
}

func NewBuilder(options *MothWriterOptions) *Builder {
	br := new(Builder)
	br.writerIdentification = options.writerIdentification
	br.stripeMinSize = options.stripeMinSize
	br.stripeMaxSize = options.stripeMaxSize
	br.stripeMaxRowCount = options.stripeMaxRowCount
	br.rowGroupMaxRowCount = options.rowGroupMaxRowCount
	br.dictionaryMaxMemory = options.dictionaryMaxMemory
	br.maxStringStatisticsLimit = options.maxStringStatisticsLimit
	br.maxCompressionBufferSize = options.maxCompressionBufferSize
	br.bloomFilterColumns = options.bloomFilterColumns
	br.bloomFilterFpp = options.bloomFilterFpp
	return br
}

func (br *Builder) SetWriterIdentification(writerIdentification metadata.WriterIdentification) *Builder {
	br.writerIdentification = writerIdentification
	return br
}

func (br *Builder) SetStripeMinSize(stripeMinSize util.DataSize) *Builder {
	br.stripeMinSize = stripeMinSize
	return br
}

func (br *Builder) SetStripeMaxSize(stripeMaxSize util.DataSize) *Builder {
	br.stripeMaxSize = stripeMaxSize
	return br
}

func (br *Builder) SetStripeMaxRowCount(stripeMaxRowCount int32) *Builder {
	br.stripeMaxRowCount = stripeMaxRowCount
	return br
}

func (br *Builder) SetRowGroupMaxRowCount(rowGroupMaxRowCount int32) *Builder {
	br.rowGroupMaxRowCount = rowGroupMaxRowCount
	return br
}

func (br *Builder) SetDictionaryMaxMemory(dictionaryMaxMemory util.DataSize) *Builder {
	br.dictionaryMaxMemory = dictionaryMaxMemory
	return br
}

func (br *Builder) SetMaxStringStatisticsLimit(maxStringStatisticsLimit util.DataSize) *Builder {
	br.maxStringStatisticsLimit = maxStringStatisticsLimit
	return br
}

func (br *Builder) SetMaxCompressionBufferSize(maxCompressionBufferSize util.DataSize) *Builder {
	br.maxCompressionBufferSize = maxCompressionBufferSize
	return br
}

func (br *Builder) SetBloomFilterColumns(bloomFilterColumns util.SetInterface[string]) *Builder {
	br.bloomFilterColumns = bloomFilterColumns
	return br
}

func (br *Builder) SetBloomFilterFpp(bloomFilterFpp float64) *Builder {
	br.bloomFilterFpp = bloomFilterFpp
	return br
}

func (br *Builder) Build() *MothWriterOptions {
	return NewMothWriterOptions2(br.writerIdentification, br.stripeMinSize, br.stripeMaxSize, br.stripeMaxRowCount, br.rowGroupMaxRowCount, br.dictionaryMaxMemory, br.maxStringStatisticsLimit, br.maxCompressionBufferSize, br.bloomFilterColumns, br.bloomFilterFpp)
}
