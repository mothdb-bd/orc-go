package store

import "github.com/mothdb-bd/orc-go/pkg/util"

var (
	DEFAULT_BLOOM_FILTERS_ENABLED  bool          = false
	DEFAULT_MAX_MERGE_DISTANCE     util.DataSize = util.Ofds(1, util.MB)
	DEFAULT_MAX_BUFFER_SIZE        util.DataSize = util.Ofds(8, util.MB)
	DEFAULT_TINY_STRIPE_THRESHOLD  util.DataSize = util.Ofds(8, util.MB)
	DEFAULT_STREAM_BUFFER_SIZE     util.DataSize = util.Ofds(8, util.MB)
	DEFAULT_MAX_BLOCK_SIZE         util.DataSize = util.Ofds(16, util.MB)
	DEFAULT_LAZY_READ_SMALL_RANGES bool          = true
	DEFAULT_NESTED_LAZY            bool          = true
)

type MothReaderOptions struct {
	bloomFiltersEnabled bool
	maxMergeDistance    util.DataSize
	maxBufferSize       util.DataSize
	tinyStripeThreshold util.DataSize
	streamBufferSize    util.DataSize
	maxBlockSize        util.DataSize
	lazyReadSmallRanges bool
	nestedLazy          bool
}

func NewMothReaderOptions() *MothReaderOptions {
	ms := new(MothReaderOptions)
	ms.bloomFiltersEnabled = DEFAULT_BLOOM_FILTERS_ENABLED
	ms.maxMergeDistance = DEFAULT_MAX_MERGE_DISTANCE
	ms.maxBufferSize = DEFAULT_MAX_BUFFER_SIZE
	ms.tinyStripeThreshold = DEFAULT_TINY_STRIPE_THRESHOLD
	ms.streamBufferSize = DEFAULT_STREAM_BUFFER_SIZE
	ms.maxBlockSize = DEFAULT_MAX_BLOCK_SIZE
	ms.lazyReadSmallRanges = DEFAULT_LAZY_READ_SMALL_RANGES
	ms.nestedLazy = DEFAULT_NESTED_LAZY
	return ms
}
func NewMothReaderOptions2(bloomFiltersEnabled bool, maxMergeDistance util.DataSize, maxBufferSize util.DataSize, tinyStripeThreshold util.DataSize, streamBufferSize util.DataSize, maxBlockSize util.DataSize, lazyReadSmallRanges bool, nestedLazy bool) *MothReaderOptions {
	ms := new(MothReaderOptions)
	ms.maxMergeDistance = maxMergeDistance
	ms.maxBufferSize = maxBufferSize
	ms.tinyStripeThreshold = tinyStripeThreshold
	ms.streamBufferSize = streamBufferSize
	ms.maxBlockSize = maxBlockSize
	ms.lazyReadSmallRanges = lazyReadSmallRanges
	ms.bloomFiltersEnabled = bloomFiltersEnabled
	ms.nestedLazy = nestedLazy
	return ms
}

func (ms *MothReaderOptions) IsBloomFiltersEnabled() bool {
	return ms.bloomFiltersEnabled
}

func (ms *MothReaderOptions) GetMaxMergeDistance() util.DataSize {
	return ms.maxMergeDistance
}

func (ms *MothReaderOptions) GetMaxBufferSize() util.DataSize {
	return ms.maxBufferSize
}

func (ms *MothReaderOptions) GetTinyStripeThreshold() util.DataSize {
	return ms.tinyStripeThreshold
}

func (ms *MothReaderOptions) GetStreamBufferSize() util.DataSize {
	return ms.streamBufferSize
}

func (ms *MothReaderOptions) GetMaxBlockSize() util.DataSize {
	return ms.maxBlockSize
}

func (ms *MothReaderOptions) IsLazyReadSmallRanges() bool {
	return ms.lazyReadSmallRanges
}

func (ms *MothReaderOptions) IsNestedLazy() bool {
	return ms.nestedLazy
}

func (ms *MothReaderOptions) WithBloomFiltersEnabled(bloomFiltersEnabled bool) *MothReaderOptions {
	return NewMothReaderOptions2(bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

func (ms *MothReaderOptions) WithMaxMergeDistance(maxMergeDistance util.DataSize) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

func (ms *MothReaderOptions) WithMaxBufferSize(maxBufferSize util.DataSize) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

func (ms *MothReaderOptions) WithTinyStripeThreshold(tinyStripeThreshold util.DataSize) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

func (ms *MothReaderOptions) WithStreamBufferSize(streamBufferSize util.DataSize) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

func (ms *MothReaderOptions) WithMaxReadBlockSize(maxBlockSize util.DataSize) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, maxBlockSize, ms.lazyReadSmallRanges, ms.nestedLazy)
}

// @Deprecated
func (ms *MothReaderOptions) WithLazyReadSmallRanges(lazyReadSmallRanges bool) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, lazyReadSmallRanges, ms.nestedLazy)
}

// @Deprecated
func (ms *MothReaderOptions) WithNestedLazy(nestedLazy bool) *MothReaderOptions {
	return NewMothReaderOptions2(ms.bloomFiltersEnabled, ms.maxMergeDistance, ms.maxBufferSize, ms.tinyStripeThreshold, ms.streamBufferSize, ms.maxBlockSize, ms.lazyReadSmallRanges, nestedLazy)
}
