package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type CachingMothDataSource struct {
	// 继承
	MothDataSource

	dataSource    MothDataSource
	regionFinder  RegionFinder
	cachePosition int64
	cacheLength   int32
	cache         *slice.Slice
}

func NewCachingMothDataSource(dataSource MothDataSource, regionFinder RegionFinder) *CachingMothDataSource {
	ce := new(CachingMothDataSource)
	ce.dataSource = dataSource
	ce.regionFinder = regionFinder
	ce.cache = slice.EMPTY_SLICE
	return ce
}

// @Override
func (ce *CachingMothDataSource) GetId() *common.MothDataSourceId {
	return ce.dataSource.GetId()
}

// @Override
func (ce *CachingMothDataSource) GetReadBytes() int64 {
	return ce.dataSource.GetReadBytes()
}

// @Override
func (ce *CachingMothDataSource) GetReadTimeNanos() int64 {
	return ce.dataSource.GetReadTimeNanos()
}

// @Override
func (ce *CachingMothDataSource) GetEstimatedSize() int64 {
	return ce.dataSource.GetEstimatedSize()
}

// @Override
func (ce *CachingMothDataSource) GetRetainedSize() int64 {
	return ce.dataSource.GetRetainedSize()
}

// @VisibleForTesting
func (ce *CachingMothDataSource) readCacheAt(offset int64) {
	newCacheRange := ce.regionFinder.GetRangeFor(offset)
	ce.cachePosition = newCacheRange.GetOffset()
	ce.cacheLength = newCacheRange.GetLength()
	ce.cache = ce.dataSource.ReadFully(newCacheRange.GetOffset(), ce.cacheLength)
}

// @Override
func (ce *CachingMothDataSource) ReadTail(length int32) *slice.Slice {
	panic("unsupported operation")
}

// @Override
func (ce *CachingMothDataSource) ReadFully(position int64, length int32) *slice.Slice {
	if position < ce.cachePosition {
		panic(fmt.Sprintf("read request (offset %d length %d) is before cache (offset %d length %d)", position, length, ce.cachePosition, ce.cacheLength))
	}
	if position >= ce.cachePosition+int64(ce.cacheLength) {
		ce.readCacheAt(position)
	}
	if position+int64(length) > ce.cachePosition+int64(ce.cacheLength) {
		panic(fmt.Sprintf("read request (offset %d length %d) partially overlaps cache (offset %d length %d)", position, length, ce.cachePosition, ce.cacheLength))
	}
	s, _ := ce.cache.MakeSlice(int(util.Int32Exact(position-ce.cachePosition)), int(length))
	return s
}

// @Override
func (ce *CachingMothDataSource) ReadFully2(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader {
	builder := make(map[StreamId]MothDataReader)
	for k, v := range diskRanges {
		buffer := ce.ReadFully(v.GetOffset(), v.GetLength())
		builder[k] = NewMemoryMothDataReader(ce.dataSource.GetId(), buffer, int64(buffer.Size()))
	}
	return builder
}

// @Override
func (ce *CachingMothDataSource) Close() {
	ce.dataSource.Close()
}

// @Override
func (ce *CachingMothDataSource) String() string {
	return ce.dataSource.String()
}

type RegionFinder interface {
	GetRangeFor(desiredOffset int64) *DiskRange
}
