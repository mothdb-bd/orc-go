package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractMothDataSource struct {
	// 继承
	MothDataSource

	id            *common.MothDataSourceId
	estimatedSize int64
	options       *MothReaderOptions
	readTimeNanos int64
	readBytes     int64
}

func NewAbstractMothDataSource(id *common.MothDataSourceId, estimatedSize int64, options *MothReaderOptions) *AbstractMothDataSource {
	ae := new(AbstractMothDataSource)
	ae.id = id
	ae.estimatedSize = estimatedSize
	ae.options = options
	return ae
}

type LazyBufferLoader struct {
	diskRange   *DiskRange
	bufferSlice *slice.Slice

	parent MothDataSource
}

func NewLazyBufferLoader(diskRange *DiskRange, parent MothDataSource) *LazyBufferLoader {
	lr := new(LazyBufferLoader)
	lr.diskRange = diskRange
	lr.parent = parent
	return lr
}

func (lr *LazyBufferLoader) LoadNestedDiskRangeBuffer(nestedDiskRange *DiskRange) *slice.Slice {
	lr.load()
	util.CheckArgument(lr.diskRange.Contains(nestedDiskRange))
	offset := util.Int32Exact(nestedDiskRange.GetOffset() - lr.diskRange.GetOffset())
	s, _ := lr.bufferSlice.MakeSlice(int(offset), int(nestedDiskRange.GetLength()))
	return s
}

func (lr *LazyBufferLoader) load() {
	if lr.bufferSlice != nil {
		return
	}
	lr.bufferSlice = lr.parent.ReadFully(lr.diskRange.GetOffset(), lr.diskRange.GetLength())
}

type MergedMothDataReader struct {
	mothDataSourceId *common.MothDataSourceId
	diskRange        *DiskRange
	lazyBufferLoader *LazyBufferLoader
	data             *slice.Slice
}

func NewMergedMothDataReader(mothDataSourceId *common.MothDataSourceId, diskRange *DiskRange, LazyBufferLoader *LazyBufferLoader) *MergedMothDataReader {
	mr := new(MergedMothDataReader)
	mr.mothDataSourceId = mothDataSourceId
	mr.diskRange = diskRange
	mr.lazyBufferLoader = LazyBufferLoader
	return mr
}

// @Override
func (mr *MergedMothDataReader) GetMothDataSourceId() *common.MothDataSourceId {
	return mr.mothDataSourceId
}

// @Override
func (mr *MergedMothDataReader) GetRetainedSize() int64 {
	return int64(util.Ternary(mr.data == nil, 0, mr.diskRange.GetLength()))
}

// @Override
func (mr *MergedMothDataReader) GetSize() int32 {
	return mr.diskRange.GetLength()
}

// @Override
func (mr *MergedMothDataReader) GetMaxBufferSize() int32 {
	return mr.diskRange.GetLength()
}

// @Override
func (mr *MergedMothDataReader) SeekBuffer(newPosition int32) *slice.Slice {
	if mr.data == nil {
		mr.data = mr.lazyBufferLoader.LoadNestedDiskRangeBuffer(mr.diskRange)
		if mr.data == nil {
			panic("Data loader returned null")
		}
		if mr.data.SizeInt32() != mr.diskRange.GetLength() {
			panic(fmt.Sprintf("Expected to load %d bytes, but %d bytes were loaded", mr.diskRange.GetLength(), mr.data.Length()))
		}
	}
	s, _ := mr.data.MakeSlice(int(newPosition), int(mr.data.Length()-newPosition))
	return s
}

// @Override
func (mr *MergedMothDataReader) String() string {
	return util.NewSB().AddString("mothDataSourceId", mr.mothDataSourceId.String()).AddString("diskRange", mr.diskRange.String()).String()
}
