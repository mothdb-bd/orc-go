package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MemoryMothDataSource struct {
	// 继承
	MothDataSource

	id        *common.MothDataSourceId
	data      *slice.Slice
	readBytes int64
}

func NewMemoryMothDataSource(id *common.MothDataSourceId, data *slice.Slice) *MemoryMothDataSource {
	me := new(MemoryMothDataSource)
	me.id = id
	me.data = data
	return me
}

// @Override
func (me *MemoryMothDataSource) GetId() *common.MothDataSourceId {
	return me.id
}

// @Override
func (me *MemoryMothDataSource) GetReadBytes() int64 {
	return me.readBytes
}

// @Override
func (me *MemoryMothDataSource) GetReadTimeNanos() int64 {
	return 0
}

// @Override
func (me *MemoryMothDataSource) GetEstimatedSize() int64 {
	return int64(me.data.Size())
}

// @Override
func (me *MemoryMothDataSource) GetRetainedSize() int64 {
	return int64(me.data.GetRetainedSize())
}

// @Override
func (me *MemoryMothDataSource) ReadTail(length int32) *slice.Slice {
	return me.ReadFully(int64(int32(me.data.Size())-length), length)
}

// @Override
func (me *MemoryMothDataSource) ReadFully(position int64, length int32) *slice.Slice {
	me.readBytes += int64(length)
	e, _ := util.ToIntExact(position)
	s, _ := me.data.MakeSlice(e, int(length))
	return s
}

// ReadFully2(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader
// @Override
func (me *MemoryMothDataSource) ReadFully2(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader {
	if len(diskRanges) == 0 {
		return make(map[StreamId]MothDataReader)
	}
	slices := make(map[StreamId]MothDataReader)
	for k, v := range diskRanges {
		diskRange := v
		slice := me.ReadFully(diskRange.GetOffset(), diskRange.GetLength())
		slices[k] = NewMemoryMothDataReader(me.id, slice, 0)
	}
	return slices
}

// @Override
func (me *MemoryMothDataSource) ToString() string {
	return me.id.String()
}

func (me *MemoryMothDataSource) Close() {
}
