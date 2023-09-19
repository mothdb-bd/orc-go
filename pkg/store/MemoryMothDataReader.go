package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MemoryMothDataReader struct {
	// 继承
	MothDataReader

	mothDataSourceId *common.MothDataSourceId
	data             *slice.Slice
	retainedSize     int64
}

func NewMemoryMothDataReader(mothDataSourceId *common.MothDataSourceId, data *slice.Slice, retainedSize int64) *MemoryMothDataReader {
	mr := new(MemoryMothDataReader)
	mr.mothDataSourceId = mothDataSourceId
	mr.data = data
	mr.retainedSize = retainedSize
	return mr
}

// @Override
func (mr *MemoryMothDataReader) GetMothDataSourceId() *common.MothDataSourceId {
	return mr.mothDataSourceId
}

// @Override
func (mr *MemoryMothDataReader) GetRetainedSize() int64 {
	return mr.retainedSize
}

// @Override
func (mr *MemoryMothDataReader) GetSize() int32 {
	return int32(mr.data.Size())
}

// @Override
func (mr *MemoryMothDataReader) GetMaxBufferSize() int32 {
	return int32(mr.data.Size())
}

// @Override
func (mr *MemoryMothDataReader) SeekBuffer(newPosition int32) *slice.Slice {
	s, _ := mr.data.MakeSlice(int(newPosition), mr.data.Size()-int(newPosition))
	return s
}

func (mr *MemoryMothDataReader) String() string {
	return util.NewSB().AppendString("MemoryMothDataReader").AddString("mothDataSourceId", mr.mothDataSourceId.String()).AddInt32("dataSize", mr.data.SizeInt32()).String()
}
