package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

type MothDataSource interface {
	GetId() *common.MothDataSourceId
	GetReadBytes() int64
	GetReadTimeNanos() int64
	GetEstimatedSize() int64
	GetRetainedSize() int64
	ReadTail(length int32) *slice.Slice
	ReadFully(position int64, length int32) *slice.Slice

	ReadFully2(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader
	Close()

	String() string
}
