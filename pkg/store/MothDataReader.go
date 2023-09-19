package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

type MothDataReader interface {
	GetMothDataSourceId() *common.MothDataSourceId
	GetRetainedSize() int64
	GetSize() int32
	GetMaxBufferSize() int32
	SeekBuffer(position int32) *slice.Slice

	// 返回当前类
	String() string
}
