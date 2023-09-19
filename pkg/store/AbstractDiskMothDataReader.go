package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

type AbstractDiskMothDataReader struct {
	// 继承
	MothDataReader

	mothDataSourceId    *common.MothDataSourceId
	dataSize            int32
	maxBufferSize       int32 //@Nullable
	buffer              []byte
	bufferSize          int32
	bufferStartPosition int32
}
