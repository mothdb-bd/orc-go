package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MothLz4Decompressor struct {
	// 继承
	MothDecompressor

	mothDataSourceId *common.MothDataSourceId
	maxBufferSize    int32
	decompressor     Decompressor
}

func NewMothLz4Decompressor(mothDataSourceId *common.MothDataSourceId, maxBufferSize int32) *MothLz4Decompressor {
	mr := new(MothLz4Decompressor)

	mr.mothDataSourceId = mothDataSourceId
	mr.maxBufferSize = maxBufferSize
	return mr
}

// @Override
func (mr *MothLz4Decompressor) Decompress(input []byte, offset int32, length int32, output OutputBuffer) int32 {
	buffer := output.Initialize(mr.maxBufferSize)
	return mr.decompressor.Decompress(input, offset, length, buffer, 0, util.Lens(buffer))
}

// @Override
func (mr *MothLz4Decompressor) String() string {
	return "lz4"
}
