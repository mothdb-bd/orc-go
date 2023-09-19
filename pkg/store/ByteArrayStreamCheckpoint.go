package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type ByteArrayStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	inputStreamCheckpoint int64
}

func NewByteArrayStreamCheckpoint(inputStreamCheckpoint int64) *ByteArrayStreamCheckpoint {
	bt := new(ByteArrayStreamCheckpoint)
	bt.inputStreamCheckpoint = inputStreamCheckpoint
	return bt
}
func NewByteArrayStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *ByteArrayStreamCheckpoint {
	bt := new(ByteArrayStreamCheckpoint)
	bt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	return bt
}

func (bt *ByteArrayStreamCheckpoint) GetInputStreamCheckpoint() int64 {
	return bt.inputStreamCheckpoint
}

func (bt *ByteArrayStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	return CreateInputStreamPositionList(compressed, bt.inputStreamCheckpoint)
}

func (bt *ByteArrayStreamCheckpoint) String() string {
	return "ByteArrayStreamCheckpoint"
}
