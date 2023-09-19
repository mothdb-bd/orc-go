package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type ByteStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	offset                int32
	inputStreamCheckpoint int64
}

func NewByteStreamCheckpoint(offset int32, inputStreamCheckpoint int64) *ByteStreamCheckpoint {
	bt := new(ByteStreamCheckpoint)
	bt.offset = offset
	bt.inputStreamCheckpoint = inputStreamCheckpoint
	return bt
}
func NewByteStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *ByteStreamCheckpoint {
	bt := new(ByteStreamCheckpoint)
	bt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	bt.offset = positionsList.NextPosition()
	return bt
}

func (bt *ByteStreamCheckpoint) GetOffset() int32 {
	return bt.offset
}

func (bt *ByteStreamCheckpoint) GetInputStreamCheckpoint() int64 {
	return bt.inputStreamCheckpoint
}

func (bt *ByteStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	l := util.NewArrayList[int32]()
	l.AddAll(CreateInputStreamPositionList(compressed, bt.inputStreamCheckpoint))
	l.Add(bt.offset)
	// return ImmutableList.builder().AddAll(byteStreamCheckpoint.ToPositionList(compressed)).Add(offset).build()
	return l
	// return ImmutableList.builder().AddAll(createInputStreamPositionList(compressed, inputStreamCheckpoint)).Add(offset).build()
}

func (bt *ByteStreamCheckpoint) String() string {
	return "ByteStreamCheckpoint"
}
