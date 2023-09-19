package store

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BooleanStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	offset               int32
	byteStreamCheckpoint *ByteStreamCheckpoint
}

func NewBooleanStreamCheckpoint(offset int32, byteStreamCheckpoint *ByteStreamCheckpoint) *BooleanStreamCheckpoint {
	bt := new(BooleanStreamCheckpoint)
	bt.offset = offset
	bt.byteStreamCheckpoint = byteStreamCheckpoint
	return bt
}
func NewBooleanStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *BooleanStreamCheckpoint {
	bt := new(BooleanStreamCheckpoint)
	bt.byteStreamCheckpoint = NewByteStreamCheckpoint2(compressed, positionsList)
	bt.offset = positionsList.NextPosition()
	return bt
}

func (bt *BooleanStreamCheckpoint) GetOffset() int32 {
	return bt.offset
}

func (bt *BooleanStreamCheckpoint) GetByteStreamCheckpoint() *ByteStreamCheckpoint {
	return bt.byteStreamCheckpoint
}

func (bt *BooleanStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	l := util.NewArrayList[int32]()
	l.AddAll(bt.byteStreamCheckpoint.ToPositionList(compressed))
	l.Add(bt.offset)
	// return ImmutableList.builder().AddAll(byteStreamCheckpoint.ToPositionList(compressed)).Add(offset).build()
	return l
}

func (bt *BooleanStreamCheckpoint) String() string {
	return "BooleanStreamCheckpoint"
}
