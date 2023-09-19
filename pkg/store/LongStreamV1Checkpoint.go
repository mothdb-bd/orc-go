package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type LongStreamV1Checkpoint struct {
	//继承
	LongStreamCheckpoint

	offset                int32
	inputStreamCheckpoint int64
}

func NewLongStreamV1Checkpoint(offset int32, inputStreamCheckpoint int64) *LongStreamV1Checkpoint {
	lt := new(LongStreamV1Checkpoint)
	lt.offset = offset
	lt.inputStreamCheckpoint = inputStreamCheckpoint
	return lt
}
func NewLongStreamV1Checkpoint2(compressed bool, positionsList *ColumnPositionsList) *LongStreamV1Checkpoint {
	lt := new(LongStreamV1Checkpoint)
	lt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	lt.offset = positionsList.NextPosition()
	return lt
}

func (lt *LongStreamV1Checkpoint) GetOffset() int32 {
	return lt.offset
}

func (lt *LongStreamV1Checkpoint) GetInputStreamCheckpoint() int64 {
	return lt.inputStreamCheckpoint
}

// @Override
func (lt *LongStreamV1Checkpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	l := util.NewArrayList[int32]()
	l.AddAll(CreateInputStreamPositionList(compressed, lt.inputStreamCheckpoint))
	l.Add(lt.offset)
	return l
}

func (bt *LongStreamV1Checkpoint) String() string {
	return "LongStreamV1Checkpoint"
}
