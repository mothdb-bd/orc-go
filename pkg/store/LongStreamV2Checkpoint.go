package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type LongStreamV2Checkpoint struct {
	//继承
	LongStreamCheckpoint

	offset                int32
	inputStreamCheckpoint int64
}

func NewLongStreamV2Checkpoint(offset int32, inputStreamCheckpoint int64) *LongStreamV2Checkpoint {
	lt := new(LongStreamV2Checkpoint)
	lt.offset = offset
	lt.inputStreamCheckpoint = inputStreamCheckpoint
	return lt
}
func NewLongStreamV2Checkpoint2(compressed bool, positionsList *ColumnPositionsList) *LongStreamV2Checkpoint {
	lt := new(LongStreamV2Checkpoint)
	lt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	lt.offset = positionsList.NextPosition()
	return lt
}

func (lt *LongStreamV2Checkpoint) GetOffset() int32 {
	return lt.offset
}

func (lt *LongStreamV2Checkpoint) GetInputStreamCheckpoint() int64 {
	return lt.inputStreamCheckpoint
}

// @Override
func (lt *LongStreamV2Checkpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	// return ImmutableList.builder().AddAll(createInputStreamPositionList(compressed, inputStreamCheckpoint)).Add(offset).build()
	l := util.NewArrayList[int32]()
	l.AddAll(CreateInputStreamPositionList(compressed, lt.inputStreamCheckpoint))
	l.Add(lt.offset)
	return l
}

func (bt *LongStreamV2Checkpoint) String() string {
	return "LongStreamV2Checkpoint"
}
