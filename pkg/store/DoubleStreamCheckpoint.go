package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type DoubleStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	inputStreamCheckpoint int64
}

func NewDoubleStreamCheckpoint(inputStreamCheckpoint int64) *DoubleStreamCheckpoint {
	dt := new(DoubleStreamCheckpoint)
	dt.inputStreamCheckpoint = inputStreamCheckpoint
	return dt
}
func NewDoubleStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *DoubleStreamCheckpoint {
	dt := new(DoubleStreamCheckpoint)
	dt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	return dt
}

func (dt *DoubleStreamCheckpoint) GetInputStreamCheckpoint() int64 {
	return dt.inputStreamCheckpoint
}

func (dt *DoubleStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	return CreateInputStreamPositionList(compressed, dt.inputStreamCheckpoint)
}

func (bt *DoubleStreamCheckpoint) String() string {
	return "DoubleStreamCheckpoint"
}
