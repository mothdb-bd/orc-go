package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type FloatStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	inputStreamCheckpoint int64
}

func NewFloatStreamCheckpoint(inputStreamCheckpoint int64) *FloatStreamCheckpoint {
	ft := new(FloatStreamCheckpoint)
	ft.inputStreamCheckpoint = inputStreamCheckpoint
	return ft
}
func NewFloatStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *FloatStreamCheckpoint {
	ft := new(FloatStreamCheckpoint)
	ft.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	return ft
}

func (ft *FloatStreamCheckpoint) GetInputStreamCheckpoint() int64 {
	return ft.inputStreamCheckpoint
}

func (ft *FloatStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	return CreateInputStreamPositionList(compressed, ft.inputStreamCheckpoint)
}

func (bt *FloatStreamCheckpoint) String() string {
	return "FloatStreamCheckpoint"
}
