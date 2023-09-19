package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type DecimalStreamCheckpoint struct {
	// 继承
	StreamCheckpoint

	inputStreamCheckpoint int64
}

func NewDecimalStreamCheckpoint(inputStreamCheckpoint int64) *DecimalStreamCheckpoint {
	dt := new(DecimalStreamCheckpoint)
	dt.inputStreamCheckpoint = inputStreamCheckpoint
	return dt
}
func NewDecimalStreamCheckpoint2(compressed bool, positionsList *ColumnPositionsList) *DecimalStreamCheckpoint {
	dt := new(DecimalStreamCheckpoint)
	dt.inputStreamCheckpoint = CreateInputStreamCheckpoint(compressed, positionsList)
	return dt
}

func (dt *DecimalStreamCheckpoint) GetInputStreamCheckpoint() int64 {
	return dt.inputStreamCheckpoint
}

func (dt *DecimalStreamCheckpoint) ToPositionList(compressed bool) *util.ArrayList[int32] {
	return CreateInputStreamPositionList(compressed, dt.inputStreamCheckpoint)
}

func (bt *DecimalStreamCheckpoint) String() string {
	return "DecimalStreamCheckpoint"
}
