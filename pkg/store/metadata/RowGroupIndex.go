package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type RowGroupIndex struct {
	positions  *util.ArrayList[int32]
	statistics *ColumnStatistics
}

func NewRowGroupIndex(positions *util.ArrayList[int32], statistics *ColumnStatistics) *RowGroupIndex {
	rx := new(RowGroupIndex)
	rx.positions = positions
	rx.statistics = statistics
	return rx
}

func (rx *RowGroupIndex) GetPositions() *util.ArrayList[int32] {
	return rx.positions
}

func (rx *RowGroupIndex) GetColumnStatistics() *ColumnStatistics {
	return rx.statistics
}
