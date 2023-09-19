package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var STRIPE_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&StripeStatistics{})

type StripeStatistics struct {
	columnStatistics    *ColumnMetadata[*ColumnStatistics]
	retainedSizeInBytes int64
}

func NewStripeStatistics(columnStatistics *ColumnMetadata[*ColumnStatistics]) *StripeStatistics {
	ss := new(StripeStatistics)
	ss.columnStatistics = columnStatistics
	ss.retainedSizeInBytes = int64(STRIPE_STATISTICS_INSTANCE_SIZE) + columnStatistics.Stream().MapToLong((*ColumnStatistics).GetRetainedSizeInBytes).Sum()
	return ss
}

func (ss *StripeStatistics) GetColumnStatistics() *ColumnMetadata[*ColumnStatistics] {
	return ss.columnStatistics
}

func (ss *StripeStatistics) GetRetainedSizeInBytes() int64 {
	return ss.retainedSizeInBytes
}
