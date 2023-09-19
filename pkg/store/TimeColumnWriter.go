package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/function"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

type TimeColumnWriter struct {
	// 继承
	LongColumnWriter
}

func NewTimeColumnWriter(columnId metadata.MothColumnId, kind block.Type, compression metadata.CompressionKind, bufferSize int32, statisticsBuilderSupplier function.Supplier[metadata.LongValueStatisticsBuilder]) *TimeColumnWriter {
	tr := new(TimeColumnWriter)
	tr.columnId = columnId
	tr.kind = kind
	tr.compressed = compression != metadata.NONE
	tr.columnEncoding = metadata.NewColumnEncoding(metadata.DIRECT_V2, 0)
	tr.dataStream = NewLongOutputStreamV2(compression, bufferSize, true, metadata.DATA)
	tr.presentStream = NewPresentOutputStream(compression, bufferSize)
	tr.statisticsBuilderSupplier = statisticsBuilderSupplier
	tr.statisticsBuilder = statisticsBuilderSupplier.Get()
	return tr
}

// @Override
func (tr *TimeColumnWriter) transformValue(value int64) int64 {
	return value / int64(time.Microsecond.Nanoseconds()*1000) //PICOSECONDS_PER_MICROSECOND
}
