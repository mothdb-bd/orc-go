package store

import (
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ColumnWriter interface {
	GetNestedColumnWriters() *util.ArrayList[ColumnWriter]
	// {
	//     return ImmutableList.of();
	// }

	GetColumnEncodings() map[metadata.MothColumnId]*metadata.ColumnEncoding
	BeginRowGroup()
	WriteBlock(block block.Block)
	FinishRowGroup() map[metadata.MothColumnId]*metadata.ColumnStatistics
	Close()
	GetColumnStripeStatistics() map[metadata.MothColumnId]*metadata.ColumnStatistics

	/**
	 * Write index streams to the output and return the streams in the
	 * order in which they were written.  The ordering is critical because
	 * the stream only contain a length with no offset.
	 */
	GetIndexStreams(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput]
	GetBloomFilters(metadataWriter *CompressedMetadataWriter) *util.ArrayList[*StreamDataOutput]

	/**
	 * Get the data streams to be written.
	 */
	GetDataStreams() *util.ArrayList[*StreamDataOutput]

	/**
	 * This method returns the size of the flushed data plus any unflushed data.
	 * If the output is compressed, flush data size is the size after compression.
	 */
	GetBufferedBytes() int64
	GetRetainedBytes() int64
	Reset()
}
