package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MetadataWriter interface {

	/**
	 * List<Integer> getMothMetadataVersion();
	 */
	GetMothMetadataVersion() []uint32

	WritePostscript(output slice.SliceOutput, footerLength uint64, metadataLength uint64, compression CompressionKind, compressionBlockSize uint64) int32

	WriteMetadata(output slice.SliceOutput, metadata *Metadata) int32

	WriteFooter(output slice.SliceOutput, footer *Footer) int32

	WriteStripeFooter(output slice.SliceOutput, footer *StripeFooter) int32

	WriteRowIndexes(output slice.SliceOutput, rowGroupIndexes *util.ArrayList[*RowGroupIndex]) int32

	WriteBloomFilters(output slice.SliceOutput, bloomFilters *util.ArrayList[*BloomFilter]) int32
}
