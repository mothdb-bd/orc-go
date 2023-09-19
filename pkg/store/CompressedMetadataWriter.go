package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type CompressedMetadataWriter struct {
	metadataWriter metadata.MetadataWriter
	buffer         *MothOutputBuffer
}

func NewCompressedMetadataWriter(metadataWriter metadata.MetadataWriter, compression metadata.CompressionKind, bufferSize int32) *CompressedMetadataWriter {
	cr := new(CompressedMetadataWriter)
	cr.metadataWriter = metadataWriter
	cr.buffer = NewMothOutputBuffer(compression, bufferSize)
	return cr
}

func (cr *CompressedMetadataWriter) GetMothMetadataVersion() []uint32 {
	return cr.metadataWriter.GetMothMetadataVersion()
}

func (cr *CompressedMetadataWriter) WritePostscript(footerLength int32, metadataLength int32, compression metadata.CompressionKind, compressionBlockSize int32) *slice.Slice {
	output := slice.NewDynamicSliceOutput(64)
	cr.metadataWriter.WritePostscript(output, uint64(footerLength), uint64(metadataLength), compression, uint64(compressionBlockSize))
	return output.Slice()
}

func (cr *CompressedMetadataWriter) WriteMetadata(metadata *metadata.Metadata) *slice.Slice {
	cr.metadataWriter.WriteMetadata(cr.buffer, metadata)
	return cr.getSliceOutput()
}

func (cr *CompressedMetadataWriter) WriteFooter(footer *metadata.Footer) *slice.Slice {
	cr.metadataWriter.WriteFooter(cr.buffer, footer)
	return cr.getSliceOutput()
}

func (cr *CompressedMetadataWriter) WriteStripeFooter(footer *metadata.StripeFooter) *slice.Slice {
	cr.metadataWriter.WriteStripeFooter(cr.buffer, footer)
	return cr.getSliceOutput()
}

func (cr *CompressedMetadataWriter) WriteRowIndexes(rowGroupIndexes *util.ArrayList[*metadata.RowGroupIndex]) *slice.Slice {
	cr.metadataWriter.WriteRowIndexes(cr.buffer, rowGroupIndexes)
	return cr.getSliceOutput()
}

func (cr *CompressedMetadataWriter) WriteBloomFilters(bloomFilters *util.ArrayList[*metadata.BloomFilter]) *slice.Slice {
	cr.metadataWriter.WriteBloomFilters(cr.buffer, bloomFilters)
	return cr.getSliceOutput()
}

func (cr *CompressedMetadataWriter) getSliceOutput() *slice.Slice {
	cr.buffer.Close()
	output := slice.NewDynamicSliceOutput(util.Int32Exact(cr.buffer.GetOutputDataSize()))
	cr.buffer.WriteDataTo(output)
	slice := output.Slice()
	cr.buffer.Reset()
	return slice
}
