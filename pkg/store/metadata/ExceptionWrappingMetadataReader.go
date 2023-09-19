package metadata

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ExceptionWrappingMetadataReader struct {
	// 继承
	MetadataReader

	mothDataSourceId *common.MothDataSourceId
	delegate         MetadataReader
}

func NewExceptionWrappingMetadataReader(mothDataSourceId *common.MothDataSourceId, delegate MetadataReader) *ExceptionWrappingMetadataReader {
	er := new(ExceptionWrappingMetadataReader)
	er.mothDataSourceId = mothDataSourceId
	er.delegate = delegate
	return er
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadPostScript(inputStream mothio.InputStream) *PostScript {
	return er.delegate.ReadPostScript(inputStream)
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadMetadata(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Metadata {
	return er.delegate.ReadMetadata(hiveWriterVersion, inputStream)
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadFooter(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Footer {
	return er.delegate.ReadFooter(hiveWriterVersion, inputStream)
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadStripeFooter(types *ColumnMetadata[*MothType], inputStream mothio.InputStream, legacyFileTimeZone *time.Location) *StripeFooter {
	return er.delegate.ReadStripeFooter(types, inputStream, legacyFileTimeZone)
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadRowIndexes(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *util.ArrayList[*RowGroupIndex] {
	return er.delegate.ReadRowIndexes(hiveWriterVersion, inputStream)
}

// @Override
func (er *ExceptionWrappingMetadataReader) ReadBloomFilterIndexes(inputStream mothio.InputStream) *util.ArrayList[*BloomFilter] {
	return er.delegate.ReadBloomFilterIndexes(inputStream)
}

// func (er *ExceptionWrappingMetadataReader) propagate(throwable *Throwable, message string) *MothCorruptionException {
// 	propagateIfPossible(throwable, MothException.class)
// 	return NewMothCorruptionException(throwable, mothDataSourceId, message)
// }
