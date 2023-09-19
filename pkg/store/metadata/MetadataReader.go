package metadata

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MetadataReader interface {

	/**
	 * Read
	 */
	ReadPostScript(inputStream mothio.InputStream) *PostScript

	/**
	 * hiveWriterVersion
	 */
	ReadMetadata(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Metadata

	/**
	 * hiveWriterVersion
	 */
	ReadFooter(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Footer

	/**
	 * types
	 */
	ReadStripeFooter(types *ColumnMetadata[*MothType], inputStream mothio.InputStream, legacyFileTimeZone *time.Location) *StripeFooter

	/**
	 * hiveWriterVersion
	 */
	ReadRowIndexes(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *util.ArrayList[*RowGroupIndex]

	/**
	 * Reader
	 */
	ReadBloomFilterIndexes(inputStream mothio.InputStream) *util.ArrayList[*BloomFilter]
}
