package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

type ColumnReader interface {
	ReadBlock() block.Block
	PrepareNextRead(batchSize int32)
	StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding])
	StartRowGroup(dataStreamSources *InputStreamSources)
	Close()
	GetRetainedSizeInBytes() int64
}
