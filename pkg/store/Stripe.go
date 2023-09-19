package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Stripe struct {
	rowCount int64
	// fileTimeZone		*ZoneId
	fileTimeZone            *time.Location
	columnEncodings         *metadata.ColumnMetadata[*metadata.ColumnEncoding]
	rowGroups               *util.ArrayList[*RowGroup]
	dictionaryStreamSources *InputStreamSources
}

func NewStripe(rowCount int64, fileTimeZone *time.Location, columnEncodings *metadata.ColumnMetadata[*metadata.ColumnEncoding], rowGroups *util.ArrayList[*RowGroup], dictionaryStreamSources *InputStreamSources) *Stripe {
	se := new(Stripe)
	se.rowCount = rowCount
	se.fileTimeZone = fileTimeZone
	se.columnEncodings = columnEncodings
	se.rowGroups = rowGroups
	se.dictionaryStreamSources = dictionaryStreamSources
	return se
}

func (se *Stripe) GetRowCount() int64 {
	return se.rowCount
}

func (se *Stripe) GetFileTimeZone() *time.Location {
	return se.fileTimeZone
}

func (se *Stripe) GetColumnEncodings() *metadata.ColumnMetadata[*metadata.ColumnEncoding] {
	return se.columnEncodings
}

func (se *Stripe) GetRowGroups() *util.ArrayList[*RowGroup] {
	return se.rowGroups
}

func (se *Stripe) GetDictionaryStreamSources() *InputStreamSources {
	return se.dictionaryStreamSources
}

// @Override
func (se *Stripe) ToString() string {
	return util.NewSB().AddInt64("rowCount", se.rowCount).AddString("fileTimeZone", se.fileTimeZone.String()).AddString("columnEncodings", se.columnEncodings.String()).AddString("rowGroups", "rowGroups").AddString("dictionaryStreams", "dictionaryStreamSources").String()
}
