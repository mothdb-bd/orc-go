package metadata

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StripeFooter struct {
	streams         *util.ArrayList[*Stream]
	columnEncodings *ColumnMetadata[*ColumnEncoding]
	// timeZone        *ZoneId
	timeZone *time.Location
}

func NewStripeFooter(streams *util.ArrayList[*Stream], columnEncodings *ColumnMetadata[*ColumnEncoding], timeZone *time.Location) *StripeFooter {
	sr := new(StripeFooter)
	sr.streams = streams
	sr.columnEncodings = columnEncodings
	sr.timeZone = timeZone
	return sr
}

func (sr *StripeFooter) GetColumnEncodings() *ColumnMetadata[*ColumnEncoding] {
	return sr.columnEncodings
}

func (sr *StripeFooter) GetStreams() *util.ArrayList[*Stream] {
	return sr.streams
}

func (sr *StripeFooter) GetTimeZone() *time.Location {
	return sr.timeZone
}
