package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Footer struct {
	numberOfRows   uint64
	rowsInRowGroup *optional.OptionalInt
	stripes        *util.ArrayList[*StripeInformation]
	types          *ColumnMetadata[*MothType]
	fileStats      *optional.Optional[*ColumnMetadata[*ColumnStatistics]]
	// Map<String, Slice> userMetadata;
	userMetadata map[string]*slice.Slice
	writerId     *optional.Optional[uint32]
}

func NewFooter(numberOfRows uint64, rowsInRowGroup *optional.OptionalInt, stripes *util.ArrayList[*StripeInformation], types *ColumnMetadata[*MothType], fileStats *optional.Optional[*ColumnMetadata[*ColumnStatistics]], userMetadata map[string]*slice.Slice, writerId *optional.Optional[uint32]) *Footer {
	fr := new(Footer)
	fr.numberOfRows = numberOfRows

	if rowsInRowGroup.IsPresent() && rowsInRowGroup.Get() <= 0 {
		panic("rowsInRowGroup must be at least 1")
	}
	fr.rowsInRowGroup = rowsInRowGroup
	fr.stripes = stripes
	fr.types = types
	fr.fileStats = fileStats
	fr.userMetadata = userMetadata
	fr.writerId = writerId
	return fr
}

func (fr *Footer) GetNumberOfRows() uint64 {
	return fr.numberOfRows
}

func (fr *Footer) GetRowsInRowGroup() *optional.OptionalInt {
	return fr.rowsInRowGroup
}

func (fr *Footer) GetStripes() *util.ArrayList[*StripeInformation] {
	return fr.stripes
}

func (fr *Footer) GetTypes() *ColumnMetadata[*MothType] {
	return fr.types
}

func (fr *Footer) GetFileStats() *optional.Optional[*ColumnMetadata[*ColumnStatistics]] {
	return fr.fileStats
}

func (fr *Footer) GetUserMetadata() map[string]*slice.Slice {
	// return ImmutableMap.copyOf(transformValues(userMetadata, Slices.copyOf))
	return fr.userMetadata
}

func (fr *Footer) GetWriterId() *optional.Optional[uint32] {
	return fr.writerId
}
