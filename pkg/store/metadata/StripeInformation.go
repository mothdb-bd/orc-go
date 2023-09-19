package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StripeInformation struct {
	numberOfRows int32
	offset       uint64
	indexLength  uint64
	dataLength   uint64
	footerLength uint64
}

func NewStripeInformation(numberOfRows int32, offset uint64, indexLength uint64, dataLength uint64, footerLength uint64) *StripeInformation {
	sn := new(StripeInformation)
	sn.numberOfRows = numberOfRows
	sn.offset = offset
	sn.indexLength = indexLength
	sn.dataLength = dataLength
	sn.footerLength = footerLength
	return sn
}

func (sn *StripeInformation) GetNumberOfRows() int32 {
	return sn.numberOfRows
}

func (sn *StripeInformation) GetOffset() uint64 {
	return sn.offset
}

func (sn *StripeInformation) GetIndexLength() uint64 {
	return sn.indexLength
}

func (sn *StripeInformation) GetDataLength() uint64 {
	return sn.dataLength
}

func (sn *StripeInformation) GetFooterLength() uint64 {
	return sn.footerLength
}

func (sn *StripeInformation) GetTotalLength() uint64 {
	return sn.indexLength + sn.dataLength + sn.footerLength
}

// @Override
func (sn *StripeInformation) String() string {
	sb := util.NewSB().AddInt32("numberOfRows", sn.numberOfRows).AddUInt64("offset", sn.offset).AddUInt64("indexLength", sn.indexLength)
	return sb.AddUInt64("dataLength", sn.dataLength).AddUInt64("footerLength", sn.footerLength).ToStringHelper()
}
