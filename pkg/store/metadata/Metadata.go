package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Metadata struct {
	stripeStatistics *util.ArrayList[*optional.Optional[*StripeStatistics]]
}

func NewMetadata(stripeStatistics *util.ArrayList[*optional.Optional[*StripeStatistics]]) *Metadata {
	ma := new(Metadata)
	ma.stripeStatistics = stripeStatistics
	return ma
}

func (ma *Metadata) GetStripeStatsList() *util.ArrayList[*optional.Optional[*StripeStatistics]] {
	return ma.stripeStatistics
}
