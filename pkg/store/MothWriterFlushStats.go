package store

import "github.com/mothdb-bd/orc-go/pkg/util"

type MothWriterFlushStats struct {
	name            string
	stripeBytes     *DistributionStat
	stripeRows      *DistributionStat
	dictionaryBytes *DistributionStat
}

func NewMothWriterFlushStats(name string) *MothWriterFlushStats {
	ms := new(MothWriterFlushStats)

	ms.stripeBytes = NewDistributionStat()
	ms.stripeRows = NewDistributionStat()
	ms.dictionaryBytes = NewDistributionStat()

	ms.name = name
	return ms
}

func (ms *MothWriterFlushStats) GetName() string {
	return ms.name
}

// @Managed
// @Nested
func (ms *MothWriterFlushStats) GetStripeBytes() *DistributionStat {
	return ms.stripeBytes
}

// @Managed
// @Nested
func (ms *MothWriterFlushStats) GetStripeRows() *DistributionStat {
	return ms.stripeRows
}

// @Managed
// @Nested
func (ms *MothWriterFlushStats) GetDictionaryBytes() *DistributionStat {
	return ms.dictionaryBytes
}

func (ms *MothWriterFlushStats) RecordStripeWritten(stripeBytes int64, stripeRows int32, dictionaryBytes int32) {
	ms.stripeBytes.Add(stripeBytes)
	ms.stripeRows.Add(int64(stripeRows))
	ms.dictionaryBytes.Add(int64(dictionaryBytes))
}

// @Override
func (ms *MothWriterFlushStats) String() string {
	return util.NewSB().AddString("name", ms.name).AddString("stripeBytes", ms.stripeBytes.String()).AddString("stripeRows", ms.stripeRows.String()).AddString("dictionaryBytes", ms.dictionaryBytes.String()).String()
}
