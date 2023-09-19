package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/stats"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DistributionStat struct {
	oneMinute      *Distribution
	fiveMinutes    *Distribution
	fifteenMinutes *Distribution
	allTime        *Distribution
}

func NewDistributionStat() *DistributionStat {
	dt := new(DistributionStat)
	dt.oneMinute = NewDistribution2(stats.OneMinute())
	dt.fiveMinutes = NewDistribution2(stats.FiveMinutes())
	dt.fifteenMinutes = NewDistribution2(stats.FifteenMinutes())
	dt.allTime = NewDistribution()
	return dt
}

func (dt *DistributionStat) Add(value int64) {
	dt.oneMinute.Add(value)
	dt.fiveMinutes.Add(value)
	dt.fifteenMinutes.Add(value)
	dt.allTime.Add(value)
}

// @Managed
// @Nested
func (dt *DistributionStat) GetOneMinute() *Distribution {
	return dt.oneMinute
}

// @Managed
// @Nested
func (dt *DistributionStat) GetFiveMinutes() *Distribution {
	return dt.fiveMinutes
}

// @Managed
// @Nested
func (dt *DistributionStat) GetFifteenMinutes() *Distribution {
	return dt.fifteenMinutes
}

// @Managed
// @Nested
func (dt *DistributionStat) GetAllTime() *Distribution {
	return dt.allTime
}

func (dt *DistributionStat) Snapshot() *DistributionStatSnapshot {
	return NewDistributionStatSnapshot(dt.GetOneMinute().Snapshot(), dt.GetFiveMinutes().Snapshot(), dt.GetFifteenMinutes().Snapshot(), dt.GetAllTime().Snapshot())
}

func (dt *DistributionStat) String() string {
	return "DistributionStat"
}

type DistributionStatSnapshot struct {
	oneMinute     *DistributionSnapshot
	fiveMinute    *DistributionSnapshot
	fifteenMinute *DistributionSnapshot
	allTime       *DistributionSnapshot
}

func NewDistributionStatSnapshot(oneMinute *DistributionSnapshot, fiveMinute *DistributionSnapshot, fifteenMinute *DistributionSnapshot, allTime *DistributionSnapshot) *DistributionStatSnapshot {
	dt := new(DistributionStatSnapshot)
	dt.oneMinute = oneMinute
	dt.fiveMinute = fiveMinute
	dt.fifteenMinute = fifteenMinute
	dt.allTime = allTime
	return dt
}

// @JsonProperty
func (dt *DistributionStatSnapshot) GetOneMinute() *DistributionSnapshot {
	return dt.oneMinute
}

// @JsonProperty
func (dt *DistributionStatSnapshot) GetFiveMinutes() *DistributionSnapshot {
	return dt.fiveMinute
}

// @JsonProperty
func (dt *DistributionStatSnapshot) GetFifteenMinutes() *DistributionSnapshot {
	return dt.fifteenMinute
}

// @JsonProperty
func (dt *DistributionStatSnapshot) GetAllTime() *DistributionSnapshot {
	return dt.allTime
}

// @Override
func (dt *DistributionStatSnapshot) String() string {
	return util.NewSB().AddString("oneMinute", dt.oneMinute.String()).AddString("fiveMinute", dt.fiveMinute.String()).AddString("fifteenMinute", dt.fifteenMinute.String()).AddString("allTime", dt.allTime.String()).String()
}
