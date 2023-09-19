package store

import (
	"sync"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Distribution struct { //@GuardedBy("this")
	digest *DecayTDigest
	total  *DecayCounter
	locker *sync.Mutex
}

func NewDistribution() *Distribution {
	return NewDistribution2(0)
}
func NewDistribution2(alpha float64) *Distribution {
	return NewDistribution3(NewDecayTDigest(DEFAULT_COMPRESSION, alpha), NewDecayCounter(alpha))
}
func NewDistribution3(digest *DecayTDigest, total *DecayCounter) *Distribution {
	dn := new(Distribution)
	dn.digest = digest
	dn.total = total
	dn.locker = new(sync.Mutex)
	return dn
}

func (dn *Distribution) Add(value int64) {
	dn.digest.Add(float64(value))
	dn.total.Add(value)
}

func (dn *Distribution) Add2(value int64, count int64) {
	dn.digest.Add2(float64(value), float64(count))
	dn.total.Add(value * count)
}

func (dn *Distribution) Duplicate() *Distribution {
	return NewDistribution3(dn.digest.Duplicate(), dn.total.Duplicate())
}

// @Managed
func (dn *Distribution) GetCount() float64 {
	return dn.digest.GetCount()
}

// @Managed
func (dn *Distribution) GetTotal() float64 {
	return dn.total.GetCount()
}

// @Managed
func (dn *Distribution) GetP01() float64 {
	return dn.digest.ValueAt(0.01)
}

// @Managed
func (dn *Distribution) GetP05() float64 {
	return dn.digest.ValueAt(0.05)
}

// @Managed
func (dn *Distribution) GetP10() float64 {
	return dn.digest.ValueAt(0.10)
}

// @Managed
func (dn *Distribution) GetP25() float64 {
	return dn.digest.ValueAt(0.25)
}

// @Managed
func (dn *Distribution) GetP50() float64 {
	return dn.digest.ValueAt(0.5)
}

// @Managed
func (dn *Distribution) GetP75() float64 {
	return dn.digest.ValueAt(0.75)
}

// @Managed
func (dn *Distribution) GetP90() float64 {
	return dn.digest.ValueAt(0.90)
}

// @Managed
func (dn *Distribution) GetP95() float64 {
	return dn.digest.ValueAt(0.95)
}

// @Managed
func (dn *Distribution) GetP99() float64 {
	return dn.digest.ValueAt(0.99)
}

// @Managed
func (dn *Distribution) GetMin() float64 {
	return dn.digest.GetMin()
}

// @Managed
func (dn *Distribution) GetMax() float64 {
	return dn.digest.GetMax()
}

// @Managed
func (dn *Distribution) GetAvg() float64 {
	return dn.GetTotal() / dn.GetCount()
}

// @Managed
func (dn *Distribution) GetPercentiles() map[float64]float64 {
	percentiles := util.NewArrayList[float64](100)
	for i := float64(0); i < 100; i++ {
		percentiles.Add(i / 100.0)
	}
	var values util.List[float64]
	dn.locker.Lock() // synchronized (this) {
	values = dn.digest.ValuesAt(percentiles)
	dn.locker.Unlock()

	result := make(map[float64]float64)
	for i := 0; i < percentiles.Size(); i++ {
		result[percentiles.Get(i)] = values.Get(i)
	}
	return result
}

func (dn *Distribution) GetPercentiles2(percentiles *util.ArrayList[float64]) util.List[float64] {
	return dn.digest.ValuesAt(percentiles)
}

func (dn *Distribution) Snapshot() *DistributionSnapshot {
	quantiles := dn.digest.ValuesAt(util.NewArrayList(0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99))
	return NewDistributionSnapshot(dn.GetCount(), dn.GetTotal(), quantiles.Get(0), quantiles.Get(1), quantiles.Get(2), quantiles.Get(3), quantiles.Get(4), quantiles.Get(5), quantiles.Get(6), quantiles.Get(7), quantiles.Get(8), dn.GetMin(), dn.GetMax(), dn.GetAvg())
}

type DistributionSnapshot struct {
	count float64
	total float64
	p01   float64
	p05   float64
	p10   float64
	p25   float64
	p50   float64
	p75   float64
	p90   float64
	p95   float64
	p99   float64
	min   float64
	max   float64
	avg   float64
}

func NewDistributionSnapshot(count float64, total float64, p01 float64, p05 float64, p10 float64, p25 float64, p50 float64, p75 float64, p90 float64, p95 float64, p99 float64, min float64, max float64, avg float64) *DistributionSnapshot {
	dt := new(DistributionSnapshot)
	dt.count = count
	dt.total = total
	dt.p01 = p01
	dt.p05 = p05
	dt.p10 = p10
	dt.p25 = p25
	dt.p50 = p50
	dt.p75 = p75
	dt.p90 = p90
	dt.p95 = p95
	dt.p99 = p99
	dt.min = min
	dt.max = max
	dt.avg = avg
	return dt
}

// @JsonProperty
func (dt *DistributionSnapshot) GetCount() float64 {
	return dt.count
}

// @JsonProperty
func (dt *DistributionSnapshot) GetTotal() float64 {
	return dt.total
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP01() float64 {
	return dt.p01
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP05() float64 {
	return dt.p05
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP10() float64 {
	return dt.p10
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP25() float64 {
	return dt.p25
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP50() float64 {
	return dt.p50
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP75() float64 {
	return dt.p75
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP90() float64 {
	return dt.p90
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP95() float64 {
	return dt.p95
}

// @JsonProperty
func (dt *DistributionSnapshot) GetP99() float64 {
	return dt.p99
}

// @JsonProperty
func (dt *DistributionSnapshot) GetMin() float64 {
	return dt.min
}

// @JsonProperty
func (dt *DistributionSnapshot) GetMax() float64 {
	return dt.max
}

// @JsonProperty
func (dt *DistributionSnapshot) GetAvg() float64 {
	return dt.avg
}

// @Override
func (dt *DistributionSnapshot) String() string {
	return util.NewSB().AddFloat64("count", dt.count).AddFloat64("total", dt.total).AddFloat64("p01", dt.p01).AddFloat64("p05", dt.p05).AddFloat64("p10", dt.p10).AddFloat64("p25", dt.p25).AddFloat64("p50", dt.p50).AddFloat64("p75", dt.p75).AddFloat64("p90", dt.p90).AddFloat64("p95", dt.p95).AddFloat64("p99", dt.p99).AddFloat64("min", dt.min).AddFloat64("max", dt.max).AddFloat64("avg", dt.avg).String()
}
