package store

import (
	"math"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var ( //@VisibleForTesting
	DECAYTDIGEST_RESCALE_THRESHOLD_SECONDS int64   = 50 //@VisibleForTesting
	ZERO_WEIGHT_THRESHOLD                  float64 = 1e-5
	SCALE_FACTOR                           float64 = 1 / ZERO_WEIGHT_THRESHOLD
)

type DecayTDigest struct {
	digest            *TDigest
	ticker            Ticker
	alpha             float64
	landmarkInSeconds int64
}

func NewDecayTDigest(compression float64, alpha float64) *DecayTDigest {
	return NewDecayTDigest3(NewTDigest2(compression), alpha, util.Ternary(alpha == 0.0, noOpTicker(), GetSystemTicker()))
}
func NewDecayTDigest2(compression float64, alpha float64, ticker Ticker) *DecayTDigest {
	return NewDecayTDigest3(NewTDigest2(compression), alpha, ticker)
}
func NewDecayTDigest3(digest *TDigest, alpha float64, ticker Ticker) *DecayTDigest {
	return NewDecayTDigest4(digest, alpha, ticker, int64(time.Nanosecond.Seconds())*ticker.Read())
}
func NewDecayTDigest4(digest *TDigest, alpha float64, ticker Ticker, landmarkInSeconds int64) *DecayTDigest {
	dt := new(DecayTDigest)
	dt.digest = digest
	dt.alpha = alpha
	dt.ticker = ticker
	dt.landmarkInSeconds = landmarkInSeconds
	return dt
}

func (dt *DecayTDigest) GetMin() float64 {
	if dt.GetCount() < ZERO_WEIGHT_THRESHOLD {
		return math.NaN()
	}
	return dt.digest.GetMin()
}

func (dt *DecayTDigest) GetMax() float64 {
	if dt.GetCount() < ZERO_WEIGHT_THRESHOLD {
		return math.NaN()
	}
	return dt.digest.GetMax()
}

func (dt *DecayTDigest) GetCount() float64 {
	dt.rescaleIfNeeded()
	result := dt.digest.GetCount()
	if dt.alpha > 0.0 {
		result /= (weight(dt.alpha, dt.nowInSeconds(), dt.landmarkInSeconds) * SCALE_FACTOR)
	}
	if result < ZERO_WEIGHT_THRESHOLD {
		result = 0
	}
	return result
}

func (dt *DecayTDigest) Add(value float64) {
	dt.Add2(value, 1)
}

func (dt *DecayTDigest) Add2(value float64, wt float64) {
	dt.rescaleIfNeeded()
	if dt.alpha > 0.0 {
		wt *= weight(dt.alpha, dt.nowInSeconds(), dt.landmarkInSeconds) * SCALE_FACTOR
	}
	dt.digest.Add2(value, wt)
}

func (dt *DecayTDigest) rescaleIfNeeded() {
	if dt.alpha > 0.0 {
		nowInSeconds := dt.nowInSeconds()
		if nowInSeconds-dt.landmarkInSeconds >= DECAYTDIGEST_RESCALE_THRESHOLD_SECONDS {
			dt.rescale(nowInSeconds)
		}
	}
}

func (dt *DecayTDigest) ValueAt(quantile float64) float64 {
	return dt.digest.ValueAt(quantile)
}

func (dt *DecayTDigest) ValuesAt(quantiles *util.ArrayList[float64]) util.List[float64] {
	return dt.digest.ValuesAt(quantiles)
}

func (dt *DecayTDigest) rescale(newLandmarkInSeconds int64) {
	factor := weight(dt.alpha, newLandmarkInSeconds, dt.landmarkInSeconds)
	dt.digest.totalWeight /= factor
	min := math.Inf(1)  // Double.POSITIVE_INFINITY
	max := math.Inf(-1) // Double.NEGATIVE_INFINITY
	index := 0
	for i := util.INT32_ZERO; i < dt.digest.centroidCount; i++ {
		weight := dt.digest.weights[i] / factor
		if weight < 1 {
			continue
		}
		dt.digest.weights[index] = weight
		dt.digest.means[index] = dt.digest.means[i]
		index++
		min = maths.MinFloat64(min, dt.digest.means[i]) // Math.min(min, dt.digest.means[i])
		max = maths.MaxFloat64(max, dt.digest.means[i])
	}
	dt.digest.centroidCount = int32(index)
	dt.digest.min = min
	dt.digest.max = max
	dt.landmarkInSeconds = newLandmarkInSeconds
}

func (dt *DecayTDigest) nowInSeconds() int64 {
	return int64(time.Nanosecond.Seconds()) * dt.ticker.Read()
}

func noOpTicker() Ticker {
	return GetNoicker()
}

func (dt *DecayTDigest) Duplicate() *DecayTDigest {
	return NewDecayTDigest4(CopyOf(dt.digest), dt.alpha, dt.ticker, dt.landmarkInSeconds)
}
