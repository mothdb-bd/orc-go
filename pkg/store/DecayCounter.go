package store

import (
	"fmt"
	"math"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var RESCALE_THRESHOLD_SECONDS int64 = 50

type DecayCounter struct {
	alpha             float64
	ticker            Ticker
	landmarkInSeconds int64
	count             float64
}

func NewDecayCounter(alpha float64) *DecayCounter {
	return NewDecayCounter2(alpha, GetSystemTicker())
}
func NewDecayCounter2(alpha float64, ticker Ticker) *DecayCounter {

	return NewDecayCounter3(0, alpha, ticker, int64(time.Nanosecond.Seconds()*float64(ticker.Read())))
}
func NewDecayCounter3(count float64, alpha float64, ticker Ticker, landmarkInSeconds int64) *DecayCounter {
	dr := new(DecayCounter)
	dr.count = count
	dr.alpha = alpha
	dr.ticker = ticker
	dr.landmarkInSeconds = landmarkInSeconds
	return dr
}

func (dr *DecayCounter) Duplicate() *DecayCounter {
	return NewDecayCounter3(dr.count, dr.alpha, dr.ticker, dr.landmarkInSeconds)
}

func (dr *DecayCounter) Add(value int64) {
	nowInSeconds := dr.getTickInSeconds()
	if nowInSeconds-dr.landmarkInSeconds >= RESCALE_THRESHOLD_SECONDS {
		dr.rescaleToNewLandmark(nowInSeconds)
	}
	dr.count += float64(value) * weight(dr.alpha, nowInSeconds, dr.landmarkInSeconds)
}

/**
* Compute the forward-decay multiplier (inverse of the decay factor)
 */
func weight(alpha float64, now int64, landmark int64) float64 {
	return math.Exp(alpha * float64(now-landmark))
}

func (dr *DecayCounter) Merge(decayCounter *DecayCounter) {
	util.CheckArgument2(decayCounter.alpha == dr.alpha, fmt.Sprintf("Expected decayCounter to have alpha %f, but was %f", dr.alpha, decayCounter.alpha))
	if dr.landmarkInSeconds < decayCounter.landmarkInSeconds {
		dr.rescaleToNewLandmark(decayCounter.landmarkInSeconds)
		dr.count += decayCounter.count
	} else {
		otherRescaledCount := decayCounter.count / weight(dr.alpha, dr.landmarkInSeconds, decayCounter.landmarkInSeconds)
		dr.count += otherRescaledCount
	}
}

func (dr *DecayCounter) rescaleToNewLandmark(newLandMarkInSeconds int64) {
	dr.count = dr.count / weight(dr.alpha, newLandMarkInSeconds, dr.landmarkInSeconds)
	dr.landmarkInSeconds = newLandMarkInSeconds
}

// @Managed
func (dr *DecayCounter) Reset() {
	dr.landmarkInSeconds = dr.getTickInSeconds()
	dr.count = 0
}

// @Deprecated
func (dr *DecayCounter) ResetTo(counter *DecayCounter) {
	dr.landmarkInSeconds = counter.landmarkInSeconds
	dr.count = counter.count
}

// @Managed
func (dr *DecayCounter) GetCount() float64 {
	nowInSeconds := dr.getTickInSeconds()
	return dr.count / weight(dr.alpha, nowInSeconds, dr.landmarkInSeconds)
}

// @Managed
func (dr *DecayCounter) GetRate() float64 {
	return dr.GetCount() * dr.alpha
}

func (dr *DecayCounter) getTickInSeconds() int64 {
	return int64(time.Nanosecond.Seconds() * float64(dr.ticker.Read()))
}

func (dr *DecayCounter) Snapshot() *DecayCounterSnapshot {
	return NewDecayCounterSnapshot(dr.GetCount(), dr.GetRate())
}

// @Override
func (dr *DecayCounter) String() string {
	return util.NewSB().AddFloat64("count", dr.GetCount()).AddFloat64("rate", dr.GetRate()).String()
}

func (dr *DecayCounter) GetAlpha() float64 {
	return dr.alpha
}

type DecayCounterSnapshot struct {
	count float64
	rate  float64
}

func NewDecayCounterSnapshot(count float64, rate float64) *DecayCounterSnapshot {
	dt := new(DecayCounterSnapshot)
	dt.count = count
	dt.rate = rate
	return dt
}

// @JsonProperty
func (dt *DecayCounterSnapshot) GetCount() float64 {
	return dt.count
}

// @JsonProperty
func (dt *DecayCounterSnapshot) GetRate() float64 {
	return dt.rate
}

// @Override
func (dt *DecayCounterSnapshot) String() string {
	return util.NewSB().AddFloat64("count", dt.count).AddFloat64("rate", dt.rate).String()
}
