package store

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DEFAULT_COMPRESSION float64 = 100
	FORMAT_TAG          byte    = 0
	T_DIGEST_SIZE       int32   = util.SizeOf(&TDigest{})
	INITIAL_CAPACITY    int32   = 1
	FUDGE_FACTOR        int32   = 10
)

type TDigest struct {
	maxSize       int32
	compression   float64
	means         []float64
	weights       []float64
	centroidCount int32
	totalWeight   float64
	min           float64
	max           float64
	backwards     bool
	needsMerge    bool
	indexes       []int32
	tempMeans     []float64
	tempWeights   []float64
}

func NewTDigest() *TDigest {
	return NewTDigest2(DEFAULT_COMPRESSION)
}
func NewTDigest2(compression float64) *TDigest {
	return NewTDigest3(compression, math.Inf(1), math.Inf(-1), 0, 0, make([]float64, INITIAL_CAPACITY), make([]float64, INITIAL_CAPACITY), false, false)
}
func NewTDigest3(compression float64, min float64, max float64, totalWeight float64, centroidCount int32, means []float64, weights []float64, needsMerge bool, backwards bool) *TDigest {
	tt := new(TDigest)
	util.CheckArgument2(compression >= 10, "compression factor too small (< 10)")
	tt.compression = compression
	tt.maxSize = (6*int32(internalCompressionFactor(compression)) + FUDGE_FACTOR)
	tt.totalWeight = totalWeight
	tt.min = min
	tt.max = max
	tt.centroidCount = centroidCount
	tt.means = means
	tt.weights = weights
	tt.needsMerge = needsMerge
	tt.backwards = backwards
	return tt
}

func CopyOf(other *TDigest) *TDigest {
	return NewTDigest3(other.compression, other.min, other.max, other.totalWeight, other.centroidCount, util.CopyOfFloat64s(other.means, other.centroidCount), util.CopyOfFloat64s(other.weights, other.centroidCount), other.needsMerge, other.backwards)
}

func Deserialize(serialized *slice.Slice) *TDigest {
	input := serialized.GetInput()
	format := input.ReadByte()
	util.CheckArgument2(format == FORMAT_TAG, "Invalid format")
	min := input.ReadDouble()
	max := input.ReadDouble()
	compression := input.ReadDouble()
	totalWeight := input.ReadDouble()
	centroidCount := input.ReadInt()
	means := make([]float64, centroidCount)
	for i := util.INT32_ZERO; i < centroidCount; i++ {
		means[i] = input.ReadDouble()
	}
	weights := make([]float64, centroidCount)
	for i := util.INT32_ZERO; i < centroidCount; i++ {
		weights[i] = input.ReadDouble()
	}
	return NewTDigest3(compression, min, max, totalWeight, centroidCount, means, weights, false, false)
}

func (tt *TDigest) GetMin() float64 {
	if tt.totalWeight == 0 {
		return math.NaN()
	}
	return tt.min
}

func (tt *TDigest) GetMax() float64 {
	if tt.totalWeight == 0 {
		return math.NaN()
	}
	return tt.max
}

func (tt *TDigest) GetCount() float64 {
	return tt.totalWeight
}

func (tt *TDigest) Add(value float64) {
	tt.Add2(value, 1)
}

func (tt *TDigest) Add2(value float64, weight float64) {
	util.CheckArgument2(!math.IsNaN(value), "value is NaN")
	util.CheckArgument2(!math.IsNaN(weight), "weight is NaN")
	util.CheckArgument2(!math.IsInf(value, 1), "value must be finite")
	util.CheckArgument2(!math.IsInf(weight, 1), "weight must be finite")
	if tt.centroidCount == util.Lens(tt.means) {
		if util.Lens(tt.means) < tt.maxSize {
			tt.ensureCapacity(maths.MinInt32(maths.MaxInt32(util.Lens(tt.means)*2, INITIAL_CAPACITY), tt.maxSize))
		} else {
			tt.merge(internalCompressionFactor(tt.compression))
			if tt.centroidCount >= util.Lens(tt.means) {
				panic("Invalid size estimation for T-Digest")
			}
		}
	}
	tt.means[tt.centroidCount] = value
	tt.weights[tt.centroidCount] = weight
	tt.centroidCount++
	tt.totalWeight += weight
	tt.min = maths.MinFloat64(value, tt.min)
	tt.max = maths.MaxFloat64(value, tt.max)
	tt.needsMerge = true
}

func (tt *TDigest) MergeWith(other *TDigest) {
	if tt.centroidCount+other.centroidCount > util.Lens(tt.means) {
		tt.merge(internalCompressionFactor(tt.compression))
		other.merge(internalCompressionFactor(tt.compression))
		tt.ensureCapacity(tt.centroidCount + other.centroidCount)
	}
	util.CopyArrays(other.means, 0, tt.means, tt.centroidCount, other.centroidCount)
	util.CopyArrays(other.weights, 0, tt.weights, tt.centroidCount, other.centroidCount)
	tt.centroidCount += other.centroidCount
	tt.totalWeight += other.totalWeight
	tt.min = maths.MinFloat64(tt.min, other.min)
	tt.max = maths.MaxFloat64(tt.max, other.max)
	tt.needsMerge = true
}

func (tt *TDigest) ValueAt(quantile float64) float64 {
	return tt.ValuesAt(util.NewArrayList(quantile)).Get(0)
}

func (tt *TDigest) ValuesAt(quantiles *util.ArrayList[float64]) util.List[float64] {
	if quantiles.IsEmpty() {
		return util.NewArrayList[float64]()
	}

	if tt.centroidCount == 0 {
		return util.NCopysList(quantiles.Size(), math.NaN())
	}
	tt.mergeIfNeeded(internalCompressionFactor(tt.compression))
	if tt.centroidCount == 1 {
		return util.NCopysList(quantiles.Size(), tt.means[0])
	}
	offsets := util.MapStream(quantiles.Stream(), func(quantile float64) float64 {
		return quantile * tt.totalWeight
	}).ToList()

	valuesAtQuantiles := util.NewArrayList[float64]()
	index := 0
	for index < quantiles.Size() && offsets.Get(index) < 1 {
		valuesAtQuantiles.Add(tt.min)
		index++
	}
	for index < quantiles.Size() && offsets.Get(index) < tt.weights[0]/2 {
		valuesAtQuantiles.Add(tt.min + interpolate(offsets.Get(index), 1, tt.min, tt.weights[0]/2, tt.means[0]))
		index++
	}
	for index < quantiles.Size() && offsets.Get(index) <= tt.totalWeight-1 && tt.totalWeight-offsets.Get(index) <= tt.weights[tt.centroidCount-1]/2 && tt.weights[tt.centroidCount-1]/2 > 1 {
		valuesAtQuantiles.Add(tt.max + interpolate(tt.totalWeight-offsets.Get(index), 1, tt.max, tt.weights[tt.centroidCount-1]/2, tt.means[tt.centroidCount-1]))
		index++
	}
	if index < quantiles.Size() && offsets.Get(index) >= tt.totalWeight-1 {
		valuesAtQuantiles.AddAll(util.NCopysList(quantiles.Size()-index, tt.max))
		return valuesAtQuantiles
	}
	weightSoFar := tt.weights[0] / 2
	currentCentroid := util.INT32_ZERO
	for index < quantiles.Size() {
		delta := (tt.weights[currentCentroid] + tt.weights[currentCentroid+1]) / 2
		for currentCentroid < tt.centroidCount-1 && weightSoFar+delta <= offsets.Get(index) {
			weightSoFar += delta
			currentCentroid++
			if currentCentroid < tt.centroidCount-1 {
				delta = (tt.weights[currentCentroid] + tt.weights[currentCentroid+1]) / 2
			}
		}
		if currentCentroid == tt.centroidCount-1 {
			for index < quantiles.Size() && offsets.Get(index) <= tt.totalWeight-1 && tt.weights[tt.centroidCount-1]/2 > 1 {
				valuesAtQuantiles.Add(tt.max + interpolate(tt.totalWeight-offsets.Get(index), 1, tt.max, tt.weights[tt.centroidCount-1]/2, tt.means[tt.centroidCount-1]))
				index++
			}
			if index < quantiles.Size() {
				valuesAtQuantiles.AddAll(util.NCopysList(quantiles.Size()-index, tt.max))
				return valuesAtQuantiles
			}
		} else {
			if tt.weights[currentCentroid] == 1 && offsets.Get(index)-weightSoFar < tt.weights[currentCentroid]/2 {
				valuesAtQuantiles.Add(tt.means[currentCentroid])
			} else if tt.weights[currentCentroid+1] == 1 && offsets.Get(index)-weightSoFar >= tt.weights[currentCentroid]/2 {
				valuesAtQuantiles.Add(tt.means[currentCentroid+1])
			} else {
				interpolationOffset := offsets.Get(index) - weightSoFar
				interpolationSectionLength := delta
				if tt.weights[currentCentroid] == 1 {
					interpolationOffset -= tt.weights[currentCentroid] / 2
					interpolationSectionLength = tt.weights[currentCentroid+1] / 2
				} else if tt.weights[currentCentroid+1] == 1 {
					interpolationSectionLength = tt.weights[currentCentroid] / 2
				}
				valuesAtQuantiles.Add(tt.means[currentCentroid] + interpolate(interpolationOffset, 0, tt.means[currentCentroid], interpolationSectionLength, tt.means[currentCentroid+1]))
			}
			index++
		}
	}
	return valuesAtQuantiles
}

func (tt *TDigest) Serialize() *slice.Slice {
	tt.merge(tt.compression)
	return tt.serializeInternal()
}

func (tt *TDigest) serializeInternal() *slice.Slice {
	result, _ := slice.NewSlice() // Slices.allocate(serializedSizeInBytes())
	// output := result.
	result.WriteByte(FORMAT_TAG)
	result.WriteFloat64LE(tt.min)
	result.WriteFloat64LE(tt.max)
	result.WriteFloat64LE(tt.compression)
	result.WriteFloat64LE(tt.totalWeight)
	result.WriteInt32LE(tt.centroidCount)
	for i := util.INT32_ZERO; i < tt.centroidCount; i++ {
		result.WriteFloat64LE(tt.means[i])
	}
	for i := util.INT32_ZERO; i < tt.centroidCount; i++ {
		result.WriteFloat64LE(tt.weights[i])
	}
	return result
}

func (tt *TDigest) SerializedSizeInBytes() int32 {
	return util.BYTE_BYTES + util.FLOAT64_BYTES + util.FLOAT64_BYTES + util.FLOAT64_BYTES + util.FLOAT64_BYTES + util.INT32_BYTES + util.FLOAT64_BYTES*tt.centroidCount + util.FLOAT64_BYTES*tt.centroidCount
}

func (tt *TDigest) EstimatedInMemorySizeInBytes() int32 {
	return T_DIGEST_SIZE + util.SizeOf(tt.means) + util.SizeOf(tt.weights) + util.SizeOf(tt.tempMeans) + util.SizeOf(tt.tempWeights) + util.SizeOf(tt.indexes)
}

func (tt *TDigest) merge(compression float64) {
	if tt.centroidCount == 0 {
		return
	}
	tt.initializeIndexes()
	util.QuickSortIndirect(tt.indexes, tt.means, 0, tt.centroidCount)
	if tt.backwards {
		util.ReverseNums(tt.indexes, 0, tt.centroidCount)
	}
	centroidMean := tt.means[tt.indexes[0]]
	centroidWeight := tt.weights[tt.indexes[0]]
	if tt.tempMeans == nil {
		tt.tempMeans = make([]float64, INITIAL_CAPACITY)
		tt.tempWeights = make([]float64, INITIAL_CAPACITY)
	}
	lastCentroid := util.INT32_ZERO
	tt.tempMeans[lastCentroid] = centroidMean
	tt.tempWeights[lastCentroid] = centroidWeight
	weightSoFar := float64(0)
	normalizer := normalizer(compression, tt.totalWeight)
	currentQuantile := float64(0)
	currentQuantileMaxClusterSize := maxRelativeClusterSize(currentQuantile, normalizer)
	for i := util.INT32_ZERO; i < tt.centroidCount; i++ {
		index := tt.indexes[i]
		entryWeight := tt.weights[index]
		entryMean := tt.means[index]
		tentativeWeight := centroidWeight + entryWeight
		tentativeQuantile := maths.MinFloat64((weightSoFar+tentativeWeight)/tt.totalWeight, 1)
		maxClusterWeight := tt.totalWeight * maths.MinFloat64(currentQuantileMaxClusterSize, maxRelativeClusterSize(tentativeQuantile, normalizer))
		if tentativeWeight <= maxClusterWeight {
			centroidMean = centroidMean + (entryMean-centroidMean)*entryWeight/tentativeWeight
			centroidWeight = tentativeWeight
		} else {
			lastCentroid++
			weightSoFar += centroidWeight
			currentQuantile = weightSoFar / tt.totalWeight
			currentQuantileMaxClusterSize = maxRelativeClusterSize(currentQuantile, normalizer)
			centroidWeight = entryWeight
			centroidMean = entryMean
		}
		tt.ensureTempCapacity(lastCentroid)
		tt.tempMeans[lastCentroid] = centroidMean
		tt.tempWeights[lastCentroid] = centroidWeight
	}
	tt.centroidCount = lastCentroid + 1
	if tt.backwards {
		util.ReverseNums(tt.tempMeans, 0, tt.centroidCount)
		util.ReverseNums(tt.tempWeights, 0, tt.centroidCount)
	}
	tt.backwards = !tt.backwards
	util.CopyArrays(tt.tempMeans, 0, tt.means, 0, tt.centroidCount)
	util.CopyArrays(tt.tempWeights, 0, tt.weights, 0, tt.centroidCount)
}

// @VisibleForTesting
func (tt *TDigest) forceMerge() {
	tt.merge(internalCompressionFactor(tt.compression))
}

// @VisibleForTesting
func (tt *TDigest) getCentroidCount() int32 {
	return tt.centroidCount
}

func (tt *TDigest) mergeIfNeeded(compression float64) {
	if tt.needsMerge {
		tt.merge(compression)
	}
}

func (tt *TDigest) ensureCapacity(newSize int32) {
	if util.Lens(tt.means) < newSize {
		tt.means = util.CopyOfFloat64s(tt.means, newSize)
		tt.weights = util.CopyOfFloat64s(tt.weights, newSize)
	}
}

func (tt *TDigest) ensureTempCapacity(capacity int32) {
	if util.Lens(tt.tempMeans) <= capacity {
		newSize := capacity + int32(math.Ceil(float64(capacity)*0.5))
		tt.tempMeans = util.CopyOfFloat64s(tt.tempMeans, newSize)
		tt.tempWeights = util.CopyOfFloat64s(tt.tempWeights, newSize)
	}
}

func (tt *TDigest) initializeIndexes() {
	if tt.indexes == nil || util.Lens(tt.indexes) != util.Lens(tt.means) {
		tt.indexes = make([]int32, util.Lens(tt.means))
	}
	for i := util.INT32_ZERO; i < tt.centroidCount; i++ {
		tt.indexes[i] = i
	}
}

func interpolate(x float64, x0 float64, y0 float64, x1 float64, y1 float64) float64 {
	return (x - x0) / (x1 - x0) * (y1 - y0)
}

func maxRelativeClusterSize(quantile float64, normalizer float64) float64 {
	return quantile * (1 - quantile) / normalizer
}

func normalizer(compression float64, weight float64) float64 {
	return compression / (4*math.Log(weight/compression) + 24)
}

func internalCompressionFactor(compression float64) float64 {
	return 2 * compression
}
