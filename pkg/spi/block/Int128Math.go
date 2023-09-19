package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	ALL_BITS_SET_64      uint64   = 0xFFFFFFFFFFFFFFFF
	INT128_POWERS_OF_TEN []Int128 = make([]Int128, 39)

	/**
	 * 10^18 fits in 2^63.
	 */
	MAX_POWER_OF_TEN_LONG int32 = 18

	MAX_POWER_OF_TEN_INT int32 = 9

	POWERS_OF_FIVES_INT []int32 = make([]int32, MAX_POWER_OF_FIVE_INT+1)

	POWERS_OF_TEN_INT []int32 = make([]int32, MAX_POWER_OF_TEN_INT+1)

	/**
	 * 5^13 fits in 2^31.
	 */
	MAX_POWER_OF_FIVE_INT int32 = 13

	LOW_32_BITS int64 = 0xFFFFFFFF
)

func init() {
	for i := util.INT32_ZERO; i < util.Lens(POWERS_OF_TEN); i += 1 {
		INT128_POWERS_OF_TEN[i] = MustI128FromString(TEN.Pow(i).StringNum())
	}

	POWERS_OF_TEN_INT[0] = 1
	for i := 1; i < len(POWERS_OF_TEN_INT); AddNum(&i) {
		POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i-1] * 10
	}

	POWERS_OF_FIVES_INT[0] = 1
	for i := 1; i < len(POWERS_OF_FIVES_INT); AddNum(&i) {
		POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i-1] * 5
	}
}

func AddNum(i *int) {
	(*i)++
}

func Int128Rescale(high int64, low int64, factor int32, result []int64, offset int32) {
	if factor == 0 {
		result[offset] = high
		result[offset+1] = low
	} else if factor > 0 {
		shiftLeftBy10(high, low, factor, result, offset)
	} else {
		scaleDownRoundUp(high, low, -factor, result, offset)
	}
}

func shiftLeftBy10(high int64, low int64, rescaleFactor int32, result []int64, offset int32) {
	if rescaleFactor >= util.Lens(INT128_POWERS_OF_TEN) {
		panic("overflow")
	}

	negative := high < 0

	if negative {
		tmpHigh := negateHighExact(high, low)
		tmpLow := negateLowExact(high, low)

		high = tmpHigh
		low = tmpLow
	}

	multiplyPositives(high, low, int64(INT128_POWERS_OF_TEN[rescaleFactor].hi), int64(INT128_POWERS_OF_TEN[rescaleFactor].lo), result, offset)

	if negative {
		tmpHigh := negateHighExact(result[offset], result[offset+1])
		tmpLow := negateLowExact(result[offset], result[offset+1])

		result[offset] = tmpHigh
		result[offset+1] = tmpLow
	}
}

func multiplyPositives(leftHigh int64, leftLow int64, rightHigh int64, rightLow int64, result []int64, offset int32) {

	z1High := unsignedMultiplyHigh(leftLow, rightLow)
	z1Low := leftLow * rightLow
	z2Low := leftLow * rightHigh
	z3Low := leftHigh * rightLow

	resultLow := z1Low
	resultHigh := z1High + z2Low + z3Low

	if (leftHigh != 0 && rightHigh != 0) ||
		resultHigh < 0 || z1High < 0 || z2Low < 0 || z3Low < 0 ||
		unsignedMultiplyHigh(leftLow, rightHigh) != 0 ||
		unsignedMultiplyHigh(leftHigh, rightLow) != 0 {
		panic(" overflowException()")
	}

	result[offset] = resultHigh
	result[offset+1] = resultLow
}

func unsignedMultiplyHigh(x, y int64) int64 {
	// From Hacker's Delight 2nd Ed. 8-3: High-Order Product Signed from/to Unsigned
	result := maths.MultiplyHigh(x, y)
	result += (y & (x >> 63)) // equivalent to: if (x < 0) result += y;
	result += (x & (y >> 63)) // equivalent to: if (y < 0) result += x;
	return result
}

func negateHighExact(high int64, low int64) int64 {

	return negateHigh(high, low)
}

func negateHigh(high int64, low int64) int64 {
	return -high - util.Ternary(low != 0, 1, util.INT64_ZERO)
}

func negateLow(unusedHigh int64, low int64) int64 {
	return -low
}

func negateLowExact(high int64, low int64) int64 {
	return negateLow(high, low)
}

func scaleDownRoundUp(high int64, low int64, scaleFactor int32, result []int64, offset int32) {
	// optimized path for smaller values
	if scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0 {
		divisor := LongTenToNth(scaleFactor)
		newLow := low / divisor
		if low%divisor >= (divisor >> 1) {
			newLow++
		}
		result[offset] = 0
		result[offset+1] = newLow
		return
	}

	scaleDown(high, low, scaleFactor, result, offset, true)
}

func scaleDown(high int64, low int64, scaleFactor int32, result []int64, offset int32, roundUp bool) {
	negative := high < 0
	if negative {
		tmpLow := negateLowExact(high, low)
		tmpHigh := negateHighExact(high, low)

		low = tmpLow
		high = tmpHigh
	}

	// Scales down for 10**rescaleFactor.
	// Because divide by int has limited divisor, we choose code path with the least amount of divisions
	if (scaleFactor-1)/MAX_POWER_OF_FIVE_INT < (scaleFactor-1)/MAX_POWER_OF_TEN_INT {
		// scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
		scaleDownFive(high, low, scaleFactor, result, offset)
		shiftRight(result[offset], result[offset+1], scaleFactor, roundUp, result, offset)
	} else {
		scaleDownTen(high, low, scaleFactor, result, offset, roundUp)
	}

	if negative {
		// negateExact not needed since all positive values can be negated without overflow
		negate(result, offset)
	}
}

func negate(value []int64, offset int32) {
	high := value[offset]
	low := value[offset+1]

	value[offset] = negateHigh(high, low)
	value[offset+1] = negateLow(high, low)
}

func scaleDownFive(high int64, low int64, fiveScale int32, result []int64, offset int32) {
	if high < 0 {
		panic("Value must be positive")
	}

	for {
		powerFive := maths.MinInt32(fiveScale, MAX_POWER_OF_FIVE_INT)
		fiveScale -= powerFive

		divisor := POWERS_OF_FIVES_INT[powerFive]
		dividePositives(high, low, int64(divisor), result, offset)

		if fiveScale == 0 {
			return
		}

		high = result[offset]
		low = result[offset+1]
	}
}

func dividePositives(dividendHigh int64, dividendLow int64, divisor int64, result []int64, offset int32) int32 {
	remainder := dividendHigh
	hi := remainder / divisor
	remainder %= divisor

	remainder = high(dividendLow) + (remainder << 32)
	z1 := int32(remainder / divisor)
	remainder %= divisor

	remainder = low(dividendLow) + (remainder << 32)
	z0 := int32(remainder / divisor)

	low := (int64(z1) << 32) | (int64(z0) & 0xFFFFFFFF)

	result[offset] = hi
	result[offset+1] = low

	return int32(remainder % divisor)
}

func high(value int64) int64 {
	return maths.UnsignedRightShift(value, 32)
}

func low(value int64) int64 {
	return value & LOW_32_BITS
}

/**
 * Scale down the value for 10**tenScale (this := this / 5**tenScale). This
 * method rounds-up, eg 44/10=4, 44/10=5.
 */
func scaleDownTen(high int64, low int64, tenScale int32, result []int64, offset int32, roundUp bool) {
	if high < 0 {
		panic("Value must be positive")
	}

	var needsRounding bool
	powerTen := maths.MinInt32(tenScale, MAX_POWER_OF_TEN_INT)
	tenScale -= powerTen
	for tenScale > 0 {

		divisor := POWERS_OF_TEN_INT[powerTen]
		needsRounding = divideCheckRound(high, low, divisor, result, offset)

		high = result[offset]
		low = result[offset+1]

		powerTen := maths.MinInt32(tenScale, MAX_POWER_OF_TEN_INT)
		tenScale -= powerTen
	}

	if roundUp && needsRounding {
		incrementUnsafe(result, offset)
	}
}

func incrementUnsafe(value []int64, offset int32) {
	high := value[offset]
	low := value[offset+1]

	value[offset] = incrementHigh(high, low)
	value[offset+1] = incrementLow(high, low)
}

func divideCheckRound(dividendHigh int64, dividendLow int64, divisor int32, result []int64, offset int32) bool {
	remainder := dividePositives(dividendHigh, dividendLow, int64(divisor), result, offset)
	return (remainder >= (divisor >> 1))
}

func shiftRight(high int64, low int64, shift int32, roundUp bool, result []int64, offset int32) {
	if high < 0 {
		panic("Value must be positive")
	}

	if shift == 0 {
		return
	}

	var needsRounding bool
	if shift < 64 {
		needsRounding = roundUp && (low&(1<<(shift-1))) != 0

		low = (high << 1 << (63 - shift)) | int64((uint64(low) >> shift))
		high = high >> shift
	} else {
		needsRounding = roundUp && (high&(1<<(shift-64-1))) != 0

		low = high >> (shift - 64)
		high = 0
	}

	if needsRounding {
		tmpHigh := incrementHigh(high, low)
		tmpLow := incrementLow(high, low)

		high = tmpHigh
		low = tmpLow
	}

	result[offset] = high
	result[offset+1] = low
}

func incrementHigh(high int64, low int64) int64 {
	return high + util.Ternary(uint64(low) == ALL_BITS_SET_64, 1, util.INT64_ZERO)
}

func incrementLow(high int64, low int64) int64 {
	return low + 1
}
