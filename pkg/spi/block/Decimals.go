package block

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
	"github.com/shopspring/decimal"
)

var (
	DECIMAL_MAX_UNSCALED_DECIMAL                   Int128    = MustI128FromString("99999999999999999999999999999999999999")
	DECIMAL_MIN_UNSCALED_DECIMAL                   Int128    = MustI128FromString("-99999999999999999999999999999999999999")
	DECIMAL_MAX_PRECISION                          int32     = 38
	DECIMAL_MAX_SHORT_PRECISION                    int32     = 18
	DECIMAL_LONG_POWERS_OF_TEN_TABLE_LENGTH        int32     = 19
	DECIMAL_BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH int32     = 100
	DECIMAL_LONG_POWERS_OF_TEN                     []int64   = make([]int64, DECIMAL_LONG_POWERS_OF_TEN_TABLE_LENGTH)
	DECIMAL_BIG_INTEGER_POWERS_OF_TEN              []*BigInt = make([]*BigInt, DECIMAL_LONG_POWERS_OF_TEN_TABLE_LENGTH)
)

func init() {

	for i := 0; i < len(DECIMAL_LONG_POWERS_OF_TEN); i++ {
		DECIMAL_LONG_POWERS_OF_TEN[i] = int64(math.Round(math.Pow(10, float64(i))))
	}
	for i := 0; i < len(DECIMAL_BIG_INTEGER_POWERS_OF_TEN); i++ {
		DECIMAL_BIG_INTEGER_POWERS_OF_TEN[i] = TEN.Pow(i)
	}
}

func LongTenToNth(n int32) int64 {
	return DECIMAL_LONG_POWERS_OF_TEN[n]
}

func BigIntegerTenToNth(n int32) *BigInt {
	return DECIMAL_BIG_INTEGER_POWERS_OF_TEN[n]
}

func ToString(unscaledValue int64, scale int32) string {
	return toString(strconv.FormatInt(unscaledValue, 10), scale)
}

func ToString2(unscaledValue *Int128, scale int32) string {
	return toString(unscaledValue.String(), scale)
}

func ToString3(unscaledValue *BigInt, scale int32) string {
	return toString(unscaledValue.String(), scale)
}

func toString(unscaledValueString string, scale int32) string {
	resultBuilder := util.NewSB()

	var l int = len(unscaledValueString)
	var l32 = int32(l)
	if strings.HasPrefix(unscaledValueString, "-") {
		resultBuilder.AppendString("-")
		unscaledValueString = util.Substring(unscaledValueString, 1, l)
	}
	l = len(unscaledValueString)
	l32 = int32(l)

	if l32 <= scale {
		resultBuilder.AppendString("0")
	} else {
		resultBuilder.AppendString(util.Substring(unscaledValueString, 0, l-int(scale)))
	}
	if scale > 0 {
		resultBuilder.AppendString(".")
		if l32 < scale {

			resultBuilder.AppendString(strings.Repeat("0", int(scale)-l))
			resultBuilder.AppendString(unscaledValueString)
		} else {
			resultBuilder.AppendString(util.SubStrNoEnd(unscaledValueString, l-int(scale)))
		}
	}
	return resultBuilder.String()
}

func Overflows(value int64, precision int32) bool {
	if precision > DECIMAL_MAX_SHORT_PRECISION {
		panic(fmt.Sprintf("expected precision to be less than %d", DECIMAL_MAX_SHORT_PRECISION))
	}
	return maths.AbsInt64(value) >= LongTenToNth(precision)
}

func Rescale(value int64, fromScale int32, toScale int32) int64 {
	if toScale < fromScale {
		panic("target scale must be larger than source scale")
	}
	return value * LongTenToNth(toScale-fromScale)
}

func IsShortDecimal(kind Type) bool {
	_, flag := kind.(*ShortDecimalType)
	return flag
}

func IsLongDecimal(kind Type) bool {
	_, flag := kind.(*LongDecimalType)
	return flag
}

func ValueOf(value *decimal.Decimal) Int128 {
	return ValueOf2(NewBigIntString(value.String()))
}

func ValueOf2(value *BigInt) Int128 {
	result := MustI128FromString(value.String())
	ThrowIfOverflows(int64(result.hi), int64(result.lo))
	return result
}

func ThrowIfOverflows(high int64, low int64) {
	if Overflows5(high, low) {
		panic("Decimal overflow")
	}
}

func Overflows4(value Int128) bool {
	return Overflows5(int64(value.hi), int64(value.lo))
}

func Overflows5(high int64, low int64) bool {
	return Int128Compare(high, low, int64(DECIMAL_MAX_UNSCALED_DECIMAL.hi), int64(DECIMAL_MAX_UNSCALED_DECIMAL.lo)) > 0 || Int128Compare(high, low, int64(DECIMAL_MIN_UNSCALED_DECIMAL.hi), int64(DECIMAL_MIN_UNSCALED_DECIMAL.lo)) < 0
}

func Overflows6(value Int128, precision int32) bool {
	if precision > DECIMAL_MAX_PRECISION {
		panic(fmt.Sprintf("precision must be in [1, %d] range", DECIMAL_MAX_PRECISION))
	}
	if precision == DECIMAL_MAX_PRECISION {
		return Overflows5(int64(value.hi), int64(value.lo))
	}
	// return AbsExact(value).compareTo(powerOfTen(precision)) >= 0
	return true
}
