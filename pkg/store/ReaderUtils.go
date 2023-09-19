package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

func VerifyStreamType(column *MothColumn, actual block.Type, validTypes util.Predicate[block.Type]) {
	if !validTypes.Test(actual) {
		InvalidStreamType(column, actual)
	}
}

func InvalidStreamType(column *MothColumn, kind block.Type) {
	panic(fmt.Sprintf("Cannot read SQL type '%s' from MOTH stream '%s' of type %d with attributes %s", kind, column.GetPath(), column.GetColumnType(), column.GetAttributes()))
}

func MinNonNullValueSize(nonNullCount int32) int32 {
	return maths.MaxInt32(nonNullCount+1, 1025)
}

func UnpackByteNulls(values []byte, isNull []bool) []byte {
	il := util.Lens(isNull)
	result := make([]byte, il)
	position := 0
	for i := util.INT32_ZERO; i < il; i++ {
		result[i] = values[position]
		if !isNull[i] {
			position++
		}
	}
	return result
}

func UnpackShortNulls(values []int16, isNull []bool) []int16 {
	il := util.Lens(isNull)
	result := make([]int16, il)
	position := 0
	for i := util.INT32_ZERO; i < il; i++ {
		result[i] = values[position]
		if !isNull[i] {
			position++
		}
	}
	return result
}

func UnpackIntNulls(values []int32, isNull []bool) []int32 {
	il := util.Lens(isNull)
	result := make([]int32, il)
	position := 0
	for i := util.INT32_ZERO; i < il; i++ {
		result[i] = values[position]
		if !isNull[i] {
			position++
		}
	}
	return result
}

func UnpackLongNulls(values []int64, isNull []bool) []int64 {
	il := util.Lens(isNull)
	result := make([]int64, il)
	position := 0
	for i := util.INT32_ZERO; i < il; i++ {
		result[i] = values[position]
		if !isNull[i] {
			position++
		}
	}
	return result
}

func UnpackInt128Nulls(values []int64, isNull []bool) []int64 {
	il := util.Lens(isNull)
	result := make([]int64, il*2)
	position := 0
	outputPosition := 0
	for i := util.INT32_ZERO; i < il; i++ {
		result[outputPosition] = values[position]
		result[outputPosition+1] = values[position+1]
		if !isNull[i] {
			position += 2
		}
		outputPosition += 2
	}
	return result
}

func UnpackLengthNulls(values []int32, isNull []bool, nonNullCount int32) {
	il := util.Lens(isNull)
	nullSuppressedPosition := nonNullCount - 1
	for outputPosition := il - 1; outputPosition >= 0; outputPosition-- {
		if isNull[outputPosition] {
			values[outputPosition] = 0
		} else {
			values[outputPosition] = values[nullSuppressedPosition]
			nullSuppressedPosition--
		}
	}
}

func ConvertLengthVectorToOffsetVector(vector []int32) {
	il := util.Lens(vector)
	currentLength := vector[0]
	vector[0] = 0
	for i := int32(1); i < il; i++ {
		nextLength := vector[i]
		vector[i] = vector[i-1] + currentLength
		currentLength = nextLength
	}
}
