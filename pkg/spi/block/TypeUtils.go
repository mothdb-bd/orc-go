package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
)

var NULL_HASH_CODE int32 = 0

func ReadNativeValue(kind Type, block Block, position int32) basic.Object {
	goType := kind.GetGoKind()
	if block.IsNull(position) {
		return nil
	}
	if goType == reflect.Int64 {
		return kind.GetLong(block, position)
	}
	if goType == reflect.Float64 {
		return kind.GetDouble(block, position)
	}
	if goType == reflect.Bool {
		return kind.GetBoolean(block, position)
	}
	// if goType == Slice.class {
	if goType == slice.SLICE_KIND {
		return kind.GetSlice(block, position)
	}
	return kind.GetObject(block, position)
}

func WriteNativeValue(kind Type, blockBuilder BlockBuilder, value basic.Object) {
	if value == nil {
		blockBuilder.AppendNull()
	} else if kind.GetGoKind() == reflect.Bool {
		kind.WriteBoolean(blockBuilder, value.(bool))
	} else if kind.GetGoKind() == reflect.Float64 {
		kind.WriteDouble(blockBuilder, (value.(float64)))
	} else if kind.GetGoKind() == reflect.Int64 {
		kind.WriteLong(blockBuilder, (value.(int64)))
	} else if kind.GetGoKind() == slice.SLICE_KIND {
		var s *slice.Slice
		// if (value instanceof byte[]) {
		v, flag := value.([]byte)
		if flag {
			s = slice.NewWithBuf(v)
			// else if (value instanceof String) {
		} else {
			v, flag := value.(string)
			if flag {
				s, _ = slice.NewByString(v)
			} else {
				s = value.(*slice.Slice)
			}
		}
		kind.WriteSlice2(blockBuilder, s, 0, int32(s.Size()))
	} else {
		kind.WriteObject(blockBuilder, value)
	}
}

func checkElementNotNull(isNull bool, errorMsg string) {
	if isNull {
		panic(errorMsg)
	}
}
