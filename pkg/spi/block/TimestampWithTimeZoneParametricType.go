package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TimestampWithTimeZoneParametricType struct {
	// 继承
	IParametricType
}

var TS_W_TZ_TIMESTAMP_WITH_TIME_ZONE *TimestampWithTimeZoneParametricType = NewTimestampWithTimeZoneParametricType()

func NewTimestampWithTimeZoneParametricType() *TimestampWithTimeZoneParametricType {
	return new(TimestampWithTimeZoneParametricType)
}

// @Override
func (te *TimestampWithTimeZoneParametricType) getName() string {
	return ST_TIMESTAMP_WITH_TIME_ZONE
}

// @Override Type createType(TypeManager typeManager, List<TypeParameter> parameters)
func (te *TimestampWithTimeZoneParametricType) createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type {
	if parameters.IsEmpty() {
		return TIMESTAMP_WITH_TIME_ZONE
	}
	if parameters.Size() != 1 {
		panic("Expected exactly one parameter for TIMESTAMP WITH TIME ZONE")
	}
	parameter := parameters.Get(0)
	if !parameter.IsLongLiteral() {
		panic("TIMESTAMP precision must be a number")
	}
	precision := int32(parameter.GetLongLiteral())
	if precision < 0 || precision > TIMESTAMP_WITHTIMEZONE_MAX_PRECISION {
		panic(fmt.Sprintf("Invalid TIMESTAMP precision %d", precision))
	}
	return CreateTimestampWithTimeZoneType(precision)
}

// @Override
func (te *TimestampWithTimeZoneParametricType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
