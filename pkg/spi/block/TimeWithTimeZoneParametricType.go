package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var T_W_TZ_TIME_WITH_TIME_ZONE *TimeWithTimeZoneParametricType = NewTimeWithTimeZoneParametricType()

type TimeWithTimeZoneParametricType struct {
	// 继承
	IParametricType
}

func NewTimeWithTimeZoneParametricType() *TimeWithTimeZoneParametricType {
	return new(TimeWithTimeZoneParametricType)
}

// @Override
func (te *TimeWithTimeZoneParametricType) getName() string {
	return ST_TIME_WITH_TIME_ZONE
}

// @Override  Type createType(TypeManager typeManager, List<TypeParameter> parameters)
func (te *TimeWithTimeZoneParametricType) createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type {
	if parameters.IsEmpty() {
		return TTZ_TIME_WITH_TIME_ZONE
	}
	if parameters.Size() != 1 {
		panic("Expected exactly one parameter for TIME WITH TIME ZONE")
	}
	parameter := parameters.Get(0)
	if !parameter.IsLongLiteral() {
		panic("TIME WITH TIME ZONE precision must be a number")
	}
	precision := int32(parameter.GetLongLiteral())
	if precision < 0 || precision > TTZ_MAX_PRECISION {
		panic(fmt.Sprintf("Invalid TIME WITH TIME ZONE precision %d", precision))
	}
	return CreateTimeWithTimeZoneType(precision)
}
