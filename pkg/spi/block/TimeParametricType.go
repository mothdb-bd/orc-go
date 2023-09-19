package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var TPT_TIME *TimeParametricType = NewTimeParametricType()

type TimeParametricType struct {
	IParametricType
}

func NewTimeParametricType() *TimeParametricType {
	return new(TimeParametricType)
}

// @Override
func (te *TimeParametricType) getName() string {
	return ST_TIME
}

// @Override Type createType(TypeManager typeManager, List<TypeParameter> parameters)
func (te *TimeParametricType) createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type {
	if parameters.IsEmpty() {
		return TIME
	}
	if parameters.Size() != 1 {
		panic("Expected exactly one parameter for TIME")
	}
	parameter := parameters.Get(0)
	if !parameter.IsLongLiteral() {
		panic("TIME precision must be a number")
	}
	precision := int32(parameter.GetLongLiteral())
	if precision < 0 || precision > TIME_MAX_PRECISION {
		panic(fmt.Sprintf("Invalid TIME precision %d", precision))
	}
	return CreateTimeType(precision)
}
