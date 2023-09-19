package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var TimestampParametricTypeTIMESTAMP *TimestampParametricType = NewTimestampParametricType()

type TimestampParametricType struct {
	// 继承
	IParametricType
}

func NewTimestampParametricType() *TimestampParametricType {
	return new(TimestampParametricType)
}

// @Override
func (te *TimestampParametricType) getName() string {
	return ST_TIMESTAMP
}

// @Override Type createType(TypeManager typeManager, List<TypeParameter> parameters)
func (te *TimestampParametricType) createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type {
	if parameters.IsEmpty() {
		return TIMESTAMP_MILLIS
	}
	if parameters.Size() != 1 {
		panic("Expected exactly one parameter for TIMESTAMP")
	}
	parameter := parameters.Get(0)
	if !parameter.IsLongLiteral() {
		panic("TIMESTAMP precision must be a number")
	}
	precision := int32(parameter.GetLongLiteral())
	if precision < 0 || precision > TIMESTAMP_MAX_PRECISION {
		panic(fmt.Sprintf("Invalid TIMESTAMP precision %d", precision))
	}
	return CreateTimestampType(precision)
}
