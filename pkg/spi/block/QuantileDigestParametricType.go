package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var QuantileDigestParametricTypeQDIGEST *QuantileDigestParametricType = &QuantileDigestParametricType{}

type QuantileDigestParametricType struct {
	IParametricType
}

// @Override
func (qe *QuantileDigestParametricType) getName() string {
	return ST_QDIGEST
}

// @Override
func (qe *QuantileDigestParametricType) createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type {
	qdcheckArgument(parameters.Size() == 1, "QDIGEST type expects exactly one type as a parameter, got %s", parameters)
	qdcheckArgument(parameters.Get(0).GetKind() == PK_TYPE, "QDIGEST expects type as a parameter, got %s", parameters)
	return NewQuantileDigestType(parameters.Get(0).GetType())
}

func qdcheckArgument(argument bool, format string, args ...basic.Object) {
	if !argument {
		panic(fmt.Sprintf(format, args))
	}
}
