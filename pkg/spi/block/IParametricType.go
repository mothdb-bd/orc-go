package block

import "github.com/mothdb-bd/orc-go/pkg/util"

type IParametricType interface {
	getName() string
	createType(typeManager TypeManager, parameters *util.ArrayList[*TypeParameter]) Type
}
