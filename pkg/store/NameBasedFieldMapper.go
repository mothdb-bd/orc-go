package store

import (
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type NameBasedFieldMapper struct {
	// 继承
	FieldMapper

	nestedColumns map[string]*MothColumn
}

func NewNameBasedFieldMapper(nestedColumns map[string]*MothColumn) *NameBasedFieldMapper {
	nr := new(NameBasedFieldMapper)
	nr.nestedColumns = nestedColumns
	return nr
}

// @Override
func (nr *NameBasedFieldMapper) Get(fieldName string) *MothColumn {
	return nr.nestedColumns[fieldName]
}

func Create(column *MothColumn) FieldMapper {
	nestedColumns := util.UniqueIndex(column.GetNestedColumns(), func(field *MothColumn) string {
		return strings.ToLower(field.GetColumnName())
	})
	return NewNameBasedFieldMapper(nestedColumns)
}

type FieldMapperFactoryImpl struct {
	// 继承
	FieldMapperFactory
}

func NewFieldMapperFactory() FieldMapperFactory {
	return new(FieldMapperFactoryImpl)
}

func (f *FieldMapperFactoryImpl) Create(column *MothColumn) FieldMapper {
	return Create(column)
}
