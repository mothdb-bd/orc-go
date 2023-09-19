package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MothColumn struct {
	path             string
	columnId         metadata.MothColumnId
	columnType       metadata.MothTypeKind
	columnName       string
	mothDataSourceId *common.MothDataSourceId
	// private final *util.ArrayList[ ]<MothColumn> nestedColumns;
	nestedColumns *util.ArrayList[*MothColumn]
	// private final Map<String, String> attributes;
	attributes map[string]string
}

func NewMothColumn(path string, columnId metadata.MothColumnId, columnName string, columnType metadata.MothTypeKind, mothDataSourceId *common.MothDataSourceId, nestedColumns *util.ArrayList[*MothColumn], attributes map[string]string) *MothColumn {
	mn := new(MothColumn)
	mn.path = path
	mn.columnId = columnId
	mn.columnName = columnName
	mn.columnType = columnType
	mn.mothDataSourceId = mothDataSourceId
	mn.nestedColumns = nestedColumns
	mn.attributes = attributes
	return mn
}

func (mn *MothColumn) GetPath() string {
	return mn.path
}

func (mn *MothColumn) GetColumnId() metadata.MothColumnId {
	return mn.columnId
}

func (mn *MothColumn) GetColumnType() metadata.MothTypeKind {
	return mn.columnType
}

func (mn *MothColumn) GetColumnName() string {
	return mn.columnName
}

func (mn *MothColumn) GetMothDataSourceId() *common.MothDataSourceId {
	return mn.mothDataSourceId
}

func (mn *MothColumn) GetNestedColumns() *util.ArrayList[*MothColumn] {
	return mn.nestedColumns
}

func (mn *MothColumn) GetAttributes() map[string]string {
	return mn.attributes
}

// @Override
func (mn *MothColumn) String() string {
	return util.NewSB().AddString("path", mn.path).AddString("columnId", mn.columnId.String()).AddInt8("streamType", int8(mn.columnType)).AddString("dataSource", mn.mothDataSourceId.String()).String()
}
