package block

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BlockBuilderStatus struct {
	pageBuilderStatus *PageBuilderStatus
	currentSize       int32
}

var BBSINSTANCE_SIZE int32 = deepInstanceSize(reflect.TypeOf(&BlockBuilderStatus{}), &BlockBuilderStatus{})

func NewBlockBuilderStatus(pageBuilderStatus *PageBuilderStatus) *BlockBuilderStatus {
	bs := new(BlockBuilderStatus)
	bs.pageBuilderStatus = pageBuilderStatus
	return bs
}

func (bs *BlockBuilderStatus) GetMaxPageSizeInBytes() int32 {
	return bs.pageBuilderStatus.GetMaxPageSizeInBytes()
}

func (bs *BlockBuilderStatus) AddBytes(bytes int32) {
	bs.currentSize += bytes
	bs.pageBuilderStatus.addBytes(bytes)
}

// @Override
func (bs *BlockBuilderStatus) ToString() string {
	b := &strings.Builder{}
	b.WriteString("BlockBuilderStatus{")
	b.WriteString(", currentSize=")
	b.WriteString(strconv.FormatInt(int64(bs.currentSize), 10))
	b.WriteString("}")
	return b.String()
}

func deepInstanceSize(kind reflect.Type, value interface{}) int32 {

	if kind.Kind() == reflect.Array {
		panic(fmt.Sprintf("Cannot determine size of %s because it contains an array", kind))
	}
	if kind.Kind() == reflect.Interface {
		panic(fmt.Sprintf("%s is an interface", kind))
	}

	size := util.SizeOf(value)
	// for _, field := range kind.getDeclaredFields() {
	// 	if !field.getType().isPrimitive() {
	// 		size += deepInstanceSize(field.getType())
	// 	}
	// }
	return size
}
