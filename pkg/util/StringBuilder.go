package util

import (
	"strconv"
	"strings"
)

type StringBuilder struct {
	sb *strings.Builder
}

func NewSB() *StringBuilder {
	return &StringBuilder{
		sb: &strings.Builder{},
	}
}

func (b *StringBuilder) AppendChar(i byte) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddInt(key string, value int) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendInt(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendInt(i int) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddInt8(key string, value int8) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendInt8(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendInt8(i int8) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddInt16(key string, value int16) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendInt16(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendInt16(i int16) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddInt32(key string, value int32) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendInt32(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendInt32(i int32) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AppendUInt32(i uint32) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddInt64(key string, value int64) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendInt64(value).AppendString(",")
	return b
}

func (b *StringBuilder) AddUInt64(key string, value uint64) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendUInt64(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendInt64(i int64) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AppendUInt64(i uint64) *StringBuilder {
	b.sb.WriteString(strconv.FormatInt(int64(i), 10))
	return b
}

func (b *StringBuilder) AddFloat32(key string, value float32) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendFloat32(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendFloat32(i float32) *StringBuilder {
	b.sb.WriteString(strconv.FormatFloat(float64(i), 'E', -1, 32))
	return b
}

func (b *StringBuilder) AddFloat64(key string, value float64) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendFloat64(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendFloat64(i float64) *StringBuilder {
	b.sb.WriteString(strconv.FormatFloat(float64(i), 'E', -1, 64))
	return b
}

func (b *StringBuilder) AddString(key string, value string) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendString(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendString(i string) *StringBuilder {
	b.sb.WriteString(i)
	return b
}

func (b *StringBuilder) AddBool(key string, value bool) *StringBuilder {
	b.AppendString(key).AppendString(":").AppendBool(value).AppendString(",")
	return b
}

func (b *StringBuilder) AppendBool(i bool) *StringBuilder {
	b.sb.WriteString(strconv.FormatBool(i))
	return b
}

func (b *StringBuilder) Length() int {
	return b.sb.Len()
}

func (b *StringBuilder) SetLength(len int) *StringBuilder {
	if len < 0 {
		panic("len must be positive")
	}
	s := b.sb.String()
	ns := Substring(s, 0, len)
	b.sb = &strings.Builder{}
	b.AppendString(ns)
	return b
}

func (b *StringBuilder) String() string {
	return b.sb.String()
}

func (b *StringBuilder) ToStringHelper() string {
	s := b.sb.String()
	if strings.HasSuffix(s, ",") {
		return Substring(s, 0, len(s)-1)
	} else {
		return s
	}
}

//获取source的子串,如果start小于0或者end大于source长度则返回""
//start:开始index，从0开始，包括0
//end:结束index，以end结束，但不包括end
func Substring(source string, start int, end int) string {
	var r = []rune(source)
	length := len(r)

	if start < 0 || end > length || start > end {
		return ""
	}

	if start == 0 && end == length {
		return source
	}

	return string(r[start:end])
}

func SubStrNoEnd(source string, start int) string {
	var r = []rune(source)
	if start < 0 {
		return ""
	}

	if start == 0 {
		return source
	}

	return string(r[start:])
}
