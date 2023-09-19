package block

import "github.com/mothdb-bd/orc-go/pkg/slice"

func ByteCountWithoutTrailingSpace2(slice *slice.Slice, offset int32, length int32) int32 {

	if length < 0 {
		panic("length must be greater than or equal to zero")
	}
	if offset < 0 || offset+length > slice.SizeInt32() {
		panic("invalid offset/length")
	}
	for i := length + offset; i > offset; i-- {
		b, _ := slice.GetByte(int(i) - 1)
		if b != ' ' {
			return i - offset
		}
	}
	return 0
}

func ByteCountWithoutTrailingSpace(slice *slice.Slice, offset int32, length int32, codePointCount int32) int32 {
	truncatedLength := ByteCount(slice, offset, length, codePointCount)
	return ByteCountWithoutTrailingSpace2(slice, offset, truncatedLength)
}
