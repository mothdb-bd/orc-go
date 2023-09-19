package slice

type FixedLengthSliceInput interface {
	// 继承
	SliceInput

	Length() int64
	Remaining() int64
}
