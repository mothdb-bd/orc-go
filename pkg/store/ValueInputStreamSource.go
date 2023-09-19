package store

// public class ValueInputStreamSource<S extends ValueInputStream<?>> implements InputStreamSource<S>

type ValueInputStreamSource[S IValueInputStream] struct {
	// 继承
	InputStreamSource

	stream S
}

func NewValueInputStreamSource[S IValueInputStream](stream S) *ValueInputStreamSource[S] {
	ve := new(ValueInputStreamSource[S])
	ve.stream = stream
	return ve
}

//@Nullable
//@Override
func (ve *ValueInputStreamSource[S]) OpenStream() S {
	return ve.stream
}
