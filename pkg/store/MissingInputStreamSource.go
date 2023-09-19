package store

// public class MissingInputStreamSource<S extends ValueInputStream<?>> implements InputStreamSource<S>
// type MissingInputStreamSource[S IValueInputStream] struct {
// 	// 继承
// 	// InputStreamSource[S]
// 	InputStreamSource
// }

type MissingInputStreamSource struct {
	// 继承
	// InputStreamSource[S]
	InputStreamSource
}

// public static <S extends ValueInputStream<?>> InputStreamSource<S> missingStreamSource(Class<S> streamType)
func MissingStreamSource() *MissingInputStreamSource {
	return newMissingInputStreamSource()
}

// private MissingInputStreamSource(Class<S> streamType)
func newMissingInputStreamSource() *MissingInputStreamSource {
	me := new(MissingInputStreamSource)
	return me
}

//@Nullable
//@Override
func (me *MissingInputStreamSource) OpenStream() IValueInputStream {
	return nil
}
