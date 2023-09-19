package store

// public interface InputStreamSource<S extends ValueInputStream<?>>
type InputStreamSource interface {
	OpenStream() IValueInputStream
}

// type InputStreamSource[S IValueInputStream] interface {
// 	OpenStream() S
// }
