package function

import "github.com/mothdb-bd/orc-go/pkg/basic"

type Function[T, R basic.Object] interface {

	/**
	 * Applies this function to the given argument.
	 *
	 * @param t the function argument
	 * @return the function result
	 */
	Apply(t T) R
}

type ToLongFunction[T basic.Object] interface {

	/**
	 * Applies this function to the given argument.
	 *
	 * @param value the function argument
	 * @return the function result
	 */
	ApplyAsLong(value T) int64
}

type Supplier[T basic.Object] interface {

	/**
	 * Gets a result.
	 *
	 * @return a result
	 */
	Get() T
}
