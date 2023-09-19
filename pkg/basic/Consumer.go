package basic

type Consumer[T Object] interface {

	/**
	 * Performs this operation on the given argument.
	 *
	 * @param t the input argument
	 */
	Accept(t T)
}
