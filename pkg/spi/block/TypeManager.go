package block

type TypeManager interface {

	/**
	 * Gets the type with the specified signature.
	 *
	 * @throws TypeNotFoundException if not found
	 */
	GetType(signature *TypeSignature) Type
}
