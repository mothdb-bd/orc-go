package block

type LazyBlockLoader interface {
	/**
	 * Loads a lazy block. If possible lazy block loader should load top level {@link Block} only
	 * (in case of when loaded blocks are nested, e.g for structural types).
	 */
	Load() Block
}
