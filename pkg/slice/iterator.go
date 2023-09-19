package slice

// FindNullIterator find till null
func FindNullIterator(b byte) bool {
	return b != 0
}

// FindNotNullIterator find till not null
func FindNotNullIterator(b byte) bool {
	return b == 0
}

// FindCRIterator find till \r
func FindCRIterator(b byte) bool {
	return b != '\r'
}

// FindNotCRIterator find till not \r
func FindNotCRIterator(b byte) bool {
	return b == '\r'
}

// FindLFIterator find till \n
func FindLFIterator(b byte) bool {
	return b != '\n'
}

// FindNotLFIterator find till not \n
func FindNotLFIterator(b byte) bool {
	return b == '\n'
}

// FindCRLFIterator find till \r or \n
func FindCRLFIterator(b byte) bool {
	return b != '\r' && b != '\n'
}

// FindNotCRLFIterator find till not \r or \n
func FindNotCRLFIterator(b byte) bool {
	return b == '\r' || b == '\n'
}

// FindWhiteSpaceIterator fdin till \s \t \r\ n
func FindWhiteSpaceIterator(b byte) bool {
	return b != ' ' && b != '\n' && b != '\t'
}

// FindNotWhiteSpaceIterator fdin till \s \t \r\ n
func FindNotWhiteSpaceIterator(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t'
}
