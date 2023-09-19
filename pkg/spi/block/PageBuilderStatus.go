package block

var DEFAULT_MAX_PAGE_SIZE_IN_BYTES int32 = 1024 * 1024

type PageBuilderStatus struct {
	maxPageSizeInBytes int32
	currentSize        int64
}

func NewPageBuilderStatus() *PageBuilderStatus {
	return NewPageBuilderStatus2(DEFAULT_MAX_PAGE_SIZE_IN_BYTES)
}
func NewPageBuilderStatus2(maxPageSizeInBytes int32) *PageBuilderStatus {
	ps := new(PageBuilderStatus)
	ps.maxPageSizeInBytes = maxPageSizeInBytes
	return ps
}

func (ps *PageBuilderStatus) CreateBlockBuilderStatus() *BlockBuilderStatus {
	return NewBlockBuilderStatus(ps)
}

func (ps *PageBuilderStatus) GetMaxPageSizeInBytes() int32 {
	return ps.maxPageSizeInBytes
}

func (ps *PageBuilderStatus) IsEmpty() bool {
	return ps.currentSize == 0
}

func (ps *PageBuilderStatus) IsFull() bool {
	return ps.currentSize >= int64(ps.maxPageSizeInBytes)
}

func (ps *PageBuilderStatus) addBytes(bytes int32) {
	if bytes < 0 {
		panic("bytes cannot be negative")
	}
	ps.currentSize += int64(bytes)
}

func (ps *PageBuilderStatus) GetSizeInBytes() int64 {
	return ps.currentSize
}
