package iostream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"unsafe"
)

// maxVintLen64 可变整数编码后的64-bit整数长度
const maxVintLen64 = 10 * 7

var errOverflow = errors.New("binary: varint overflows a 64-bit integer")

// source 表示解码器正常工作的源接口
type source interface {
	io.Reader
	io.Seeker
	io.ByteReader
	Slice(n int) ([]byte, error)
	ReadUvarint() (uint64, error)
	ReadVarint() (int64, error)
	Offset() int64
}

// newSource 根据类型进入不同的源处理
func newSource(r io.Reader) source {
	switch r := r.(type) {
	case nil:
		return newSliceSource(nil)
	case *bytes.Buffer:
		return newSliceSource(r.Bytes())
	case *sliceSource:
		return r
	default:
		ret, ok := r.(source)
		if !ok {
			ret = newStreamSource(r)
		}
		return ret
	}
}

type sliceSource struct {
	buffer []byte
	offset int64
}

func newSliceSource(b []byte) *sliceSource {
	return &sliceSource{buffer: b}
}

// Slice 读取下边的n长度的slice
func (s *sliceSource) Slice(n int) ([]byte, error) {
	if s.offset+int64(n) > int64(len(s.buffer)) {
		return nil, io.EOF
	}
	cur := s.offset
	s.offset += int64(n)
	return s.buffer[cur:s.offset], nil
}

// ReadUvarint 读取编码的无符号int并将其返回为uint64
func (s *sliceSource) ReadUvarint() (uint64, error) {
	var ret uint64
	for i := 0; i < maxVintLen64; i += 7 {
		if s.offset >= int64(len(s.buffer)) {
			return 0, io.EOF
		}
		// 参考littleEndian
		b := s.buffer[s.offset]
		s.offset++
		if b < 0x80 {
			if i == maxVintLen64-7 && b > 1 {
				return ret, errOverflow
			}
			return ret | uint64(b)<<i, nil
		}
		ret |= uint64(b&0x7f) << i
	}
	return ret, errOverflow
}

// ReadUvarint 读取编码的有符号int并将其返回为int64
func (s *sliceSource) ReadVarint() (int64, error) {
	uret, err := s.ReadUvarint()
	ret := int64(uret >> 1)
	if uret&1 != 0 {
		ret = ^ret
	}
	return ret, err
}

func (s *sliceSource) Offset() int64 {
	return s.offset
}

// Read 读取p个byte,实现io.Reader接口
func (s *sliceSource) Read(p []byte) (n int, err error) {
	if s.offset >= int64(len(s.buffer)) {
		return 0, io.EOF
	}
	n = copy(p, s.buffer[s.offset:])
	s.offset += int64(n)
	return
}

// Seek 参考bytes和string编写，可能存在疑问的点io.SeekEnd
func (s *sliceSource) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.offset + offset
	case io.SeekEnd:
		abs = int64(len(s.buffer)) + offset
	default:
		return 0, errors.New("sliceSource.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("sliceSource.Seek: negative position")
	}
	s.offset = abs
	return abs, nil
}

// ReadByte 读取一个byte,实现io.ByteReader接口
func (s *sliceSource) ReadByte() (byte, error) {
	if s.offset >= int64(len(s.buffer)) {
		return 0, io.EOF
	}
	b := s.buffer[s.offset]
	s.offset++
	return b, nil
}

type streamSource struct {
	io.Reader
	io.ByteReader
	buffer []byte
	offset int64
}

func newStreamSource(r io.Reader) *streamSource {
	s := &streamSource{Reader: r}

	// 如果Reader就是一个ByteReader，说明正在读byte
	if br, ok := r.(io.ByteReader); ok {
		s.ByteReader = br
		return s
	}

	// 如果不存在byte reader，包装一个出来
	br := bufio.NewReader(r)
	s.Reader = br
	s.ByteReader = br
	return s
}

// Slice 读取下边的n长度的slice
func (s *streamSource) Slice(n int) ([]byte, error) {
	if len(s.buffer) < n {
		s.buffer = make([]byte, capacityFor(uint(n+1)))
	}

	// 从流中将数据读取的buffer中
	n, err := io.ReadAtLeast(s.Reader, s.buffer[:n], n)
	s.offset += int64(n)
	return s.buffer[:n], err
}

// ReadUvarint 读取编码的无符号int并将其返回为uint64
func (s *streamSource) ReadUvarint() (uint64, error) {
	return binary.ReadUvarint(s)
}

// ReadUvarint 读取编码的有符号int并将其返回为int64
func (s *streamSource) ReadVarint() (int64, error) {
	return binary.ReadVarint(s)
}

func (s *streamSource) Offset() int64 {
	return s.offset
}

// Read 读取p个byte,实现io.Reader接口
func (s *streamSource) Read(p []byte) (n int, err error) {
	n, err = s.Reader.Read(p)
	s.offset += int64(n)
	return
}

// Seek 参考bytes和string编写，可能存在疑问的点io.SeekEnd
func (s *streamSource) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.offset + offset
	case io.SeekEnd:
		abs = int64(len(s.buffer)) + offset
	default:
		return 0, errors.New("sliceSource.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("sliceSource.Seek: negative position")
	}
	s.offset = abs
	return abs, nil
}

// ReadByte 读取一个byte,实现io.ByteReader接口
func (s *streamSource) ReadByte() (byte, error) {
	b, err := s.ByteReader.ReadByte()
	s.offset++
	return b, err
}

// capacityFor 计算一个指定index的2的次方
func capacityFor(v uint) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return int(v)
}

func toString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}

func toBytes(s string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	byteHeader := (*reflect.StringHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(s)
	byteHeader.Len = l
	return
}
