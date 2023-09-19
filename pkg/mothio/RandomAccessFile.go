package mothio

import (
	"io"

	"github.com/mothdb-bd/orc-go/pkg/iostream"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type RandomAccessFile struct {
	input  *iostream.InputStream
	output *iostream.OutputStream
}

func NewRandomAccessFile(randAcc io.ReadWriteSeeker) *RandomAccessFile {
	return &RandomAccessFile{
		input:  iostream.NewInputStream(randAcc),
		output: iostream.NewOutputStream(randAcc),
	}
}

func (rl *RandomAccessFile) Seek(offset int64, whence int) (int64, error) {
	return rl.input.Seek(offset, whence)
}

func (rl *RandomAccessFile) ReadFully2(b []byte, off int, bLen int) {
	l := len(b)
	if off == 0 && l == bLen {
		rl.input.Read(b)
	} else {
		nl := maths.MinInt(l-off, bLen-off)
		nb := make([]byte, nl)
		rl.input.Read(nb)
		util.CopyBytes(nb, 0, b, int32(off), int32(bLen))
	}
}

func (rl *RandomAccessFile) Close() error {
	return rl.output.Close()
}

// type RandomAccessFile interface {
// 	// // 继承
// 	// DataOutput
// 	// // 继承
// 	// DataInput

// 	Seek(pos int64) (int64, error)

// 	ReadFully2(b []byte, off int, len int)
// 	Close() error
// }

// type IO_RWSC interface {
// 	io.ReadWriteSeeker
// 	io.Closer
// }

// type RandomAccessFileImpl struct {
// 	RandomAccessFile

// 	ranAccess IO_RWSC
// }

// func NewRandomAccessFile(ranAccess IO_RWSC) RandomAccessFile {
// 	rl := new(RandomAccessFileImpl)
// 	rl.ranAccess = ranAccess
// 	return rl
// }

// func (rl *RandomAccessFileImpl) Seek(pos int64) (int64, error) {
// 	return rl.ranAccess.Seek(pos, io.SeekStart)
// }

// func (rl *RandomAccessFileImpl) ReadFully2(b []byte, off int, bLen int) {
// 	l := len(b)
// 	if off == 0 && l == bLen {
// 		rl.ranAccess.Read(b)
// 	} else {
// 		nl := maths.MinInt(l-off, bLen-off)
// 		nb := make([]byte, nl)
// 		rl.ranAccess.Read(nb)
// 		util.CopyBytes(nb, 0, b, int32(off), int32(bLen))
// 	}
// }

// func (rl *RandomAccessFileImpl) Close() error {
// 	return rl.ranAccess.Close()
// }
