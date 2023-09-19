package util

import (
	"crypto/md5"
	"crypto/sha1"
)

var (
	EMPTY = []byte{}
)

func Md5Hash(data []byte) string {
	Md5Inst := md5.New()
	Md5Inst.Write(data)
	return string(Md5Inst.Sum(EMPTY))
}

func Md5HashBytes(data []byte) []byte {
	Md5Inst := md5.New()
	Md5Inst.Write(data)
	return Md5Inst.Sum(EMPTY)
}

func Sha1Hash(data []byte) string {
	Sha1Inst := sha1.New()
	Sha1Inst.Write(data)
	return string(Sha1Inst.Sum(EMPTY))
}

func Sha1HashBytes(data []byte) []byte {
	Sha1Inst := md5.New()
	Sha1Inst.Write(data)
	return Sha1Inst.Sum(EMPTY)
}
