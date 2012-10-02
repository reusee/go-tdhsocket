package tdhsocket

import (
  "io"
)

func ReaderOfBytesArray(ary [][]byte) *Reader {
  return &Reader{
    s: ary,
    si: 0,
    i: 0,
  }
}

type Reader struct {
  s [][]byte
  si int
  i int
}

func (self *Reader) Read(target []byte) (n int, err error) {
  need := len(target)
  if need == 0 {
    return 0, nil
  }
  if self.si >= len(self.s) {
    return 0, io.EOF
  }
  if self.si >= len(self.s) - 1 && self.i >= len(self.s[self.si]) {
    return 0, io.EOF
  }

  targetIndex := 0
  for need > 0 {
    if self.si >= len(self.s) {
      return targetIndex, io.EOF
    }
    if self.si >= len(self.s) - 1 && self.i >= len(self.s[self.si]) {
      return targetIndex, io.EOF
    }
    canProvide := len(self.s[self.si]) - self.i
    getLength := canProvide
    if need < getLength {
      getLength = need
    }
    readN := copy(target[targetIndex:], self.s[self.si][self.i : self.i + getLength])
    need -= readN
    targetIndex += readN
    self.i += readN
    if self.i >= len(self.s[self.si]) {
      self.si += 1
      self.i = 0
    }
  }

  return len(target), nil
}
