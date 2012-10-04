package tdhsocket

import (
  "io"
)

func (self *Tdh) newResultBodyReader(initLength uint32, initCode uint32) *ResultBodyReader {
  return &ResultBodyReader{
    remainPacketLength: int(initLength),
    lastCode: initCode,
    buf: self.conn,
    packetReadHeaderFunc: func(buf io.Reader) (uint32, int) {
      code, length := self.readHeader(buf)
      return code, int(length)
    },
  }
}

type ResultBodyReader struct {
  remainPacketLength int
  lastCode uint32
  buf io.Reader
  packetReadHeaderFunc func(io.Reader) (uint32, int)
}

func (self *ResultBodyReader) Read(target []byte) (alreadyReadN int, err error) {
  needN := len(target)
  if needN == 0 {
    return 0, nil
  }
  if self.remainPacketLength == 0 { // can't provide
    return 0, io.EOF
  }

  var willReadN int
  alreadyReadN = 0

  for {
    willReadN = needN
    if self.remainPacketLength < willReadN {
      willReadN = self.remainPacketLength
    }
    err := read(self.buf, target[alreadyReadN : alreadyReadN + willReadN])
    if err != nil {
      err = io.EOF
      break
    }
    self.remainPacketLength -= willReadN
    alreadyReadN += willReadN
    needN -= willReadN
    if self.remainPacketLength == 0 && self.lastCode == CLIENT_STATUS_ACCEPT { // read next packet
      self.lastCode, self.remainPacketLength = self.packetReadHeaderFunc(self.buf)
    }
    if needN == 0 {
      break
    } else if self.remainPacketLength == 0 { // can't provide
      err = io.EOF
      break
    }
  }
  return
}
