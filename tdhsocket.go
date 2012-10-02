package tdhsocket

import (
  "net"
  "encoding/binary"
  "bytes"
  "io"
  "fmt"
)

var (
  Timeout = 1000
)

type Tdh struct {
  conn net.Conn
  sequenceId uint32
}

func New(hostPort string, readCode string, writeCode string) (*Tdh, error) {
  self := &Tdh{}
  conn, err := net.Dial("tcp", hostPort)
  if err != nil {
    return nil, err
  }
  self.conn = conn
  self.handShake(readCode, writeCode)
  return self, nil
}

func packStr(str string) []byte {
  ret := new(bytes.Buffer)
  write(ret, uint32(len(str) + 1))
  ret.Write([]byte(str))
  ret.Write([]byte("\x00"))
  return ret.Bytes()
}

func write(buf io.Writer, data interface{}) error {
  return binary.Write(buf, binary.BigEndian, data)
}

func read(buf io.Reader, data interface{}) error {
  return binary.Read(buf, binary.BigEndian, data)
}

func (self *Tdh) handShake(readCode string, writeCode string) {
  data := new(bytes.Buffer)

  data.Write([]byte("TDHS"))
  write(data, uint32(1))
  write(data, uint32(Timeout))
  data.Write(packStr(readCode))
  data.Write(packStr(writeCode))

  header := new(bytes.Buffer)
  self.writeHeader(header, REQUEST_TYPE_SHAKE_HANDS, uint32(0), uint32(len(data.Bytes())))

  self.conn.Write(header.Bytes())
  self.conn.Write(data.Bytes())
}

func (self *Tdh) Insert(dbname string, table string, index string, fields []string, values []string) (ret error) {
  data := new(bytes.Buffer)

  data.Write(packStr(dbname))
  data.Write(packStr(table))
  data.Write(packStr(index))
  write(data, uint32(len(fields)))
  for _, field := range fields {
    data.Write(packStr(field))
  }
  write(data, uint32(len(values)))
  for _, value := range values {
    data.Write([]byte("\x00"))
    data.Write(packStr(value))
  }

  header := new(bytes.Buffer)
  self.writeHeader(header, REQUEST_TYPE_INSERT, uint32(0), uint32(len(data.Bytes())))

  self.conn.Write(header.Bytes())
  self.conn.Write(data.Bytes())

  code, length := self.readHeader()
  body := make([]byte, length)
  io.ReadFull(self.conn, body)
  if code == CLIENT_STATUS_OK {
    ret = nil
  } else if code == CLIENT_STATUS_ACCEPT {
    panic("TODO")
  } else if code == CLIENT_STATUS_MULTI_STATUS {
    panic("TODO")
  } else {
    var errorCode uint32
    read(bytes.NewBuffer(body), &errorCode)
    ret = &Error{code, errorCode}
  }
  return
}

func (self *Tdh) writeHeader(buf *bytes.Buffer, command uint32, reserved uint32, length uint32) {
  write(buf, uint32(0xffffffff))
  write(buf, command)
  self.sequenceId++
  write(buf, self.sequenceId)
  write(buf, reserved)
  write(buf, length)
}

func (self *Tdh) readHeader() (uint32, uint32) {
  retHeader := make([]byte, 20)
  io.ReadFull(self.conn, retHeader)
  var retCode, bodyLength, pad uint32
  headerBuffer := bytes.NewBuffer(retHeader)
  read(headerBuffer, &pad)
  read(headerBuffer, &retCode)
  read(headerBuffer, &pad)
  read(headerBuffer, &pad)
  read(headerBuffer, &bodyLength)
  return retCode, bodyLength
}

type Error struct {
  ResponseCode uint32
  ErrorCode uint32
}

func (self *Error) Error() string {
  return fmt.Sprintf("response code: %d, error code: %d", self.ResponseCode, self.ErrorCode)
}
