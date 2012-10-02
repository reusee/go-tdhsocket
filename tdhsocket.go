package tdhsocket

import (
  "net"
  "encoding/binary"
  "bytes"
  "io"
  "strconv"
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

func writeStr(buf io.Writer, str string) {
  write(buf, uint32(len(str) + 1))
  buf.Write([]byte(str))
  buf.Write([]byte("\x00"))
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
  writeStr(data, readCode)
  writeStr(data, writeCode)

  self.writeHeader(self.conn, REQUEST_TYPE_SHAKE_HANDS, uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())
}

func (self *Tdh) Insert(dbname string, table string, index string, fields []string, values []string) (err error) {
  data := new(bytes.Buffer)

  writeStr(data, dbname)
  writeStr(data, table)
  writeStr(data, index)
  write(data, uint32(len(fields)))
  for _, field := range fields {
    writeStr(data, field)
  }
  write(data, uint32(len(values)))
  for _, value := range values {
    data.Write([]byte("\x00"))
    writeStr(data, value)
  }

  self.writeHeader(self.conn, REQUEST_TYPE_INSERT, uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  code, length := self.readHeader()
  if code == CLIENT_STATUS_OK {
    read(self.conn, make([]byte, length))
    err = nil
  } else {
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
  }
  return
}

type Filter struct {
  field string
  op uint8
  value string
}

func (self *Tdh) getOrCount(reqType uint32, dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) {
  data := new(bytes.Buffer)

  writeStr(data, dbname)
  writeStr(data, table)
  writeStr(data, index)
  write(data, uint32(len(fields)))
  for _, field := range fields {
    writeStr(data, field)
  }
  write(data, uint32(len(keys)))
  for _, key := range keys {
    write(data, uint32(len(key)))
    for _, column := range key {
      writeStr(data, column)
    }
  }
  write(data, op)
  write(data, start)
  write(data, limit)
  write(data, uint32(len(filters)))
  for _, filter := range filters {
    writeStr(data, filter.field)
    write(data, filter.op)
    writeStr(data, filter.value)
  }

  self.writeHeader(self.conn, reqType, uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())
}

func (self *Tdh) Get(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) (rows [][][]byte, 
                     fieldTypes []uint8, err error) {
  self.getOrCount(REQUEST_TYPE_GET, dbname, table, index, fields, keys, op, start, limit, filters)
  return self.parseGetResult()
}

func (self *Tdh) Count(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) (count int, err error) {
  self.getOrCount(REQUEST_TYPE_COUNT, dbname, table, index, fields, keys, op, start, limit, filters)
  rows, _, err := self.parseGetResult()
  if err != nil {
    return -1, err
  }
  count, _ = strconv.Atoi(string(rows[0][0]))
  return count, nil
}

func (self *Tdh) parseGetResult() (rows [][][]byte, fieldTypes []uint8, err error) {
  code, length := self.readHeader()
  var numFields, remainLength uint32
  switch code {
  case CLIENT_STATUS_OK:
    numFields, fieldTypes, remainLength = self.parseResultHead(length)
    rows = self.parseResultBody(self.conn, numFields, remainLength)
  case CLIENT_STATUS_ACCEPT:
    numFields, fieldTypes, remainLength = self.parseResultHead(length)
    totalLength := remainLength
    bodies := make([][]byte, 0)
    body := make([]byte, remainLength)
    read(self.conn, body)
    bodies = append(bodies, body)
    for {
      code, length := self.readHeader()
      totalLength += length
      body := make([]byte, length)
      read(self.conn, body)
      bodies = append(bodies, body)
      if code == CLIENT_STATUS_OK {
        break
      }
    }
    rows = self.parseResultBody(ReaderOfBytesArray(bodies), numFields, totalLength)
  default:
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
  }
  return rows, fieldTypes, err
}

func (self *Tdh) parseResultHead(length uint32) (numFields uint32, fieldTypes []uint8, remainLength uint32) {
  remainLength = length
  read(self.conn, &numFields)
  remainLength -= 4
  fieldTypes = make([]uint8, numFields)
  for i := uint32(0); i < numFields; i++ {
    read(self.conn, &fieldTypes[i])
    remainLength -= 1
  }
  return
}

func (self *Tdh) parseResultBody(buf io.Reader, numFields uint32, length uint32) [][][]byte {
  var fieldLength uint32
  rows := make([][][]byte, 0)
  for length > 0 {
    fieldValues := make([][]byte, numFields)
    for i := uint32(0); i < numFields; i++ {
      read(buf, &fieldLength)
      length -= 4
      fieldValue := make([]byte, fieldLength)
      fieldValues[i] = fieldValue
      read(buf, fieldValue)
      length -= fieldLength
    }
    rows = append(rows, fieldValues)
  }
  return rows
}

func (self *Tdh) writeHeader(buf io.Writer, command uint32, reserved uint32, length uint32) {
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
