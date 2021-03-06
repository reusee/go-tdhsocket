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
  Timeout = 1000 * 60 * 5
)

type Conn struct {
  conn net.Conn
  sequenceId uint32

  batchBuffer *bytes.Buffer
  batchSeqId uint32
  batchReqTypes []int
  inBatchMode bool

  writer io.Writer
}

func New(hostPort string, readCode string, writeCode string) (*Conn, error) {
  self := &Conn{
    batchBuffer: new(bytes.Buffer),
    batchReqTypes: make([]int, 0),
    inBatchMode: false,
  }
  self.batchSeqId = self.getSequence()
  conn, err := net.Dial("tcp", hostPort)
  if err != nil {
    return nil, err
  }
  self.conn = conn
  self.writer = self.conn
  self.handShake(readCode, writeCode)
  return self, nil
}

func writeStr(buf io.Writer, str string) {
  if str == "(null)" {
    write(buf, uint32(0))
    return
  }
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

func (self *Conn) handShake(readCode string, writeCode string) {
  data := new(bytes.Buffer)

  data.Write([]byte("TDHS"))
  write(data, uint32(2)) // protocol version
  write(data, uint32(Timeout))
  writeStr(data, readCode)
  writeStr(data, writeCode)

  self.writeHeader(self.conn, REQUEST_TYPE_SHAKE_HANDS, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())
}

func (self *Conn) writeInsertRequest(data io.Writer, dbname string, table string, index string, fields []string, values []string) {
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
}

func (self *Conn) readInsertResult() (err error) {
  code, length := self.readHeader(self.conn)
  if code == CLIENT_STATUS_OK {
    read(self.conn, make([]byte, length))
    err = nil
  } else {
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
  }
  return err
}

func (self *Conn) Insert(dbname string, table string, index string, fields []string, values []string) (err error) {
  data := new(bytes.Buffer)
  self.writeInsertRequest(data, dbname, table, index, fields, values)
  self.writeHeader(self.writer, REQUEST_TYPE_INSERT, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.writer.Write(data.Bytes())
  if self.inBatchMode {
    self.batchReqTypes = append(self.batchReqTypes, INSERT)
  } else {
    err = self.readInsertResult()
  }
  return
}

func (self *Conn) writeGetRequest(data io.Writer, dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []Filter) {
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
    writeStr(data, filter.Field)
    write(data, filter.Op)
    writeStr(data, filter.Value)
  }
}

func (self *Conn) Get(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []Filter) (rows [][][]byte, 
                     fieldTypes []uint8, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  self.writeHeader(self.conn, REQUEST_TYPE_GET, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readResult()
}

func (self *Conn) readCountResult() (count int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return -1, err
  }
  count, _ = strconv.Atoi(string(rows[0][0]))
  return count, nil
}

func (self *Conn) Count(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []Filter) (count int, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  self.writeHeader(self.conn, REQUEST_TYPE_COUNT, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readCountResult()
}

func (self *Conn) writeUpdateRequest(data io.Writer, dbname string, table string, index string, fields []string, keys [][]string,
                                    op uint8, start uint32, limit uint32, filters []Filter, values []string) {
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  write(data, uint32(len(values)))
  for _, value := range values {
    write(data, uint8(0))
    writeStr(data, value)
  }
}

func (self *Conn) readUpdateResult() (count int, change int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return 0, 0, err
  }
  count, _ = strconv.Atoi(string(rows[0][0]))
  change, _ = strconv.Atoi(string(rows[0][1]))
  return count, change, nil
}

func (self *Conn) Update(dbname string, table string, index string, fields []string, keys [][]string,
                        op uint8, start uint32, limit uint32, filters []Filter, values []string) (count int, change int, err error) {
  data := new(bytes.Buffer)
  self.writeUpdateRequest(data, dbname, table, index, fields, keys, op, start, limit, filters, values)
  self.writeHeader(self.writer, REQUEST_TYPE_UPDATE, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.writer.Write(data.Bytes())
  if self.inBatchMode {
    self.batchReqTypes = append(self.batchReqTypes, UPDATE)
  } else {
    count, change, err = self.readUpdateResult()
  }
  return
}

func (self *Conn) readDeleteResult() (change int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return 0, err
  }
  change, _ = strconv.Atoi(string(rows[0][0]))
  return change, nil
}

func (self *Conn) Delete(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []Filter) (change int, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  self.writeHeader(self.writer, REQUEST_TYPE_DELETE, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.writer.Write(data.Bytes())
  if self.inBatchMode {
    self.batchReqTypes = append(self.batchReqTypes, DELETE)
  } else {
    change, err = self.readDeleteResult()
  }
  return
}

func (self *Conn) clearBatchBuffer() bool {
  if self.batchBuffer.Len() > 0 { // clear batch buffer
    self.writeHeader(self.conn, REQUEST_TYPE_BATCH, self.batchSeqId, uint32(len(self.batchReqTypes)), uint32(self.batchBuffer.Len()))
    self.conn.Write(self.batchBuffer.Bytes())
    self.batchBuffer.Reset()
    self.batchSeqId = self.getSequence()
    return true
  }
  return false
}

func (self *Conn) Batch() (ret []Response, err error) {
  send := self.clearBatchBuffer()
  self.inBatchMode = true
  self.writer = self.batchBuffer
  if send {
    ret, err = self.readBatchResult()
  }
  self.batchReqTypes = make([]int, 0)
  return
}

func (self *Conn) Commit() (ret []Response, err error) {
  send := self.clearBatchBuffer()
  self.inBatchMode = false
  self.writer = self.conn
  if send {
    ret, err = self.readBatchResult()
  }
  self.batchReqTypes = make([]int, 0)
  return
}

func (self *Conn) readBatchResult() (ret []Response, err error) {
  ret = make([]Response, len(self.batchReqTypes))
  code, _ := self.readHeader(self.conn)
  if code != CLIENT_STATUS_MULTI_STATUS {
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
    return ret, err
  }
  for i, t := range self.batchReqTypes {
    switch t {
    case DELETE:
      change, err := self.readDeleteResult()
      ret[i] = Response{T: DELETE, Change: change, Count: change, Err: err}
    case UPDATE:
      count, change, err := self.readUpdateResult()
      ret[i] = Response{T: UPDATE, Count: count, Change: change, Err: err}
    case INSERT:
      err := self.readInsertResult()
      ret[i] = Response{T: INSERT, Err: err}
    }
  }
  return ret, nil
}

func (self *Conn) readResult() (rows [][][]byte, fieldTypes []uint8, err error) {
  code, length := self.readHeader(self.conn)
  var numFields, remainLength uint32
  switch code {
  case CLIENT_STATUS_OK, CLIENT_STATUS_ACCEPT:
    numFields, fieldTypes, remainLength = self.readResultHead(length)
    rows = self.readResultBody(numFields, remainLength, code)
  default:
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
  }
  return rows, fieldTypes, err
}

func (self *Conn) readResultHead(length uint32) (numFields uint32, fieldTypes []uint8, remainLength uint32) {
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

func (self *Conn) readResultBody(numFields uint32, length uint32, code uint32) [][][]byte {
  reader := self.newResultBodyReader(length, code)
  var fieldLength uint32
  rows := make([][][]byte, 0)
start:
  for {
    fieldValues := make([][]byte, numFields)
    for i := uint32(0); i < numFields; i++ {
      err := read(reader, &fieldLength)
      if err == io.EOF {
        break start
      }
      fieldValue := make([]byte, fieldLength)
      read(reader, fieldValue)
      if fieldLength == 1 && fieldValue[0] == byte(0x00) {
        fieldValue = []byte("")
      }
      fieldValues[i] = fieldValue
    }
    rows = append(rows, fieldValues)
  }
  return rows
}

func (self *Conn) getSequence() uint32 {
  self.sequenceId++
  return self.sequenceId
}

func (self *Conn) writeHeader(buf io.Writer, command uint32, sequence uint32, reserved uint32, length uint32) {
  write(buf, uint32(0xffffffff))
  write(buf, command)
  write(buf, sequence)
  write(buf, reserved)
  write(buf, length)
}

func (self *Conn) readHeader(buf io.Reader) (uint32, uint32) {
  var retCode, bodyLength, pad uint32
  read(self.conn, &pad)
  read(self.conn, &retCode)
  read(self.conn, &pad)
  read(self.conn, &pad)
  read(self.conn, &bodyLength)
  return retCode, bodyLength
}

type Error struct {
  ClientStatus uint32
  ErrorCode uint32
}

func (self *Error) Error() string {
  return fmt.Sprintf("(%d) %s, (%d) %s", 
    self.ClientStatus, ClientStatusMessage[self.ClientStatus],
    self.ErrorCode, ErrorCodeMessage[self.ErrorCode])
}

type Filter struct {
  Field string
  Op uint8
  Value string
}

type Response struct {
  T int
  Change int
  Count int
  Err error
}
