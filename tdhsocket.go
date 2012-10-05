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

  self.writeHeader(self.conn, REQUEST_TYPE_SHAKE_HANDS, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())
}

func (self *Tdh) writeInsertRequest(data io.Writer, dbname string, table string, index string, fields []string, values []string) {
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

func (self *Tdh) readInsertResult() (err error) {
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

func (self *Tdh) Insert(dbname string, table string, index string, fields []string, values []string) (err error) {
  data := new(bytes.Buffer)
  self.writeInsertRequest(data, dbname, table, index, fields, values)
  self.writeHeader(self.conn, REQUEST_TYPE_INSERT, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readInsertResult()
}

type Filter struct {
  field string
  op uint8
  value string
}

func (self *Tdh) writeGetRequest(data io.Writer, dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) {
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
}

func (self *Tdh) Get(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) (rows [][][]byte, 
                     fieldTypes []uint8, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  fmt.Printf("Get data: %#q\n", data)
  self.writeHeader(self.conn, REQUEST_TYPE_GET, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readResult()
}

func (self *Tdh) readCountResult() (count int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return -1, err
  }
  count, _ = strconv.Atoi(string(rows[0][0]))
  return count, nil
}

func (self *Tdh) Count(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) (count int, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  self.writeHeader(self.conn, REQUEST_TYPE_COUNT, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readCountResult()
}

func (self *Tdh) writeUpdateRequest(data io.Writer, dbname string, table string, index string, fields []string, keys [][]string,
                                    op uint8, start uint32, limit uint32, filters []*Filter, values []string) {
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  write(data, uint32(len(values)))
  for _, value := range values {
    write(data, uint8(0))
    writeStr(data, value)
  }
}

func (self *Tdh) readUpdateResult() (count int, change int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return 0, 0, err
  }
  count, _ = strconv.Atoi(string(rows[0][0]))
  change, _ = strconv.Atoi(string(rows[0][1]))
  return count, change, nil
}

func (self *Tdh) Update(dbname string, table string, index string, fields []string, keys [][]string,
                        op uint8, start uint32, limit uint32, filters []*Filter, values []string) (count int, change int, err error) {
  data := new(bytes.Buffer)
  self.writeUpdateRequest(data, dbname, table, index, fields, keys, op, start, limit, filters, values)
  self.writeHeader(self.conn, REQUEST_TYPE_UPDATE, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readUpdateResult()
}

func (self *Tdh) readDeleteResult() (change int, err error) {
  rows, _, err := self.readResult()
  if err != nil {
    return 0, err
  }
  change, _ = strconv.Atoi(string(rows[0][0]))
  return change, nil
}

func (self *Tdh) Delete(dbname string, table string, index string, fields []string, keys [][]string,
                     op uint8, start uint32, limit uint32, filters []*Filter) (change int, err error) {
  data := new(bytes.Buffer)
  self.writeGetRequest(data, dbname, table, index, fields, keys, op, start, limit, filters)
  self.writeHeader(self.conn, REQUEST_TYPE_DELETE, self.getSequence(), uint32(0), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  return self.readDeleteResult()
}

func (self *Tdh) Batch(requests ...*Request) (ret []*Response, err error) {
  data := new(bytes.Buffer)
  reqTypes := make([]int, len(requests))
  headerSequence := self.getSequence()
  for i, r := range requests {
    reqTypes[i] = r.req.t
    subdata := new(bytes.Buffer)
    switch r.req.t {
    case DELETE:
      self.writeGetRequest(subdata, r.req.dbname, r.req.table, r.req.index, r.req.fields, r.keys, r.op, r.start, r.limit, r.filters)
      self.writeHeader(data, REQUEST_TYPE_DELETE, self.getSequence(), uint32(0), uint32(len(subdata.Bytes())))
      data.Write(subdata.Bytes())
    case UPDATE:
      self.writeUpdateRequest(subdata, r.req.dbname, r.req.table, r.req.index, r.req.fields, r.keys, r.op, r.start, r.limit, r.filters, r.values)
      self.writeHeader(data, REQUEST_TYPE_UPDATE, self.getSequence(), uint32(0), uint32(len(subdata.Bytes())))
      data.Write(subdata.Bytes())
    case INSERT:
      self.writeInsertRequest(subdata, r.req.dbname, r.req.table, r.req.index, r.req.fields, r.values)
      self.writeHeader(data, REQUEST_TYPE_INSERT, self.getSequence(), uint32(0), uint32(len(subdata.Bytes())))
      data.Write(subdata.Bytes())
    default:
      panic("Batch request type unknown. Be sure pointer is passed")
    }
  }
  self.writeHeader(self.conn, REQUEST_TYPE_BATCH, headerSequence, uint32(len(requests)), uint32(len(data.Bytes())))
  self.conn.Write(data.Bytes())

  ret = make([]*Response, len(requests))
  code, _ := self.readHeader(self.conn)
  if code != CLIENT_STATUS_MULTI_STATUS {
    var errorCode uint32
    read(self.conn, &errorCode)
    err = &Error{code, errorCode}
    return ret, err
  }
  for i := 0; i < len(requests); i++ {
    switch reqTypes[i] {
    case DELETE:
      change, err := self.readDeleteResult()
      ret[i] = &Response{t: DELETE, change: change, count: change, err: err}
    case UPDATE:
      count, change, err := self.readUpdateResult()
      ret[i] = &Response{t: UPDATE, count: count, change: change, err: err}
    case INSERT:
      err := self.readInsertResult()
      ret[i] = &Response{t: INSERT, err: err}
    }
  }
  return ret, nil
}

func (self *Tdh) readResult() (rows [][][]byte, fieldTypes []uint8, err error) {
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

func (self *Tdh) readResultHead(length uint32) (numFields uint32, fieldTypes []uint8, remainLength uint32) {
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

func (self *Tdh) readResultBody(numFields uint32, length uint32, code uint32) [][][]byte {
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
      fieldValues[i] = fieldValue
      read(reader, fieldValue)
    }
    rows = append(rows, fieldValues)
  }
  return rows
}

func (self *Tdh) getSequence() uint32 {
  self.sequenceId++
  return self.sequenceId
}

func (self *Tdh) writeHeader(buf io.Writer, command uint32, sequence uint32, reserved uint32, length uint32) {
  write(buf, uint32(0xffffffff))
  write(buf, command)
  write(buf, sequence)
  write(buf, reserved)
  write(buf, length)
}

func (self *Tdh) readHeader(buf io.Reader) (uint32, uint32) {
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

type Req struct {
  t int
  dbname string
  table string
  index string
  fields []string
}

type Request struct {
  req *Req
  keys [][]string
  op uint8
  start uint32
  limit uint32
  filters []*Filter
  values []string
}

type Response struct {
  t int
  change int
  count int
  err error
}
