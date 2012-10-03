package tdhsocket

const (
  REQUEST_TYPE_SHAKE_HANDS uint32 = 0xffff
  REQUEST_TYPE_GET uint32 = 0
  REQUEST_TYPE_COUNT uint32 = 1
  REQUEST_TYPE_INSERT uint32 = 12

  CLIENT_STATUS_OK = 200 //完成所有数据的返回
  CLIENT_STATUS_ACCEPT = 202 //对于流的处理,还有未返回的数据
  CLIENT_STATUS_MULTI_STATUS = 207 //对于batch请求的返回,表示后面跟了多个请求

  CLIENT_STATUS_DB_ERROR = 502 //handler返回的错误信息

  EQ uint8 = 0 // = for asc
  GE uint8 = 1 // >=
  LE uint8 = 2 // <=
  GT uint8 = 3 // >
  LT uint8 = 4 // <
  IN uint8 = 5 // in
  DEQ uint8 = 6 // = for desc

  FILTER_EQ uint8 = 0 // =
  FILTER_GE uint8 = 1 // >=
  FILTER_LE uint8 = 2 // <=
  FILTER_GT uint8 = 3 // >
  FILTER_LT uint8 = 4 // <
  FILTER_NOT uint8 = 5 // !
)
