package tdhsocket

const (
  REQUEST_TYPE_SHAKE_HANDS uint32 = 0xffff
  REQUEST_TYPE_INSERT uint32 = 12

  CLIENT_STATUS_OK = 200 //完成所有数据的返回
  CLIENT_STATUS_ACCEPT = 202 //对于流的处理,还有未返回的数据
  CLIENT_STATUS_MULTI_STATUS = 207 //对于batch请求的返回,表示后面跟了多个请求

  CLIENT_STATUS_DB_ERROR = 502 //handler返回的错误信息
)
