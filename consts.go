package tdhsocket

const (
  REQUEST_TYPE_SHAKE_HANDS uint32 = 0xffff
  REQUEST_TYPE_GET uint32 = 0
  REQUEST_TYPE_COUNT uint32 = 1
  REQUEST_TYPE_UPDATE uint32 = 10
  REQUEST_TYPE_DELETE uint32 = 11
  REQUEST_TYPE_INSERT uint32 = 12
  REQUEST_TYPE_BATCH uint32 = 20

  CLIENT_STATUS_OK = 200 //完成所有数据的返回
  CLIENT_STATUS_ACCEPT = 202 //对于流的处理还有未返回的数据
  CLIENT_STATUS_MULTI_STATUS = 207 //对于batch请求的返回表示后面跟了多个请求

  CLIENT_STATUS_BAD_REQUEST = 400
  CLIENT_STATUS_FORBIDDEN = 403 //没权限
  CLIENT_STATUS_NOT_FOUND = 404 //没有找到资源如 db/table/index 等
  CLIENT_STATUS_REQUEST_TIME_OUT = 408 //超时
  CLIENT_STATUS_SERVER_ERROR = 500 //server无法处理的错误比如内存不够
  CLIENT_STATUS_NOT_IMPLEMENTED = 501 //server没实现这个功能
  CLIENT_STATUS_DB_ERROR = 502 //handler返回的错误信息
  CLIENT_STATUS_SERVICE_UNAVAILABLE = 503 //被kill这种情况或可能的负载过重

  CLIENT_ERROR_CODE_FAILED_TO_OPEN_TABLE = 1     //无法打开表
  CLIENT_ERROR_CODE_FAILED_TO_OPEN_INDEX = 2     //找不到索引 
  CLIENT_ERROR_CODE_FAILED_TO_MISSING_FIELD = 3  //有找不到的字段
  CLIENT_ERROR_CODE_FAILED_TO_MATCH_KEY_NUM = 4  //索引需要的key的数目不对
  CLIENT_ERROR_CODE_FAILED_TO_LOCK_TABLE = 5     //锁表失败
  CLIENT_ERROR_CODE_NOT_ENOUGH_MEMORY = 6        //没有足够的内存
  CLIENT_ERROR_CODE_DECODE_REQUEST_FAILED = 7    //解码请求失败
  CLIENT_ERROR_CODE_FAILED_TO_MISSING_FIELD_IN_FILTER_OR_USE_BLOB = 8 //filter里没有找到对应的字段或字段是Blob类型
  CLIENT_ERROR_CODE_FAILED_TO_COMMIT = 9         //commit失败
  CLIENT_ERROR_CODE_NOT_IMPLEMENTED = 10         //功能没实现
  CLIENT_ERROR_CODE_REQUEST_TIME_OUT = 11        //超时
  CLIENT_ERROR_CODE_UNAUTHENTICATION = 12        //认证失败
  CLIENT_ERROR_CODE_KILLED = 13                  //请求被kill
  CLIENT_ERROR_CODE_THROTTLED = 14               //请求被流控

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

  INSERT = iota
  UPDATE
  DELETE

  MYSQL_TYPE_DECIMAL = 0
  MYSQL_TYPE_TINY = 1
  MYSQL_TYPE_SHORT = 2
  MYSQL_TYPE_LONG = 3
  MYSQL_TYPE_FLOAT = 4
  MYSQL_TYPE_DOUBLE = 5
  MYSQL_TYPE_NULL = 6
  MYSQL_TYPE_TIMESTAMP = 7
  MYSQL_TYPE_LONGLONG = 8
  MYSQL_TYPE_INT24 = 9
  MYSQL_TYPE_DATE = 10
  MYSQL_TYPE_TIME = 11
  MYSQL_TYPE_DATETIME = 12
  MYSQL_TYPE_YEAR = 13
  MYSQL_TYPE_NEWDATE = 14
  MYSQL_TYPE_VARCHAR = 15
  MYSQL_TYPE_BIT = 16
  MYSQL_TYPE_NEWDECIMAL = 246
  MYSQL_TYPE_ENUM = 247
  MYSQL_TYPE_SET = 248
  MYSQL_TYPE_TINY_BLOB = 249
  MYSQL_TYPE_MEDIUM_BLOB = 250
  MYSQL_TYPE_LONG_BLOB = 251
  MYSQL_TYPE_BLOB = 252
  MYSQL_TYPE_VAR_STRING = 253
  MYSQL_TYPE_STRING = 254
  MYSQL_TYPE_GEOMETRY = 255
)

var (
  ClientStatusMessage = map[uint32]string{
    200: "ok",
    202: "accept",
    207: "multi",

    400: "bad request",
    403: "forbidden",
    404: "not found",
    408: "time out",
    500: "server error",
    501: "not implemented",
    502: "database error",
    503: "service unavailable",
  }

  ErrorCodeMessage = map[uint32]string{
    1: "failed to open table",
    2: "failed to open index",
    3: "field missing",
    4: "keys number not match",
    5: "failed to lock table",
    6: "out of memory",
    7: "request decode error",
    8: "field not found in filter, or field type is blob",
    9: "failed to commit",
    10: "not implemented",
    11: "time out",
    12: "need authentication",
    13: "request killed",
    14: "request throttled",
  }
)
