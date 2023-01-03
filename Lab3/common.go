package kvraft

const (
	OK        = "Ok"        // 响应成功
	TIMEOUT   = "Timeout"   // 超时
	REPEAT    = "Repeat"    // 重复执行
	NotLeader = "NotLeader" // 非Leader
	KILLED    = "Killed"    // 节点被杀死
)

const (
	GET    = 1 // Get请求
	PUT    = 2 // Put请求
	APPEND = 3 // Append请求
)

type Err string

type PutAppendArgs struct {
	Key         string // 键名
	Value       string // 键值
	Op          string // 操作类型
	ClientId    int64  // 客户端的唯一身份标识
	SequenceNum int64  // 客户端的请求序号，用于服务端保证幂等性
}

type PutAppendReply struct {
	State Err // 请求状态
}

type GetArgs struct {
	Key         string // 键名
	ClientId    int64  // 客户端的唯一身份标识
	SequenceNum int64  // 客户端的请求序号，用于服务端保证幂等性
}

type GetReply struct {
	State    Err    // 请求状态
	Response string // 响应结果
}
