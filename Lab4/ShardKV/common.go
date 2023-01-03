package shardkv

const (
	OK             = "OK"             // 成功响应
	ErrNoKey       = "ErrNoKey"       // 数据库中没有当前key
	ErrWrongGroup  = "ErrWrongGroup"  // 请求的数据不在当前Group内
	ErrWrongLeader = "ErrWrongLeader" // 请求的节点不是Leader节点
	TIMEOUT        = "TIMEOUT"        // 操作超时
	WrongConfigNum = "WrongConfigNum" // 请求的Config出错
	OldConfigNum   = "OldConfigNum"   // 说明请求方的ConfigNum小于自己，可能是故障的节点正在重放日志以恢复数据
	KeyInMoving    = "KeyInMoving"    // 请求的Key正在迁移
)

// 消息类型
const (
	GET          = 1 // Get请求 => 获取Key对应的Value
	PUT          = 2 // Put请求 => 新增Key-Value
	APPEND       = 3 // Append请求 => 在Key对应的Value后面增加数据
	GetShard     = 4 // GetShard请求 => 用于接受其他ShardKV发送来的分片数据
	DeleteShard  = 5 // DeleteShard请求 => 用于ShardKV发送分片数据后进行垃圾挥手
	ChangeConfig = 6 // ChangeConfig请求 => 当检查到新的Config之后进行的升级请求
	SendShard    = 8 // SendShard => 当升级到新的Config之后，准备发送不属于自己的分片数据
)

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key           string          // 键
	Value         string          // 值
	Op            string          // 发送的命令类型
	ClientId      int64           // 客户端Id
	SequenceNum   int64           // 命令序号
	ConfigNum     int             // 请求方当前的配置序号
	ShardId       int             // 消息中包含分片数据的id
	ShardData     []KVData        // 分片数据
	RequestRecord map[int64]int64 // 在SendShard的时候，也需要将自己处理过每个Client对应的最大Sequence同步给接收方，来保证幂等性
}

type PutAppendReply struct {
	Err string
}

type SyncMsgReply struct {
	Err string
}

type GetArgs struct {
	Key         string
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   string
	Value string
}
