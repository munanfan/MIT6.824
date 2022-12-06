package kvraft

const (
	OK              = "OK"
	TIMEOUT         = "TIMEOUT"
	REPEAT          = "REPEAT"
	NOKEY           = "NOKEY"
	NOT_LEADER      = "NOTLEADER"
	KILLED          = "KILLED"
	SESSION_EXPIRED = "SESSION_EXPIRED"
)

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	State Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	State    Err
	Response string
}
