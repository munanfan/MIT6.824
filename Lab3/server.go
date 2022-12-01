package kvraft

const (
	OK        = "OK"
	TIMEOUT   = "TIMEOUT"
	REPEAT    = "REPEAT"
	NOKEY     = "NOKEY"
	NOTLEADER = "NOTLEADER"
	KILLED    = "KILLED"
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
	Sender   int64
	CmdIndex int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sender   int64
	CmdIndex int
}

type GetReply struct {
	Err   Err
	Value string
}
