package kvraft

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Op 用于封装命令信息的数据结构
type Op struct {
	Type        int    // 消息类型
	Key         string // Client请求操作的Key
	Value       string // Client请求操作的value
	ClientID    int64  // 发送请求的客户端Id
	SequenceNum int64  // 发送请求的客户端命令序号
}

// KVServer KV所需要的信息
type KVServer struct {
	mu      sync.Mutex         // 锁
	me      int                // 自身的编号，也可以当做唯一标识
	rf      *raft.Raft         // 自己所拥有的Raft实例
	applyCh chan raft.ApplyMsg // 消息通道，用于Raft和KV Server的交流
	dead    int32              // 当前KV Server实例是否存活

	maxraftstate int // Raft的最大状态大小，如果大于该值，需要进行快照以节约空间

	stateMachine  map[string]string     // 状态机，用于维护Key-Value数据
	requestChan   map[int]chan OpResult // 请求时产生的Chan，用于发送raft的执行结果到请求函数中
	requestRecord map[int64]OpResult    // 记录每一个client的最大执行Sequence，用于保证请求的幂等性
	lastCmdIndex  int                   // 已经执行的最大命令序号
}

// OpResult 用于封装请求执行结果
type OpResult struct {
	SequenceNum int64  // Client请求某个命令时生成的Sequence序号
	Result      string // 请求执行结果
}

// setSnapshot
/*
	安装从Raft层传递来的快照
*/
func (kv *KVServer) setSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		// 快照大小为空，直接返回
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var stateMachine map[string]string
	var requestRecord map[int64]OpResult
	if decoder.Decode(&stateMachine) != nil ||
		decoder.Decode(&requestRecord) != nil {
		//fmt.Println("---------decode error------------")
		return
	}
	kv.stateMachine = stateMachine
	kv.requestRecord = requestRecord
}

// makeSnapshot
/*
	根据目前状态机的数据以及持久化所需要的数据，生成快照
*/
func (kv *KVServer) makeSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.stateMachine) != nil ||
		encoder.Encode(kv.requestRecord) != nil {
		//fmt.Println("-----------Encode error----------")
		return nil
	}
	return buffer.Bytes()
}

// Get
/*
	用于处理客户端发送来的Get请求
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 检查是否为Leader，只有Leader能处理客户端发送的消息
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.State = NotLeader
		return
	}
	kv.mu.Lock()
	// 根据已经缓存的Client -> Sequence，判断是否已经执行过当前命令
	value, ok := kv.requestRecord[args.ClientId]
	if ok {
		if args.SequenceNum <= value.SequenceNum {
			reply.State = OK
			reply.Response = kv.stateMachine[args.Key]
			kv.mu.Unlock()
			return
		}
	}
	// 生成消息传递给Raft层
	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.ClientID = args.ClientId
	op.SequenceNum = args.SequenceNum
	// 发送给raft，以同步当前命令到其他raft节点
	realIndex, _, _ := kv.rf.Start(op)
	// 创建Chan，用来接受之后的请求结果
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	select {
	case result := <-resultChan:
		// 收到处理结果，再次执行Leader检查
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.State = NotLeader
			return
		}
		reply.Response = result.Result
		reply.State = OK
	case <-time.After(1 * time.Second):
		// 执行时间超时，直接返回结果
		reply.State = TIMEOUT
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 检查是否为Leader，只有Leader能处理客户端发送的消息
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.State = NotLeader
		return
	}
	kv.mu.Lock()
	// 根据已经缓存的Client -> Sequence，判断是否已经执行过当前命令
	value, ok := kv.requestRecord[args.ClientId]
	if ok {
		if args.SequenceNum <= value.SequenceNum {
			reply.State = OK
			kv.mu.Unlock()
			return
		}
	}
	// 生成用于传递给Raft的消息格式
	op := Op{}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	op.Key = args.Key
	op.Value = args.Value
	op.ClientID = args.ClientId
	op.SequenceNum = args.SequenceNum
	// 将消息发送给自身的Raft，已同步消息到大部分节点
	realIndex, _, _ := kv.rf.Start(op)
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	select {
	case <-resultChan:
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.State = NotLeader
			return
		}
		reply.State = OK
	case <-time.After(1 * time.Second):
		reply.State = TIMEOUT
	}
}

// checkSnapshot
/*
	检查快照
*/
func (kv *KVServer) checkSnapshot() {
	for kv.killed() == false {
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			// 当前Raft的状态信息大于阈值，进行快照操作
			kv.mu.Lock()
			snapshot := kv.makeSnapshot()
			kv.rf.Snapshot(kv.lastCmdIndex, snapshot)
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// applyMessage
/*
	执行从Raft提交上来命令或者快照
	当Raft将消息同步到大部分的节点的时候，就将消息一次提交到channel中
	KV Server层面接收到的消息，都已经同步到大部分的节点。
*/
func (kv *KVServer) applyMessage() {
	for kv.killed() == false { // 检查当前KV Server实例是否存活
		msg := <-kv.applyCh
		if msg.CommandValid {
			switch msg.Command.(type) {
			case int:
				// int类型消息，代表是当有Raft成为Leader时，为了提交之前Term的消息，发送的Empty消息
			case Op:
				kv.mu.Lock()
				result := OpResult{}
				op := msg.Command.(Op)
				/*
					检查命令是否已经执行
					如果已经执行，直接返回数据而无需执行操作
				*/
				value, ok := kv.requestRecord[op.ClientID]
				if ok && value.SequenceNum >= op.SequenceNum {
					// 已经执行过
					if op.Type == GET {
						result.Result = kv.stateMachine[op.Key]
					}
				} else {
					// 未执行过命令，开始执行命令
					if op.Type == GET {
						result.Result = kv.stateMachine[op.Key]
					} else if op.Type == PUT {
						kv.stateMachine[op.Key] = op.Value
					} else if op.Type == APPEND {
						kv.stateMachine[op.Key] = kv.stateMachine[op.Key] + op.Value
					}
					if value.SequenceNum < op.SequenceNum {
						// 更新该client对应的Sequence序号
						result.SequenceNum = op.SequenceNum
						kv.requestRecord[op.ClientID] = result
					}
				}
				// 发送执行结果到chan中，Get/Put函数会受到此消息
				ch, ok := kv.requestChan[msg.CommandIndex]
				if ok {
					go func(ch chan OpResult, result OpResult) {
						ch <- result
					}(ch, result)
				}
				if msg.CommandIndex > kv.lastCmdIndex {
					kv.lastCmdIndex = msg.CommandIndex
				}
				kv.mu.Unlock()
			}
		} else if msg.SnapshotValid {
			// Raft发送的是快照，执行安装快照操作
			kv.mu.Lock()
			kv.setSnapshot(msg.Snapshot)
			kv.lastCmdIndex = msg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

// Kill
/*
	模型shutdown当前Server
*/
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

// killed
/*
	判断是否shutdown当前Server
*/
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
/*
	用于初始化KVRaftServer，初始化所有变量
*/
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.mu = sync.Mutex{}
	kv.lastCmdIndex = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.requestRecord = make(map[int64]OpResult)
	kv.stateMachine = make(map[string]string)
	kv.requestChan = make(map[int]chan OpResult)
	// 安装已经持久化的快照以快速恢复信息
	kv.setSnapshot(persister.ReadSnapshot())

	go kv.applyMessage()
	// 如果设置了raft的状态大小阈值，开启快照检测
	if kv.maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	return kv
}
