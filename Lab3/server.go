package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"strconv"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   int // 0 => get; 1 => put; 2 => append
	Key    string
	Value  string
	Sender int64
	Index  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// state machine =>  save the command result of client
	stateMachine map[string]string
	// record the request record of client
	requestRecord map[string]int
}

func (kv *KVServer) checkLeader() bool {
	_, leader := kv.rf.GetState()
	return leader
}

func (kv *KVServer) Get(args GetArgs, reply *GetReply) {
	// 检查是否被kill
	if kv.killed() {
		reply.Err = KILLED
		return
	}
	// Your code here.
	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.Sender = args.Sender
	op.Index = args.CmdIndex
	key := strconv.FormatInt(op.Sender, 10) + "|" + strconv.Itoa(op.Index)
	kv.mu.Lock()
	_, ok := kv.requestRecord[key]
	if ok {
		reply.Err = OK
		reply.Value = kv.stateMachine[op.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	_, _, leader := kv.rf.Start(op)
	if leader == false {
		reply.Err = NOTLEADER
		return
	}
	// 等待Raft提交
	startTime := time.Now().UnixMilli()
	for time.Now().UnixMilli()-startTime < 100 {
		kv.mu.Lock()
		_, ok = kv.requestRecord[key]
		if ok {
			reply.Err = OK
			reply.Value = kv.stateMachine[op.Key]
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	reply.Err = TIMEOUT
}

func (kv *KVServer) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = KILLED
		return
	}
	// Your code here.
	op := Op{}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	op.Key = args.Key
	op.Value = args.Value
	op.Sender = args.Sender
	op.Index = args.CmdIndex
	key := strconv.FormatInt(op.Sender, 10) + "|" + strconv.Itoa(op.Index)
	// check whether commit
	kv.mu.Lock()
	_, ok := kv.requestRecord[key]
	if ok {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	_, _, leader := kv.rf.Start(op)
	if leader == false {
		reply.Err = NOTLEADER
		return
	}
	// 等待Raft提交
	startTime := time.Now().UnixMilli()
	for time.Now().UnixMilli()-startTime < 100 {
		kv.mu.Lock()
		_, ok = kv.requestRecord[key]
		if ok {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	reply.Err = TIMEOUT
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handlerMsg() {
	for kv.killed() == false {
		// 从applyCH中获取信息
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		key := strconv.FormatInt(op.Sender, 10) + "|" + strconv.Itoa(op.Index)
		kv.mu.Lock()
		_, state := kv.requestRecord[key]
		if state == false {
			// 没有执行过
			if op.Type == PUT {
				kv.stateMachine[op.Key] = op.Value
			} else if op.Type == APPEND {
				kv.stateMachine[op.Key] = kv.stateMachine[op.Key] + op.Value
			}
			kv.requestRecord[key] = 1
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the Index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.mu = sync.Mutex{}
	kv.dead = -1

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = make(map[string]string)
	kv.requestRecord = make(map[string]int)

	go kv.handlerMsg()

	return kv
}
