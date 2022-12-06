package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
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
	Type        int
	Key         string
	Value       string
	ClientID    int64
	SequenceNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine  map[string]string
	requestChan   map[int]chan OpResult
	requestRecord map[int64]OpResult
	lastCmdIndex  int
}

type OpResult struct {
	SequenceNum int64
	Result      string
}

func (kv *KVServer) setSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	fmt.Println("kvserver => setSnapshot")
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var stateMachine map[string]string
	var requestRecord map[int64]OpResult
	if decoder.Decode(&stateMachine) != nil ||
		decoder.Decode(&requestRecord) != nil {
		fmt.Println("---------decode error------------")
		return
	}
	kv.stateMachine = stateMachine
	for k, v := range kv.stateMachine {
		fmt.Println("I'm kvserver ", kv.me, ",receive snapshot key is ", k, ",value is ", v)
	}
	kv.requestRecord = requestRecord
}

func (kv *KVServer) makeSnapshot() []byte {
	fmt.Println("server ", kv.me, ",make a snapshot,lastCmdIndex ", kv.lastCmdIndex)
	for k, v := range kv.stateMachine {
		fmt.Println("server ", kv.me, ",make a snapshot, key is ", k, ",value is ", v)
	}
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.stateMachine) != nil ||
		encoder.Encode(kv.requestRecord) != nil {
		fmt.Println("-----------Encode error----------")
		return nil
	}
	return buffer.Bytes()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	fmt.Println("I'm kvserver ", kv.me, ",receive a Get request, key is ", args.Key)
	// check the leader state
	_, isLeader := kv.rf.GetState()
	fmt.Println("I'm kvserver ", kv.me, ",kv.rf.GetState invoke")
	if isLeader == false {
		fmt.Println("I'm kvserver ", kv.me, ",receive a Get request, key is ", args.Key, ", not leader")
		reply.State = NOT_LEADER
		return
	}
	kv.mu.Lock()
	fmt.Println("kvserver ", kv.me, ",Get get the lock")
	// check is already executed?
	value, ok := kv.requestRecord[args.ClientId]
	if ok {
		if args.SequenceNum <= value.SequenceNum {
			reply.State = OK
			reply.Response = kv.stateMachine[args.Key]
			fmt.Println("I'm kvserver ", kv.me, ",receive a Get request, key is ", args.Key, ",repeat execute, response is ", reply.Response)
			kv.mu.Unlock()
			return
		}
	}
	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.ClientID = args.ClientId
	op.SequenceNum = args.SequenceNum
	// send command to raft
	fmt.Println("I'm kvserver ", kv.me, ",receive a Get request, key is ", args.Key, ",send to raft")
	realIndex, _, _ := kv.rf.Start(op)
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	fmt.Println("kvserver ", kv.me, ",wait for the get result from chan")
	select {
	case result := <-resultChan:
		// check is leader?
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			fmt.Println("I'm kvserver ", kv.me, ",receive the get reply , key is ", args.Key, "value is ", reply.Response, ",not leader..................")
			reply.State = NOT_LEADER
			return
		}
		reply.Response = result.Result
		reply.State = OK
		fmt.Println("I'm kvserver ", kv.me, ",receive the get reply , key is ", args.Key, "value is ", reply.Response, ",ok..................")
	case <-time.After(200 * time.Millisecond):
		reply.State = TIMEOUT
		fmt.Println("TIMEOUT, key ", op.Key)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	fmt.Println("I'm kvserver ", kv.me, ",receive a PutAppend request, key is ", args.Key, ",value is ", args.Value)
	// check the leader state
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		fmt.Println("I'm kvserver ", kv.me, ",receive a PutAppend request, key is ", args.Key, ", not leader")
		reply.State = NOT_LEADER
		return
	}
	kv.mu.Lock()
	fmt.Println("kvserver ", kv.me, ",putAppend get the lock")
	// check is already executed?
	value, ok := kv.requestRecord[args.ClientId]
	if ok {
		if args.SequenceNum <= value.SequenceNum {
			fmt.Println("I'm kvserver ", kv.me, ",receive a PutAppend request, key is ", args.Key, ",repeat execute")
			reply.State = OK
			kv.mu.Unlock()
			return
		}
	}
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
	// send command to raft
	realIndex, _, _ := kv.rf.Start(op)
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	fmt.Println("kvserver ", kv.me, ",wait for the putAppend result from chan")
	select {
	case <-resultChan:
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			fmt.Println("I'm kvserver ", kv.me, ",receive the get reply , key is ", args.Key, ",not leader..................")
			reply.State = NOT_LEADER
			return
		}
		reply.State = OK
		fmt.Println("I'm kvserver ", kv.me, ",receive the reply , key is ", args.Key)
	case <-time.After(100 * time.Millisecond):
		reply.State = TIMEOUT
		fmt.Println("TIMEOUT")
	}
}

func (kv *KVServer) checkSnapshot() {
	for kv.killed() == false {
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			snapshot := kv.makeSnapshot()
			kv.rf.Snapshot(kv.lastCmdIndex, snapshot)
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) applyMessage() {
	for kv.killed() == false {
		fmt.Println("I'm kvserver ", kv.me, ",prepare receive a msg from raft")
		msg := <-kv.applyCh
		fmt.Println("I'm kvserver ", kv.me, ",receive a msg from raft, commandIndex is ", msg.CommandIndex)
		if msg.CommandValid {
			kv.mu.Lock()
			result := OpResult{}
			op := msg.Command.(Op)
			fmt.Println("clientID ", op.ClientID, ",sequenceNum ", op.SequenceNum, ",key is ", op.Key, ",value is ", op.Value)
			value, ok := kv.requestRecord[op.ClientID]
			if ok && value.SequenceNum >= op.SequenceNum {
				// already execute
				if op.Type == GET {
					fmt.Println("already apply get request, key is ", op.Key, ",result is ", kv.stateMachine[op.Key])
					result.Result = kv.stateMachine[op.Key]
				}
			} else {
				if op.Type == GET {
					fmt.Println("apply get request, key is ", op.Key, ",result is ", kv.stateMachine[op.Key])
					result.Result = kv.stateMachine[op.Key]
				} else if op.Type == PUT {
					fmt.Println("set put request, key is ", op.Key, ",value is ", op.Value)
					kv.stateMachine[op.Key] = op.Value
				} else if op.Type == APPEND {
					fmt.Println("set append request, key is ", op.Key, ",value is ", kv.stateMachine[op.Key]+op.Value)
					kv.stateMachine[op.Key] = kv.stateMachine[op.Key] + op.Value
				}
				if value.SequenceNum < op.SequenceNum {
					fmt.Println("save requestRecord")
					result.SequenceNum = op.SequenceNum
					kv.requestRecord[op.ClientID] = result
				}
			}
			ch, ok := kv.requestChan[msg.CommandIndex]
			if ok {
				fmt.Println("send result to resultChan")
				go func(ch chan OpResult, result OpResult) {
					ch <- result
				}(ch, result)
				// kazhule
			}
			if msg.CommandIndex > kv.lastCmdIndex {
				kv.lastCmdIndex = msg.CommandIndex
			}
			fmt.Println("CommandValid: prepare lose lock")
			kv.mu.Unlock()
			fmt.Println("CommandValid: lose lock")
		} else if msg.SnapshotValid {
			fmt.Println("read snapshot from raft, last snapshotIndex ", msg.SnapshotIndex)
			kv.mu.Lock()
			kv.setSnapshot(msg.Snapshot)
			kv.lastCmdIndex = msg.SnapshotIndex
			kv.mu.Unlock()
		}
	}

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

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
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
	kv.lastCmdIndex = 0

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.requestRecord = make(map[int64]OpResult)
	kv.stateMachine = make(map[string]string)
	kv.requestChan = make(map[int]chan OpResult)

	kv.setSnapshot(persister.ReadSnapshot())

	go kv.applyMessage()
	if kv.maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	return kv
}
