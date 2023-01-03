package shardkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"shardctrler"
	"sync"
	"time"
)

// Op 消息，用于在Raft之间传递同步
type Op struct {
	Type          int             // 消息类型
	Key           string          // 键
	Value         string          // 值
	ClientID      int64           // 客户端Id
	SequenceNum   int64           // 客户端对应的命令序号
	ShardId       int             // 消息中包含的Shard数据的Id
	ShardData     []KVData        // Shard数据
	ConfigNum     int             // 请求时发送方的ConfigNum
	RequestRecord map[int64]int64 // 发送SendShard数据时发送方Client对应的最大Sequence序号，用来保证幂等性
}

type KVData struct {
	Key   string
	Value string
}

type OpResult struct {
	SequenceNum int64
	Result      string
	Value       string
}

// ShardKV 数据结构
type ShardKV struct {
	mu           sync.Mutex                     // 锁
	me           int                            // 唯一标识
	rf           *raft.Raft                     // raft实例
	applyCh      chan raft.ApplyMsg             // 与raft层交流的管道
	make_end     func(string) *labrpc.ClientEnd // 用于制作指定server的端点，用于发送RPC请求
	gid          int                            // 自身所属的Group
	ctrlers      []*labrpc.ClientEnd            // 配置控制服务节点列表，用于请求最新的Config
	maxraftstate int                            // raft最大的状态
	killed       bool                           // 标识自己是否被shutdown

	stateMachine  map[string]string     // 状态机，保存key-value数据
	requestChan   map[int]chan OpResult // 每一个未处理完成的请求都对应一个Chan
	requestRecord map[int64]int64       // 记录每一个Client对应最大Sequence序号
	lastCmdIndex  int                   // 当前执行的最大命令序号

	notSend      map[int]bool       // 记录还没有发送的分片序号
	notReceive   map[int]bool       // 记录还没有收到的分片序号
	notDelete    map[int]bool       // 记录还没有删除的分片序号
	config       shardctrler.Config // 记录当前的Config信息
	oldConfig    shardctrler.Config // 记录当前配置的的上一届信息
	ctlClient    *shardctrler.Clerk // 自己的配置查询实例，用于与配置控制节点通信
	shard2KVData map[int][]KVData   // 记录每个shard对应的KV数据，方便Send数据
}

// isLeaderNow
/*
	检查目前是否是leader
*/
func (kv *ShardKV) isLeaderNow() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// Get
/*
	处理客户端发送的Get请求
*/
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// 检查自己是否是Leader
	if !kv.isLeaderNow() {
		reply.Err = ErrWrongLeader
		return
	}
	// 检查请求的Key是否属于自己所处的分组
	requestShardId := key2shard(args.Key)
	if kv.gid != kv.config.Shards[requestShardId] {
		reply.Err = ErrWrongGroup
		return
	}
	// 检查Key是否正在迁移
	if _, ok := kv.notSend[requestShardId]; ok {
		reply.Err = KeyInMoving
		return
	}
	if kv.notReceive[requestShardId] {
		reply.Err = KeyInMoving
		return
	}
	kv.mu.Lock()
	// 检查命令是否已经被执行过
	value, ok := kv.requestRecord[args.ClientId]
	if ok {
		if args.SequenceNum <= value {
			reply.Err = OK
			reply.Value = kv.stateMachine[args.Key]
			kv.mu.Unlock()
			return
		}
	}
	// 构造消息准备发送到Raft层进行同步
	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.ClientID = args.ClientId
	op.SequenceNum = args.SequenceNum
	realIndex, _, _ := kv.rf.Start(op)
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	// 等待处理结果
	select {
	case result := <-resultChan:
		reply.Err = result.Result
		if reply.Err == OK {
			reply.Value = result.Value
		}
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
	kv.mu.Lock()
	delete(kv.requestChan, realIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 检查是否当前是否是Leader
	if !kv.isLeaderNow() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.Op == "Put" || args.Op == "Append" {
		// 当前请求时put或者append这类数据操作请求
		// 检查Key是否属于自己所在组
		requestShardId := key2shard(args.Key)
		if kv.gid != kv.config.Shards[requestShardId] {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		// 检查key是否正在迁移
		if _, ok := kv.notSend[requestShardId]; ok {
			reply.Err = KeyInMoving
			kv.mu.Unlock()
			return
		}
		if kv.notReceive[requestShardId] {
			reply.Err = KeyInMoving
			kv.mu.Unlock()
			return
		}
		// 检查命令是否已经被执行过
		value, ok := kv.requestRecord[args.ClientId]
		if ok {
			if args.SequenceNum <= value {
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
		}
	} else {
		if args.ConfigNum < kv.config.Num {
			// 请求的Config序号小于自己，可能是故障节点在重放日志
			reply.Err = OldConfigNum
			kv.mu.Unlock()
			return
		} else if args.ConfigNum > kv.config.Num {
			// 请求的ConfigNum序号大于自己，可能是自己在故障恢复过程中收到正常节点发送的配置迁移请求
			reply.Err = WrongConfigNum
			kv.mu.Unlock()
			return
		}
	}
	// 构造消息
	op := Op{}
	if args.Op == "Put" {
		op.Type = PUT
		op.Key = args.Key
		op.Value = args.Value
		op.ClientID = args.ClientId
		op.SequenceNum = args.SequenceNum
	} else if args.Op == "Append" {
		op.Type = APPEND
		op.Key = args.Key
		op.Value = args.Value
		op.ClientID = args.ClientId
		op.SequenceNum = args.SequenceNum
	} else if args.Op == "ChangeConfig" {
		op.Type = ChangeConfig
		op.ConfigNum = args.ConfigNum
	} else if args.Op == "GetShard" {
		if kv.notReceive[args.ShardId] == false {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		op.Type = GetShard
		op.ConfigNum = args.ConfigNum
		op.ShardId = args.ShardId
		op.ShardData = args.ShardData
		op.RequestRecord = args.RequestRecord
	} else if args.Op == "DeleteShard" {
		if kv.notDelete[args.ShardId] == false {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		op.Type = DeleteShard
		op.ConfigNum = args.ConfigNum
		op.ShardId = args.ShardId
	} else if args.Op == "SendShard" {
		if kv.notSend[args.ShardId] == false {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		op.Type = SendShard
		op.ConfigNum = args.ConfigNum
		op.ShardId = args.ShardId
	}
	// 将消息发送到Raft层
	realIndex, _, _ := kv.rf.Start(op)
	resultChan := make(chan OpResult)
	kv.requestChan[realIndex] = resultChan
	kv.mu.Unlock()
	select {
	case result := <-resultChan:
		reply.Err = result.Result
	case <-time.After(1 * time.Second):
		// 处理超时
		reply.Err = TIMEOUT
	}
	kv.mu.Lock()
	delete(kv.requestChan, realIndex)
	kv.mu.Unlock()
}

// getShardsMapOwnMe
/*
	从配置信息中获取属于自己的Shard数据
*/
func (kv *ShardKV) getShardsMapOwnMe(config *shardctrler.Config) map[int]bool {
	shards := make(map[int]bool)
	for shardId, gid := range config.Shards {
		if gid == kv.gid {
			shards[shardId] = true
		}
	}
	return shards
}

// applyMessage
/*
	用于处理raft层传递上来的数据
*/
func (kv *ShardKV) applyMessage() {
	for kv.killed == false {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			switch msg.Command.(type) {
			case int:
				// 表示新上任的Leader为了提交不属于自己Term的日志，发送的Empty消息
			case Op:
				cmd := msg.Command.(Op)
				result := OpResult{}
				if cmd.Type == GET || cmd.Type == PUT || cmd.Type == APPEND {
					// 数据操作类请求，进行检测
					requestShardId := key2shard(cmd.Key)
					if kv.gid != kv.config.Shards[requestShardId] {
						result.Result = ErrWrongGroup
					} else if _, ok := kv.notSend[requestShardId]; ok {
						result.Result = KeyInMoving
					} else if kv.notReceive[requestShardId] {
						result.Result = KeyInMoving
					} else {
						// 检测通过，开始执行请求
						result.Result = OK
						op := msg.Command.(Op)
						value, ok := kv.requestRecord[op.ClientID]
						if ok && value >= op.SequenceNum {
							// 已经执行过该请求
							if op.Type == GET {
								result.Value = kv.stateMachine[op.Key]
							}
						} else {
							// 执行请求
							if op.Type == GET {
								result.Value = kv.stateMachine[op.Key]
							} else if op.Type == PUT {
								kv.stateMachine[op.Key] = op.Value
							} else if op.Type == APPEND {
								kv.stateMachine[op.Key] = kv.stateMachine[op.Key] + op.Value
							}
							if value < op.SequenceNum {
								result.SequenceNum = op.SequenceNum
								kv.requestRecord[op.ClientID] = op.SequenceNum
							}
						}
					}
				} else {
					// 配置迁移类数据
					if kv.config.Num == cmd.ConfigNum {
						result.Result = OK
						if cmd.Type == ChangeConfig {
							// 升级config请求
							kv.notSend = make(map[int]bool)
							kv.notDelete = make(map[int]bool)
							kv.notReceive = make(map[int]bool)
							kv.oldConfig = kv.config
							// 请求最新配置
							kv.config = kv.ctlClient.Query(cmd.ConfigNum + 1)
							if kv.config.Num != 1 { // 当ConfigNum=1的时候，各节点都处于初始阶段，没有数据需要发送接收，直接保持默认状态
								kv.shard2KVData = make(map[int][]KVData)
								oldShardsMap := kv.getShardsMapOwnMe(&kv.oldConfig)
								newShardsMap := kv.getShardsMapOwnMe(&kv.config)
								// 计算不属于自己、需要发送给其他组的分片数据
								for oldShardId, _ := range oldShardsMap {
									if _, ok := newShardsMap[oldShardId]; !ok {
										kv.notSend[oldShardId] = true
										kv.notDelete[oldShardId] = true
										kv.shard2KVData[oldShardId] = make([]KVData, 0)
									}
								}
								// 计算属于自己、但是还没接收到的数据
								for newShardId, _ := range newShardsMap {
									if _, ok := oldShardsMap[newShardId]; !ok {
										kv.notReceive[newShardId] = true
									}
								}
								// 根据目前的状态机，计算每个Shard对应的数据，方便后续发送
								for k, v := range kv.stateMachine {
									targetShardId := key2shard(k)
									if kv.notSend[targetShardId] {
										kvData := KVData{}
										kvData.Key = k
										kvData.Value = v
										kv.shard2KVData[targetShardId] = append(kv.shard2KVData[targetShardId], kvData)
									}
								}
							}
						} else if cmd.Type == GetShard {
							// 表示收到了其他节点发送给自己的分片数据
							if kv.notReceive[cmd.ShardId] {
								// 接受key-value数据
								for _, data := range cmd.ShardData {
									kv.stateMachine[data.Key] = data.Value
								}
								// 合并历史执行信息
								for clientId, sequenceNum := range cmd.RequestRecord {
									if kv.requestRecord[clientId] < sequenceNum {
										kv.requestRecord[clientId] = sequenceNum
									}
								}
								// 更改分片状态，表示已经收到
								kv.notReceive[cmd.ShardId] = false
							}
						} else if cmd.Type == DeleteShard {
							// 删除分片操作，在已经发送完分片后执行
							if kv.notDelete[cmd.ShardId] {
								// 删除分片数据
								for _, kvData := range kv.shard2KVData[cmd.ShardId] {
									delete(kv.stateMachine, kvData.Key)
								}
								kv.notDelete[cmd.ShardId] = false
							}
							// 检测是否删除完所有需要删除的数据，用来重置状态等待下次升级
							deleteAll := true
							for _, v := range kv.notDelete {
								if v {
									deleteAll = false
									break
								}
							}
							if deleteAll {
								kv.notDelete = make(map[int]bool)
								kv.notSend = make(map[int]bool)
							}
						} else if cmd.Type == SendShard {
							// 发送分片数据，数据在其它函数中已经发送给其他节点，该消息仅用来保持所有ShardKV保持相同的状态
							if kv.notSend[cmd.ShardId] {
								kv.notSend[cmd.ShardId] = false
							}
						}
					} else if cmd.ConfigNum < kv.config.Num {
						// 请求方的ConfigNum小于自己
						result.Result = OldConfigNum
					} else {
						result.Result = WrongConfigNum
					}
				}
				// 将消息发送到请求对应的chan中，以便响应给请求方
				ch, ok := kv.requestChan[msg.CommandIndex]
				if ok && kv.isLeaderNow() {
					select {
					case ch <- result:
					case <-time.After(20 * time.Millisecond):
					}
				}
				if msg.CommandIndex > kv.lastCmdIndex {
					kv.lastCmdIndex = msg.CommandIndex
				}
			}
		} else if msg.SnapshotValid {
			// 接收到快照信息
			kv.setSnapshot(msg.Snapshot)
			kv.lastCmdIndex = msg.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

// Kill
/*
	shutdown当前节点
*/
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.killed = true
}

// checkAllState
/*
	检测所有的状态，是否发送、接收、删除到相应的分片数据
*/
func (kv *ShardKV) checkAllState() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.notSend == nil || kv.notReceive == nil || kv.notDelete == nil {
		return false
	}
	for _, v := range kv.notSend {
		if v {
			return false
		}
	}
	for _, v := range kv.notReceive {
		if v {
			return false
		}
	}
	for _, v := range kv.notDelete {
		if v {
			return false
		}
	}
	return true
}

// checkAllStateNoLock
/*
	在不加锁的状态下，检查所有状态是否满足
*/
func (kv *ShardKV) checkAllStateNoLock() bool {
	if kv.notSend == nil || kv.notReceive == nil || kv.notDelete == nil {
		return false
	}
	for _, v := range kv.notSend {
		if v {
			return false
		}
	}
	for _, v := range kv.notReceive {
		if v {
			return false
		}
	}
	for _, v := range kv.notDelete {
		if v {
			return false
		}
	}
	return true
}

// allSend
/*
	检查是否已经发送了所有不属于自己的分片数据
*/
func (kv *ShardKV) allSend() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.notSend == nil {
		return false
	}
	for _, v := range kv.notSend {
		if v {
			return false
		}
	}
	return true
}

// allReceive
/*
	检查是否已经收到了所有属于自己但不在自己这里的分片数据
*/
func (kv *ShardKV) allReceive() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.notReceive == nil {
		return false
	}
	for _, v := range kv.notReceive {
		if v {
			return false
		}
	}
	return true
}

// allDelete
/*
	检查是否已经删除了所有不属于自己的分片数据
*/
func (kv *ShardKV) allDelete() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.notDelete == nil {
		return false
	}
	for _, v := range kv.notDelete {
		if v {
			return false
		}
	}
	return true
}

// setSnapshot
/*
	安装快照
*/
func (kv *ShardKV) setSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var stateMachine map[string]string
	var requestRecord map[int64]int64
	var notSend map[int]bool
	var notReceive map[int]bool
	var notDelete map[int]bool
	var oldConfigNum int
	var configNum int
	if decoder.Decode(&stateMachine) != nil ||
		decoder.Decode(&requestRecord) != nil ||
		decoder.Decode(&notSend) != nil ||
		decoder.Decode(&notReceive) != nil ||
		decoder.Decode(&notDelete) != nil ||
		decoder.Decode(&oldConfigNum) != nil ||
		decoder.Decode(&configNum) != nil {
		fmt.Println("---------decode error------------")
		return
	}
	kv.stateMachine = stateMachine
	kv.requestRecord = requestRecord
	kv.notSend = notSend
	kv.notReceive = notReceive
	kv.notDelete = notDelete
	kv.oldConfig = kv.ctlClient.Query(oldConfigNum)
	kv.config = kv.ctlClient.Query(configNum)
	kv.shard2KVData = make(map[int][]KVData)
	for k, v := range kv.stateMachine {
		targetShardId := key2shard(k)
		if kv.notSend[targetShardId] {
			kvData := KVData{}
			kvData.Key = k
			kvData.Value = v
			kv.shard2KVData[targetShardId] = append(kv.shard2KVData[targetShardId], kvData)
		}
	}
}

// makeSnapshot
/*
	制作快照
*/
func (kv *ShardKV) makeSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.stateMachine) != nil ||
		encoder.Encode(kv.requestRecord) != nil ||
		encoder.Encode(kv.notSend) != nil ||
		encoder.Encode(kv.notReceive) != nil ||
		encoder.Encode(kv.notDelete) != nil ||
		encoder.Encode(kv.oldConfig.Num) != nil ||
		encoder.Encode(kv.config.Num) != nil {
		fmt.Println("-----------Encode error----------")
		return nil
	}
	return buffer.Bytes()
}

// checkSnapshot
/*
	定期检查是否需要生成快照
*/
func (kv *ShardKV) checkSnapshot() {
	for kv.killed == false {
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			snapshot := kv.makeSnapshot()
			kv.rf.Snapshot(kv.lastCmdIndex, snapshot)
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// checkConfig
/*
	定期检查是否有新的配置需要升级
*/
func (kv *ShardKV) checkConfig() {
	for kv.killed == false {
		if kv.isLeaderNow() {
			if kv.checkAllState() {
				currentConfig := kv.config.Num
				newConfig := kv.ctlClient.Query(currentConfig + 1)
				if newConfig.Num == kv.config.Num+1 {
					args := PutAppendArgs{}
					args.Op = "ChangeConfig"
					args.ConfigNum = currentConfig
					reply := PutAppendReply{}
					for kv.killed == false {
						kv.PutAppend(&args, &reply)
						if reply.Err == OK {
							break
						} else if reply.Err == OldConfigNum || reply.Err == ErrWrongLeader || reply.Err == WrongConfigNum {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
			}
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// sendShard
/*
	发送不属于自己的分片数据
*/
func (kv *ShardKV) sendShard() {
	for kv.killed == false {
		if kv.isLeaderNow() {
			if kv.allSend() == false {
				configNum := kv.config.Num
				for shardId, v := range kv.notSend {
					if v {
						var targetGid int
						if kv.config.Num == configNum {
							targetGid = kv.config.Shards[shardId]
						}
						if targetGid == kv.gid {
							break
						}
						getShardArgs := PutAppendArgs{}
						getShardArgs.Op = "GetShard"
						getShardArgs.ConfigNum = configNum
						getShardArgs.ShardId = shardId
						getShardArgs.ShardData = kv.shard2KVData[shardId]
						getShardArgs.RequestRecord = kv.requestRecord
						getShardReply := PutAppendReply{}
						for _, serverName := range kv.config.Groups[targetGid] { // 尝试分片所属的组中所有的server
							if kv.killed {
								break
							}
							clientEnd := kv.make_end(serverName)
							status := clientEnd.Call("ShardKV.PutAppend", &getShardArgs, &getShardReply)
							if status {
								if getShardReply.Err == OK || getShardReply.Err == OldConfigNum {
									// send success message to raft
									sendShardArgs := PutAppendArgs{}
									sendShardArgs.Op = "SendShard"
									sendShardArgs.ConfigNum = configNum
									sendShardArgs.ShardId = shardId
									sendShardReply := PutAppendReply{}
									for {
										kv.PutAppend(&sendShardArgs, &sendShardReply)
										if sendShardReply.Err == OK {
											break
										} else if sendShardReply.Err == ErrWrongLeader || sendShardReply.Err == OldConfigNum {
											break
										}
										time.Sleep(10 * time.Microsecond)
									}
									break
								} else if getShardReply.Err == WrongConfigNum {
									break
								} else {
									// timeout
									continue
								}
							} else {
								continue
							}
						}
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// checkGC
/*
	垃圾回收，检查是否有已经发送的分片数据需要删除
*/
func (kv *ShardKV) checkGC() {
	for kv.killed == false {
		if kv.isLeaderNow() {
			configNum := kv.config.Num
			kv.mu.Lock()
			copyNotSend := make(map[int]bool)
			for k, v := range kv.notSend {
				copyNotSend[k] = v
			}
			kv.mu.Unlock()
			for shardId, v := range copyNotSend {
				if v == false && kv.notDelete[shardId] {
					deleteArgs := PutAppendArgs{}
					deleteArgs.Op = "DeleteShard"
					deleteArgs.ConfigNum = configNum
					deleteArgs.ShardId = shardId
					deleteReply := PutAppendReply{}
					for kv.killed == false {
						kv.PutAppend(&deleteArgs, &deleteReply)
						if deleteReply.Err == OK {
							break
						} else if deleteReply.Err == ErrWrongLeader || deleteReply.Err == OldConfigNum {
							break
						} else if deleteReply.Err == TIMEOUT {
							continue
						}
						// Timeout
					}
					if kv.killed {
						break
					}
					if deleteReply.Err == ErrWrongLeader {
						continue
					} else {
						break
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// StartServer
/*
	创建并开启ShardServer
*/
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.mu = sync.Mutex{}
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.killed = false
	kv.lastCmdIndex = 0

	kv.ctlClient = shardctrler.MakeClerk(kv.ctrlers)
	kv.requestRecord = make(map[int64]int64)
	kv.stateMachine = make(map[string]string)
	kv.requestChan = make(map[int]chan OpResult)
	kv.shard2KVData = nil
	kv.notSend = make(map[int]bool)
	kv.notReceive = make(map[int]bool)
	kv.notDelete = make(map[int]bool)
	kv.config = kv.ctlClient.Query(0)
	kv.oldConfig = kv.config

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.setSnapshot(persister.ReadSnapshot())

	go kv.checkConfig()
	go kv.sendShard()
	go kv.applyMessage()
	go kv.checkGC()
	if kv.maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	return kv
}
