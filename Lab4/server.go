package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	replyChan     map[int]chan OpResult
	requestRecord map[int64]int64
	killed        bool

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientId    int64
	SequenceNum int64
	Type        int
	// Join Data
	Servers map[int][]string
	// Leave Data
	GIDs []int
	// Move Data
	Move MoveData
	// Query Data
	Num int
}

type OpResult struct {
	status bool
	Result interface{}
}

type MoveData struct {
	Shard int
	GID   int
}

func (sc *ShardCtrler) showAllotInfo() {
	for i, v := range sc.configs[len(sc.configs)-1].Shards {
		fmt.Println("shard ", i, " => server ", v)
	}
	fmt.Println("------------------------")
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// check Leader
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	// make operation
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = JOIN
	op.Servers = make(map[int][]string, len(args.Servers))
	for k, v := range args.Servers {
		serverList := make([]string, len(v))
		copy(serverList, v)
		op.Servers[k] = serverList
	}
	sc.mu.Lock()
	realIndex, _, _ := sc.rf.Start(op)
	// make a chan to wait replt
	sc.replyChan[realIndex] = make(chan OpResult)
	sc.mu.Unlock()
	select {
	case <-sc.replyChan[realIndex]:
		reply.Err = OK
		delete(sc.replyChan, realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// check Leader
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	// make operation
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = LEAVE
	op.GIDs = args.GIDs
	sc.mu.Lock()
	realIndex, _, _ := sc.rf.Start(op)
	// make a chan to wait replt
	sc.replyChan[realIndex] = make(chan OpResult)
	sc.mu.Unlock()
	select {
	case <-sc.replyChan[realIndex]:
		reply.Err = OK
		delete(sc.replyChan, realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// check Leader
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	// make operation
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = MOVE
	moveData := MoveData{}
	moveData.GID = args.GID
	moveData.Shard = args.Shard
	op.Move = moveData
	sc.mu.Lock()
	realIndex, _, _ := sc.rf.Start(op)
	// make a chan to wait replt
	sc.replyChan[realIndex] = make(chan OpResult)
	sc.mu.Unlock()
	select {
	case <-sc.replyChan[realIndex]:
		reply.Err = OK
		delete(sc.replyChan, realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// check Leader
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	// make operation
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = QUERY
	op.Num = args.Num
	sc.mu.Lock()
	realIndex, _, _ := sc.rf.Start(op)
	// make a chan to wait replt
	sc.replyChan[realIndex] = make(chan OpResult)
	sc.mu.Unlock()
	select {
	case <-sc.replyChan[realIndex]:
		reply.Err = OK
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		delete(sc.replyChan, realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) allotShard(notAllot *[]int, groups *map[int][]string, shards *[10]int) {
	if len(*groups) == 0 {
		for i, _ := range shards {
			shards[i] = 0
		}
		return
	}
	index := 0
	count := make(map[int]int)
	for _, v := range shards {
		if v != 0 {
			count[v]++
		}
	}
	averageNum := 0
	if len(*groups) != 0 {
		if len(*groups) > len(shards) {
			averageNum = 1
		} else {
			averageNum = len(shards) / len(*groups)
		}
	}
	// get the groupId and sort
	groupIds := make([]int, len(*groups))
	mapIndex := 0
	for k, _ := range *groups {
		groupIds[mapIndex] = k
		mapIndex++
	}
	sort.Ints(groupIds)
	for _, k := range groupIds {
		flag := false
		for count[k] < averageNum {
			if index < len(*notAllot) {
				shards[(*notAllot)[index]] = k
				count[k]++
				index++
			} else {
				flag = true
				break
			}
		}
		if flag {
			break
		}
	}
	if index < len(*notAllot) {
		for _, k := range groupIds {
			if index < len(*notAllot) {
				shards[(*notAllot)[index]] = k
				count[k]++
				index++
			} else {
				break
			}
		}
	}
}

func (sc *ShardCtrler) loadBalance(cmdType int, data interface{}) {
	nowConfig := sc.configs[len(sc.configs)-1]
	op := data.(Op)
	if cmdType == JOIN {
		// Join message
		newServers := op.Servers
		// make a new config
		newConfig := Config{}
		newConfig.Groups = make(map[int][]string, len(nowConfig.Groups)+len(newServers))
		newConfig.Shards = [10]int{}
		newConfig.Num = nowConfig.Num + 1
		for k, v := range nowConfig.Groups {
			serverList := make([]string, len(v))
			copy(serverList, v)
			newConfig.Groups[k] = v
		}
		for k, v := range newServers {
			serverList := make([]string, len(v))
			copy(serverList, v)
			newConfig.Groups[k] = serverList
		}
		averageNum := int(float64(len(nowConfig.Shards)) / float64(len(newConfig.Groups)))
		count := make(map[int]int)
		notAllot := make([]int, 0)
		for i, v := range nowConfig.Shards {
			if v != 0 && count[v] < averageNum {
				newConfig.Shards[i] = v
				count[v]++
			} else {
				notAllot = append(notAllot, i)
				newConfig.Shards[i] = 0
			}
		}
		sc.allotShard(&notAllot, &newConfig.Groups, &newConfig.Shards)
		sc.configs = append(sc.configs, newConfig)
	} else if cmdType == QUERY {
		return
	} else if cmdType == LEAVE {
		// make new Config
		newConfig := Config{}
		newConfig.Groups = make(map[int][]string, len(nowConfig.Groups)-len(op.GIDs))
		newConfig.Shards = [10]int{}
		newConfig.Num = nowConfig.Num + 1
		// Leave message
		leaveGroupId := op.GIDs
		record := make(map[int]bool)
		for _, v := range leaveGroupId {
			record[v] = true
		}
		for k, v := range nowConfig.Groups {
			_, ok := record[k]
			if ok == false {
				serverList := make([]string, len(v))
				copy(serverList, v)
				newConfig.Groups[k] = serverList
			}
		}
		notAllot := make([]int, 0)
		for i, v := range nowConfig.Shards {
			_, ok := record[v]
			if ok || v == 0 {
				notAllot = append(notAllot, i)
				newConfig.Shards[i] = 0
			} else {
				newConfig.Shards[i] = v
			}
		}
		sc.allotShard(&notAllot, &newConfig.Groups, &newConfig.Shards)
		sc.configs = append(sc.configs, newConfig)
	} else {
		// make new Config
		newConfig := Config{}
		newConfig.Groups = make(map[int][]string, len(nowConfig.Groups))
		newConfig.Shards = [10]int{}
		newConfig.Num = nowConfig.Num + 1
		// MOVE Message
		moveData := op.Move
		for k, v := range nowConfig.Groups {
			serverList := make([]string, len(v))
			copy(serverList, v)
			newConfig.Groups[k] = serverList
		}
		for i, v := range nowConfig.Shards {
			newConfig.Shards[i] = v
		}
		newConfig.Shards[moveData.Shard] = moveData.GID
		sc.configs = append(sc.configs, newConfig)
	}
}

func (sc *ShardCtrler) checkMessage() {
	for sc.killed == false {
		msg := <-sc.applyCh
		switch msg.Command.(type) {
		case string:
			// empty msg ,don't need proceed
		case Op:
			cmd := msg.Command.(Op)
			// check isExecuted?
			sc.mu.Lock()
			value, ok := sc.requestRecord[cmd.ClientId]
			if !ok || value < cmd.SequenceNum {
				// not executed
				sc.loadBalance(cmd.Type, msg.Command)
				sc.requestRecord[cmd.ClientId] = cmd.SequenceNum
			}
			sc.showAllotInfo()
			ch, ok := sc.replyChan[msg.CommandIndex]
			if ok {
				result := OpResult{}
				result.status = true
				if cmd.Type == QUERY {
					result.Result = sc.configs[len(sc.configs)-1]
				}
				ch <- result
			}
			sc.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.killed = true
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.mu = sync.Mutex{}

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.killed = false

	// Your code here.
	sc.replyChan = make(map[int]chan OpResult)
	sc.requestRecord = make(map[int64]int64)

	go sc.checkMessage()

	return sc
}
