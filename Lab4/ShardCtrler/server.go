package shardctrler

import (
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	replyChan     map[int]chan OpResult
	requestRecord map[int64]int64
	killed        bool

	configs []Config
}

type Op struct {
	ClientId    int64
	SequenceNum int64
	Type        int
	Servers     map[int][]string
	GIDs        []int
	Move        MoveData
	Num         int
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
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
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
	realIndex, _, _ := sc.rf.Start(op)
	ch := make(chan OpResult)
	sc.mu.Lock()
	sc.replyChan[realIndex] = ch
	sc.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		go func(index int) {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			delete(sc.replyChan, index)
		}(realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = LEAVE
	op.GIDs = args.GIDs
	realIndex, _, _ := sc.rf.Start(op)
	ch := make(chan OpResult)
	sc.mu.Lock()
	sc.replyChan[realIndex] = ch
	sc.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		go func(index int) {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			delete(sc.replyChan, index)
		}(realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = MOVE
	moveData := MoveData{}
	moveData.GID = args.GID
	moveData.Shard = args.Shard
	op.Move = moveData
	realIndex, _, _ := sc.rf.Start(op)
	ch := make(chan OpResult)
	sc.mu.Lock()
	sc.replyChan[realIndex] = ch
	sc.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		go func(index int) {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			delete(sc.replyChan, index)
		}(realIndex)
	case <-time.After(1 * time.Second):
		reply.Err = TIMEOUT
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.Err = NOT_LEADER
		return
	}
	op := Op{}
	op.ClientId = args.ClientId
	op.SequenceNum = args.SequenceNum
	op.Type = QUERY
	op.Num = args.Num
	realIndex, _, _ := sc.rf.Start(op)
	ch := make(chan OpResult)
	sc.mu.Lock()
	sc.replyChan[realIndex] = ch
	sc.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		go func(index int) {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			delete(sc.replyChan, index)
		}(realIndex)
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
		newServers := op.Servers
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
		//fmt.Println("Solve Query Request")
		return
	} else if cmdType == LEAVE {
		newConfig := Config{}
		newConfig.Groups = make(map[int][]string, len(nowConfig.Groups)-len(op.GIDs))
		newConfig.Shards = [10]int{}
		newConfig.Num = nowConfig.Num + 1
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
		newConfig := Config{}
		newConfig.Groups = make(map[int][]string, len(nowConfig.Groups))
		newConfig.Shards = [10]int{}
		newConfig.Num = nowConfig.Num + 1
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
		case int:
		case Op:
			cmd := msg.Command.(Op)
			value, ok := sc.requestRecord[cmd.ClientId]
			if !ok || value < cmd.SequenceNum {
				sc.loadBalance(cmd.Type, msg.Command)
				sc.requestRecord[cmd.ClientId] = cmd.SequenceNum
			}
			sc.mu.Lock()
			ch, ok := sc.replyChan[msg.CommandIndex]
			sc.mu.Unlock()
			if ok {
				result := OpResult{}
				result.status = true
				if cmd.Type == QUERY {
					result.Result = sc.configs[len(sc.configs)-1]
				}
				ch <- result
			}
		}
	}
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	sc.killed = true
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.mu = sync.Mutex{}

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = make(map[int][]string)
	sc.configs[0].Num = 0
	sc.configs[0].Shards = [NShards]int{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.killed = false

	sc.replyChan = make(map[int]chan OpResult)
	sc.requestRecord = make(map[int64]int64)

	go sc.checkMessage()

	return sc
}
