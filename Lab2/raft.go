package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg 消息格式
type ApplyMsg struct {
	CommandValid bool        // 是否是消息
	Command      interface{} // Raft层传递上来的消息
	CommandIndex int         // 消息Index

	SnapshotValid bool   // 是否是快照
	Snapshot      []byte // 快照信息
	SnapshotTerm  int    // 快照所包含的最新的Log的Term
	SnapshotIndex int    // 快照所包含的最新的Log的Index
}

// Entry 日志格式，包括接受每一条命令时的Term以及对应的命令
type Entry struct {
	Term    int         // 收到命令时的人气
	Command interface{} // 收到的命令
}

// Raft Raft实例所需的属性
type Raft struct {
	mu        sync.Mutex          // 锁
	peers     []*labrpc.ClientEnd // 其他Raft节点
	persister *Persister          // 持久化
	me        int                 // 唯一表示
	dead      int32               // 是否被杀死
	applyCh   chan ApplyMsg       // 消息通道

	currentTerm       int     // 当前任期
	voteFor           int     // 投票
	log               []Entry // 日志列表
	snapshot          []byte  // 快照
	commitIndex       int     // 已经提交的日志Index，提交表示被大多数Raft节点接受
	lastApplied       int     // 已经提交给上层apply的Index
	lastElectionTime  int64   // 上一次选举时间/收到心跳时间
	lastIncludedTerm  int     // Snapshot包括的最后一个Log的Term
	lastIncludedIndex int     // Snapshot包括的最后一个Log的Index

	identity   int   // 表示Raft当前的身份
	nextIndex  []int // 记录每一个Follower的下一个应该发送的Log的Index
	matchIndex []int // 记录每一个Follower的最后一个提交的Log的Index

	totalVotes    int // 在某轮Term中获得的总票数
	totalPreVotes int // 在预投票的过程中获得的票数
}

// AppendEntriesArgs Leader给Follower发送日志同步或者心跳时的请求参数
type AppendEntriesArgs struct {
	Term         int     // 当前term
	LeaderId     int     // 发送的Leader方的id
	PrevLogIndex int     // 发送的起始Log的前一个Log的Index
	PrevLogTerm  int     // 发送的起始Log的前一个Log的Term
	Entries      []Entry // 发送的Log Entry列表，用于主从同步
	LeaderCommit int     // Leader目前已经Commit的日志Index
}

// AppendEntriesReply Leader收到Follower对于AppendEntries的回复
type AppendEntriesReply struct {
	Term          int  // 响应方当前的Term
	Success       bool // 是否接受发送的日志
	ConflictIndex int  // 冲突的日志Index
	ConflictTerm  int  // 冲突的日志的Term
}

// RequestVoteArgs 请求其他Raft节点投票时的请求参数
type RequestVoteArgs struct {
	Term         int // 请求者当前的Term
	CandidateId  int // 请求者自己的Id
	LastLogIndex int // 请求者日志列表中的最后一个日志的Index
	LastLogTerm  int // 请求者日志列表中的最后一个日志的Term
}

// RequestVoteReply Follower或者Candidate对于投票者请求的响应参数
type RequestVoteReply struct {
	Term        int  // 接收方的Term
	VoteGranted bool // 投票结果，是否投票给自己
}

// InstallSnapshotArgs Leader向Follower发送快照的参数
type InstallSnapshotArgs struct {
	Term              int    // 发送者的Term
	LeaderId          int    // 发送者的Id
	LastIncludedIndex int    // 快照所包含的最后一个日志的Index序号
	LastIncludedTerm  int    // 快照所包含的最后一个日志的Term
	Snapshot          []byte // 快照
}

// InstallSnapshotReply Follower对应快照请求的响应
type InstallSnapshotReply struct {
	Term int // 接受方的Term
}

// Kill
/*
	shutdown当前Raft节点
*/
func (rf *Raft) Kill() { // ok
	atomic.StoreInt32(&rf.dead, 1)
}

// killed
/*
	监测当前节点是否被Kill
*/
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// GetState
/*
	用于ShardKV层获取Raft层的状态
*/
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.identity == 3
}

// GetRaftStateSize
/*
	获取当前Raft状态的大小，用于判断是否需要快照
*/
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// getPersistData
/*
	获取需要持久化数据的字节流
*/
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "Encode rf.currentTerm error.")
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "Encode rf.voteFor error.")
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "Encode rf.lastIncludedIndex error.")
	}
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "Encode rf.lastIncludedTerm error.")
	}
	err = e.Encode(rf.log)
	if err != nil {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "Encode rf.logList error.")
	}
	data := w.Bytes()
	return data
}

// persist
/*
	持久化当前Raft的信息
*/
func (rf *Raft) persist() { // ok
	rf.persister.SaveRaftState(rf.getPersistData())
}

// readPersist
/*
	读取持久化的数据，用于节点故障后快速恢复等场景
*/
func (rf *Raft) readPersist(data []byte) { // ok
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var logList []Entry
	if d.Decode(&currentTerm) == nil &&
		d.Decode(&voteFor) == nil &&
		d.Decode(&lastIncludedIndex) == nil &&
		d.Decode(&lastIncludedTerm) == nil &&
		d.Decode(&logList) == nil {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.log = logList
	} else {
		fmt.Println("time: ", time.Now().UnixMilli(), " ", "decode fail.")
	}
}

// CondInstallSnapshot 判断当前是否能够进行快照
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot
/*
	用于ShardKV向Raft层传送snapshot快照数据，减少Log占用的存储空间
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		// 当收到的快照没有自己已经存储的快照新，或者已经包含了自己并未commit的数据
		return
	}
	// 删除已经在快照中包含的日志，用来减少日志占用的空间
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
	newLog := make([]Entry, 1)
	copy(newLog, rf.log[:1])
	newLog = append(newLog, rf.log[index-rf.lastIncludedIndex+1:]...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), rf.snapshot)
}

// AppendEntries
/*
	Follower处理Leader发送的日志同步请求
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// 发送者的Term小于自己的Term，忽略请求
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		// 自己的Term小于Leader的Term，说明自己可能网络分区或者其他原因，更新自己所处的任期并重置自己的投票
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	if rf.identity != 1 {
		// 自己目前可能参与选举，收到Leader的RPC信息，重置自己为Follower
		rf.identity = 1
		rf.totalPreVotes = 0
	}
	// 刷新选举超时时间
	rf.lastElectionTime = time.Now().UnixMilli()
	if args.PrevLogIndex >= rf.lastIncludedIndex+len(rf.log) {
		// PrevLogIndex不存在
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log)
		return
	}
	if (args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm == rf.lastIncludedTerm) || (args.PrevLogIndex > rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm) {
		// 日志比自己新，接受请求中的日志
		reply.Success = true
		if args.PrevLogIndex+len(args.Entries) > rf.lastIncludedIndex+len(rf.log)-1 {
			// 日志比自己长，说明比自己更新，直接全部替换
			rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1]
			rf.log = append(rf.log, args.Entries...)
		} else {
			// 日志可能没有自己新，查看是否有不符合的Log Entry，如果有就进行替换
			for i := 0; i < len(args.Entries); i++ {
				if rf.log[args.PrevLogIndex-rf.lastIncludedIndex+1+i].Term != args.Entries[i].Term {
					// 发现不同，替换此日志以及之后的日志
					rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1+i]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			}
		}
		// 根据LeaderCommit更新自己的commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.lastIncludedIndex+len(rf.log) {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastIncludedIndex + len(rf.log) - 1
			}
		}
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		// 进行持久化
		rf.persist()
		return
	} else {
		// 进入此处说明Leader发送的日志和自己有冲突
		if args.PrevLogIndex <= rf.lastIncludedIndex {
			// 设置冲突Term为rf.lastIncludedTerm
			reply.ConflictTerm = rf.lastIncludedTerm
		} else if args.PrevLogIndex > rf.lastIncludedIndex {
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		}
		// 这部分都是根据Lab中的快速同步设置的
		if args.PrevLogIndex-rf.lastIncludedIndex == 0 {
			reply.ConflictIndex = rf.lastIncludedIndex
		} else {
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i + rf.lastIncludedIndex
				} else {
					break
				}
			}
		}
		reply.Success = false
		return
	}
}

// RequestVote
/*
	处理Candidate发送给自己的投票请求
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 说明发送者过期，忽略投票请求并且发送自己的Term帮助其更新自己的信息
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	length := len(rf.log)
	// 判断请求者的日志是否比自己的日志新，由此来保证Leader一定是至少包含最新的日志
	if (length == 1 && args.LastLogTerm > rf.lastIncludedTerm) || (length != 1 && args.LastLogTerm > rf.log[length-1].Term) {
		// 日志比自己新
		if rf.voteFor == -1 {
			// 自己在本轮Term并未投票，投票给请求者
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.lastElectionTime = time.Now().UnixMilli()
			rf.identity = 1
			rf.currentTerm = args.Term
		} else {
			// Term比自己大，清空之前的投票（因为进入了新的term），把票投给请求者
			if args.Term > rf.currentTerm {
				rf.voteFor = args.CandidateId
				rf.currentTerm = args.Term
				rf.lastElectionTime = time.Now().UnixMilli()
				rf.identity = 1
				//// fmt.Println("330 Index'm ", rf.me, ",myname is ", rf.name, ",identity change from ", oldIdentity, " to ", rf.identity)
				reply.VoteGranted = true
			}
		}
	} else if (length == 1 && args.LastLogTerm == rf.lastIncludedTerm) || (length != 1 && args.LastLogTerm == rf.log[length-1].Term) {
		// 请求者的日志Term和自己相同
		if args.LastLogIndex >= rf.lastIncludedIndex+length-1 {
			// 日志最少和自己相同或者更新
			if rf.voteFor == -1 {
				reply.VoteGranted = true
				rf.voteFor = args.CandidateId
				rf.lastElectionTime = time.Now().UnixMilli()
				rf.identity = 1
				rf.currentTerm = args.Term
			} else {
				// Term比自己打，清空投票并把票投给请求者
				if args.Term > rf.currentTerm {
					rf.voteFor = args.CandidateId
					rf.currentTerm = args.Term
					rf.lastElectionTime = time.Now().UnixMilli()
					rf.identity = 1
					reply.VoteGranted = true
				}
			}
		}
	}
}

// InstallSnapshot
/*
	处理Leader发送过来的Snapshot
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	rf.mu.Lock()
	if rf.currentTerm > args.Term || args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 发送者Term太低，过期，不接受请求
		rf.mu.Unlock()
		return
	}
	if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log) {
		// 快照自身起码和自己所有的日志相同或者更新，清空Log完全替换为快照
		msg := ApplyMsg{}
		msg.Command = nil
		msg.CommandValid = false
		msg.CommandIndex = -1
		msg.SnapshotValid = true
		msg.Snapshot = args.Snapshot
		msg.SnapshotIndex = args.LastIncludedIndex
		msg.SnapshotTerm = args.LastIncludedTerm
		rf.snapshot = make([]byte, len(args.Snapshot))
		copy(rf.snapshot, args.Snapshot)
		rf.log = make([]Entry, 0)
		entry := Entry{}
		entry.Term = 0
		entry.Command = nil
		rf.log = append(rf.log, entry)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastElectionTime = time.Now().UnixMilli()
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
		}
		// 更新ApplyIndex和CommitIndex，持久化自身状态以及快照信息
		rf.persister.SaveStateAndSnapshot(rf.getPersistData(), rf.snapshot)
		rf.mu.Unlock()
		rf.applyCh <- msg
	} else if args.LastIncludedIndex > rf.lastIncludedIndex {
		// 快照比自己之前保存的快照新，部分替换自己的Log，保留发送来的快照之后的日志
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := ApplyMsg{}
			msg.Command = nil
			msg.CommandValid = false
			msg.CommandIndex = -1
			msg.SnapshotValid = true
			msg.Snapshot = args.Snapshot
			msg.SnapshotIndex = args.LastIncludedIndex
			msg.SnapshotTerm = args.LastIncludedTerm
			rf.snapshot = make([]byte, len(args.Snapshot))
			copy(rf.snapshot, args.Snapshot)
			newLog := make([]Entry, 0)
			newLog = append(newLog, rf.log[0])
			newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
			rf.log = newLog
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.lastElectionTime = time.Now().UnixMilli()
			rf.lastApplied = args.LastIncludedIndex
			if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
			}
			rf.persister.SaveStateAndSnapshot(rf.getPersistData(), rf.snapshot)
			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}
}

// handlerInstallSnapshot
/*
	处理Follower发送来的响应
*/
func (rf *Raft) handlerInstallSnapshot(serverIndex int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Term != rf.currentTerm {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.identity = 1
		rf.lastElectionTime = time.Now().UnixMilli()
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		return
	}
	// Follower接受了Snapshot，更新nextIndex和matchIndex
	if rf.nextIndex[serverIndex] < args.LastIncludedIndex+1 {
		rf.nextIndex[serverIndex] = args.LastIncludedIndex + 1
	}
	if rf.matchIndex[serverIndex] < args.LastIncludedIndex {
		rf.matchIndex[serverIndex] = args.LastIncludedIndex
	}
}

// RequestPreVote
/*
	处理Candidate发送来的预投票请求
	预投票的作用是为了确保Candidate可以与大多数的节点通信
	避免网络分区之后，候选者的Term不停增加
	Notice: 预投票只是检测是否满足投票条件，并不会真正的把自己的票投给请求者，投票只发生在正是投票过程中
*/
func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 请求者Term太低，过期，忽略请求并发送自己的Term帮助其更新自身信息
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	length := len(rf.log)
	if (length == 1 && args.LastLogIndex > rf.lastIncludedIndex) || (length != 0 && args.LastLogTerm > rf.log[length-1].Term) {
		// 日志比自己更
		if args.Term == rf.currentTerm {
			if rf.voteFor == -1 {
				// Term相同并且自己在本轮term中并未投票，预投票给发送者
				reply.VoteGranted = true
			}
		} else {
			// Term比自己高，直接预投票给发送者
			reply.VoteGranted = true
		}
	} else if (length == 1 && args.LastLogIndex == rf.lastIncludedIndex) || (length != 1 && args.LastLogTerm == rf.log[length-1].Term) {
		// 最后一条Log对应的Term相同，比较日志长度
		if args.LastLogIndex >= rf.lastIncludedIndex+length-1 {
			// 发送者日志更长，代表日志比自己新
			if rf.voteFor == -1 {
				// 本轮微投票，直接预投票给发送者
				reply.VoteGranted = true
			} else {
				if args.Term > rf.currentTerm {
					// term高于我，直接进行预投票
					reply.VoteGranted = true
				}
			}
		}
	}
}

// sendRequestVote
/*
	发送投票请求RPC到指定Raft节点
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // ok
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries
/*
	发送日志同步RPC到指定Raft节点
*/
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { // ok
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// sendHeartbeat
/*
	发送心跳RPC到指定Raft节点
*/
func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { // no
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// sendInstallSnapshot
/*
	发送快照RPC到指定Raft节点
*/
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool { // no
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// sendRequestPreVote
/*
	发送预投票RPC到指定Raft节点
*/
func (rf *Raft) sendRequestPreVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // ok
	ok := rf.peers[server].Call("Raft.RequestPreVote", args, reply)
	return ok
}

// makeVoteMessage
/*
	生成投票/预投票请求参数
*/
func (rf *Raft) makeVoteMessage(prev bool) *RequestVoteArgs { // ok
	rf.mu.Lock()
	defer rf.mu.Unlock()
	length := len(rf.log)
	args := new(RequestVoteArgs)
	if length == 1 {
		args.LastLogTerm = rf.lastIncludedTerm
		args.LastLogIndex = rf.lastIncludedIndex
	} else {
		args.LastLogTerm = rf.log[length-1].Term
		args.LastLogIndex = rf.lastIncludedIndex + length - 1
	}
	args.CandidateId = rf.me
	if prev {
		args.Term = rf.currentTerm + 1
	} else {
		args.Term = rf.currentTerm
	}
	return args
}

// makeAppendMessage
/*
	生成同步消息请求参数
*/
func (rf *Raft) makeAppendMessage(serverIndex int) *AppendEntriesArgs { // ok
	args := new(AppendEntriesArgs)
	args.PrevLogIndex = rf.nextIndex[serverIndex] - 1
	args.PrevLogTerm = rf.log[rf.nextIndex[serverIndex]-rf.lastIncludedIndex-1].Term
	args.LeaderCommit = rf.commitIndex
	args.Entries = rf.log[rf.nextIndex[serverIndex]-rf.lastIncludedIndex:]
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	return args
}

// handlerRequestPreVoteReply
/*
	处理其他Raft对于预投票请求的响应
*/
func (rf *Raft) handlerRequestPreVoteReply(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term != rf.currentTerm+1 || reply.Term > rf.currentTerm || rf.totalPreVotes >= (len(rf.peers)/2+1) || rf.identity != 2 {
		// 请求消息过期、自己的Term过期、已经获得了超过一半的投票、自己不是Candidate身份
		return
	}
	if reply.VoteGranted {
		rf.mu.Lock()
		rf.totalPreVotes++
		if rf.totalPreVotes >= (len(rf.peers)/2 + 1) {
			// 预投票获得超过一半的票数，准备发送真实投票请求
			rf.currentTerm++
			// 先把票投给自己
			rf.totalVotes = 1
			rf.voteFor = rf.me
			rf.mu.Unlock()
			msg := rf.makeVoteMessage(false)
			// 发送正式投票
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					reply := new(RequestVoteReply)
					go rf.sendRequestVoteToFollower(i, msg, reply)
				}
			}
			return
		}
		rf.mu.Unlock()
	}
}

// handlerRequestVoteReply
/*
	处理其他Raft对于正式投票请求的响应
*/
func (rf *Raft) handlerRequestVoteReply(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term != rf.currentTerm || reply.Term > rf.currentTerm || rf.totalVotes >= (len(rf.peers)/2+1) || rf.identity != 2 {
		// 请求消息过期、自己的Term过期、已经获得了超过一半的投票、自己不是Candidate身份
		return
	}
	if reply.VoteGranted {
		rf.mu.Lock()
		rf.totalVotes++
		if rf.totalVotes >= (len(rf.peers)/2 + 1) {
			// 选举成功，成为leader
			rf.identity = 3
			rf.totalVotes = 0
			rf.totalPreVotes = 0
			// 重置所有节点的nextIndex和matchIndex
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) // 相当于乐观估计
				if i == rf.me {
					rf.matchIndex[i] = rf.nextIndex[i] - 1
				} else {
					rf.matchIndex[i] = 0
				}
			}
			rf.mu.Unlock()
			// 开始发送心跳
			go rf.CheckHeartbeat()
			return
		}
		rf.mu.Unlock()
	}
}

// handlerAppendEntriesReply
/*
	处理其他Raft对于日志同步请求的响应
*/
func (rf *Raft) handlerAppendEntriesReply(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) { // ok
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != 3 {
		return
	}
	if reply.Term > rf.currentTerm {
		// 自己过时，清空变量
		rf.identity = 1
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.lastElectionTime = time.Now().UnixMilli()
		return
	}
	if args.Term != rf.currentTerm {
		// 请求时的Term和自己现在的term不对应，说明消息过时
		return
	}
	if reply.Success {
		// 响应方接收了自己发送的Log，更新nextIndex和matchIndex
		rf.nextIndex[serverIndex] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[serverIndex] = rf.nextIndex[serverIndex] - 1
	} else {
		// 响应方未接收Log，处理冲突
		if reply.ConflictTerm == -1 {
			// 仅有ConflictIndex信息，设置下次该Raft节点的发送序号为ConflictIndex
			if reply.ConflictIndex >= 0 {
				rf.nextIndex[serverIndex] = reply.ConflictIndex
			}
		} else {
			// 在日志中寻找和ConflictTerm一样的Term的下一个Index
			flag := false
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					rf.nextIndex[serverIndex] = rf.lastIncludedIndex + i + 1
					flag = true
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}
			if !flag {
				// 没有找到conflictTerm，直接设置nextIndex为ConflictIndex
				rf.nextIndex[serverIndex] = reply.ConflictIndex
			}
		}
	}
}

// sendHeartbeatToFollower
/*
	给某一个Raft节点发送心跳信息
*/
func (rf *Raft) sendHeartbeatToFollower(serverIndex int) { // ok
	rf.mu.Lock()
	if rf.identity != 3 || rf.killed() {
		// 检测自身状态是否合法
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[serverIndex] <= rf.lastIncludedIndex {
		// 该Follower下一个需要发送的Log已经被快照替代，直接发送快照给Follower
		args := new(InstallSnapshotArgs)
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LastIncludedIndex = rf.lastIncludedIndex
		args.LastIncludedTerm = rf.lastIncludedTerm
		args.Snapshot = make([]byte, len(rf.snapshot))
		copy(args.Snapshot, rf.snapshot)
		reply := new(InstallSnapshotReply)
		go rf.sendInstallSnapshotToFollower(serverIndex, args, reply)
		rf.mu.Unlock()
		return
	}
	// 将日志列表同步给Follower
	args := new(AppendEntriesArgs)
	args.PrevLogIndex = rf.nextIndex[serverIndex] - 1
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
	}
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]Entry, rf.lastIncludedIndex+len(rf.log)-rf.nextIndex[serverIndex])
	copy(args.Entries, rf.log[rf.nextIndex[serverIndex]-rf.lastIncludedIndex:])
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	reply := new(AppendEntriesReply)
	rf.mu.Unlock()
	state := false
	if rf.identity == 3 {
		state = rf.sendHeartbeat(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendHeartbeat(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerAppendEntriesReply(serverIndex, args, reply)
}

// sendRequestVoteToFollower
/*
	发送投票请求给Follower
*/
func (rf *Raft) sendRequestVoteToFollower(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) {
	state := false
	if rf.identity == 2 {
		state = rf.sendRequestVote(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond) // 避免频繁发送
		if rf.identity == 2 {
			state = rf.sendRequestVote(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerRequestVoteReply(serverIndex, args, reply)
}

// sendRequestPreVoteToFollower
/*
	发送预投票请求给其他Raft节点
*/
func (rf *Raft) sendRequestPreVoteToFollower(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	state := false
	if rf.identity == 2 {
		state = rf.sendRequestPreVote(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
		if rf.identity == 2 {
			state = rf.sendRequestPreVote(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerRequestPreVoteReply(serverIndex, args, reply)
}

// sendAppendEntriesToFollower
/*
	发送同步消息给其他Follower
*/
func (rf *Raft) sendAppendEntriesToFollower(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.identity != 3 || rf.killed() {
		return
	}
	state := rf.sendAppendEntries(serverIndex, args, reply)
	for state == false {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendAppendEntries(serverIndex, args, reply)
		} else {
			return
		}
	}
	// 调用处理响应函数
	rf.handlerAppendEntriesReply(serverIndex, args, reply)
}

// sendInstallSnapshotToFollower
/*
	发送快照给相应的Follower
*/
func (rf *Raft) sendInstallSnapshotToFollower(serverIndex int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.identity != 3 || rf.killed() {
		return
	}
	state := rf.sendInstallSnapshot(serverIndex, args, reply)
	// 发送失败重复发送
	for state == false {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendInstallSnapshot(serverIndex, args, reply)
		} else {
			return
		}
	}
	// 处理回复
	rf.handlerInstallSnapshot(serverIndex, args, reply)
}

// CheckHeartbeat
/*
	每隔50s给其他Follower发送心跳
*/
func (rf *Raft) CheckHeartbeat() {
	for rf.killed() == false && rf.identity == 3 {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendHeartbeatToFollower(i)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// CheckApplyCommand
/*
	检测是否有日志能够提交给上层ShardKV
*/
func (rf *Raft) CheckApplyCommand() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.identity == 3 {
			// Leader额外检测能够更新commitIndex：将各Follower的matchIndex排序取红位数
			sortedMatchIndex := make([]int, len(rf.peers))
			copy(sortedMatchIndex, rf.matchIndex)
			sort.Ints(sortedMatchIndex)
			middleIndex := (len(rf.peers)+1)/2 - 1
			if rf.commitIndex < sortedMatchIndex[middleIndex] && rf.log[sortedMatchIndex[middleIndex]-rf.lastIncludedIndex].Term == rf.currentTerm {
				rf.commitIndex = sortedMatchIndex[middleIndex]
			}
		}
		rf.mu.Unlock()
		// 检测是否有新的日志可以提交到上层
		if len(rf.log) != 1 {
			// 如果当前Log没有本Term的Log，发送一条Empty消息，用来提交之前非自己Term的Log
			if rf.log[len(rf.log)-1].Term != rf.currentTerm {
				rf.Start(-1)
			}
		} else {
			if rf.lastIncludedTerm != 0 && rf.lastIncludedTerm != rf.currentTerm {
				rf.Start(-1)
			}
		}
		nowCommitIndex := rf.commitIndex
		nowApplyIndex := rf.lastApplied
		lastIncludeIndex := rf.lastIncludedIndex
		if rf.lastApplied < nowCommitIndex {
			for i := nowApplyIndex + 1; i <= nowCommitIndex; i++ {
				rf.mu.Lock()
				if lastIncludeIndex != rf.lastIncludedIndex || i <= rf.lastApplied {
					/*
						如果在提交过程中，lastIncludedIndex改变（比如收到了上层的Snapshot或者Leader发送的Snapshot）
						或者lastApplied改变，就暂停提交过程。
					*/
					rf.mu.Unlock()
					break
				}
				msg := ApplyMsg{}
				msg.CommandIndex = i
				msg.CommandValid = true
				msg.Command = rf.log[i-lastIncludeIndex].Command
				msg.Snapshot = nil
				msg.SnapshotIndex = -1
				msg.SnapshotTerm = -1
				msg.SnapshotValid = false
				rf.mu.Unlock()
				if rf.killed() {
					return
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied+1 == i {
					rf.lastApplied++
				}
				rf.mu.Unlock()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Start
/*
	用于接收上层的Command命令，发送给其他Raft节点同步消息
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) { // ok
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != 3 {
		return -1, -1, false
	}
	// 获取当前命令的Index
	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.currentTerm
	entry := Entry{}
	entry.Term = rf.currentTerm
	entry.Command = command
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	// 发送日志同步请求给其他Raft节点
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				// 该Raft节点需要的日志已经被快照替代，切换发送快照给Follower
				args := new(InstallSnapshotArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.lastIncludedIndex
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.Snapshot = make([]byte, len(rf.snapshot))
				copy(args.Snapshot, rf.snapshot)
				reply := new(InstallSnapshotReply)
				go rf.sendInstallSnapshotToFollower(i, args, reply)
			} else {
				// 发送日志给Follower
				args := new(AppendEntriesArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.Entries = make([]Entry, rf.lastIncludedIndex+len(rf.log)-rf.nextIndex[i])
				copy(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:])
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex-rf.lastIncludedIndex == 0 {
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
				}
				args.LeaderCommit = rf.commitIndex
				reply := new(AppendEntriesReply)
				go rf.sendAppendEntriesToFollower(i, args, reply)
			}
		}
	}
	return index, term, true
}

// ticker
/*
	循环检测是否出现选举超时的情况，发起选举
*/
func (rf *Raft) ticker() {
	randomSleepTime := rand.Int63n(20) + 30         // 随机睡眠时间
	randomElectionTimeout := rand.Int63n(100) + 100 // 随机选举超时时间，避免Raft同一时间进行选举，导致多个Term选举不出来Leader的情况
	for rf.killed() == false {
		if rf.identity != 3 {
			nowTime := time.Now().UnixMilli()
			if nowTime-rf.lastElectionTime > randomElectionTimeout {
				// 选举超时，进行选举，切换信息 + 发送预投票
				rf.identity = 2
				rf.lastElectionTime = nowTime
				randomElectionTimeout = rand.Int63n(300) + 1000
				rf.totalPreVotes = 1
				msg := rf.makeVoteMessage(true)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						reply := new(RequestVoteReply)
						go rf.sendRequestPreVoteToFollower(i, msg, reply)
					}
				}
			}
		}
		time.Sleep(time.Duration(randomSleepTime) * time.Millisecond)
		randomSleepTime = rand.Int63n(20) + 30
	}
}

// Make
/*
	创建Raft节点，初始化变量
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft { // ok
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.applyCh = applyCh

	rf.identity = 1
	rf.voteFor = -1
	rf.log = make([]Entry, 0)
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.lastElectionTime = time.Now().UnixMilli()
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.totalVotes = 0
	rf.totalPreVotes = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	entry := Entry{}
	entry.Term = 0
	entry.Command = nil
	rf.log = append(rf.log, entry)

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// 读取之前的持久化信息
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	go rf.ticker()
	go rf.CheckApplyCommand()

	return rf
}
