package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	voteFor           int
	log               []Entry
	snapshot          []byte
	commitIndex       int
	lastApplied       int
	lastElectionTime  int64
	lastHeartbeatTime int64
	heartbeatTime     int64
	lastIncludedTerm  int // 上一个Log的Term
	lastIncludedIndex int // 上一个Log的下标，非[]Entry的下标，而是真实地Entry下标，也就是接收到cmd的序号，从1开始

	identity int

	nextIndex  []int
	matchIndex []int

	totalVotes    int
	totalPreVotes int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	SendTime     int64
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() { // ok
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool { // ok
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { // ok
	return rf.currentTerm, rf.identity == 3
}

func (rf *Raft) GetRaftStateSize() int { // ok
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// save the currentTerm, voteFor, Log
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() { // ok
	rf.persister.SaveRaftState(rf.getPersistData())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) { // ok
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// read the persistData
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		// 当日志没有自己新，或者已经包含了自己并未commit的数据
		return
	}
	// 切除已经快照了的日志
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

// 请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Println("I'm ", rf.me, ",receive a append from ", args.LeaderId, "args.Term ", args.Term, "log length is ", len(rf.log), ",my lastApplyIndex:", rf.lastApplied, ",commitIndex:", rf.commitIndex, ",entry length:", len(args.Entries), ",args.prevLogIndex:", args.PrevLogIndex, ",args.prevLogTerm:", args.PrevLogTerm, ",my lastIncludeIndex:", rf.lastIncludedIndex, ",my lastIncludeTerm:", rf.lastIncludedTerm, ",argTime ", args.SendTime, ",nowTime:", time.Now().UnixMilli())
	reply.Term = rf.currentTerm
	// 请求者的Term小于自己 或者 自己并非follower，拒绝Append
	if args.Term < rf.currentTerm {
		//fmt.Println("Index'm ", rf.me, ",receive a append from ", args.LeaderId, ",he out of date, my term is ", rf.currentTerm, ",request Term is ", args.Term, ",nowtime:", time.Now().UnixMilli())
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		//fmt.Println("Index'm ", rf.me, ",finish receive a append from ", args.LeaderId, "log length is ", len(rf.log))
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	if rf.identity != 1 {
		// 可能是旧Leader，也可能是正在和自己竞争选举
		rf.identity = 1
		rf.totalPreVotes = 0
		//fmt.Println("Index'm ", rf.me, ",receive a append from ", args.LeaderId, ", change my identity ", rf.identity)
	}
	// 判断PrevLogIndex是否符合
	rf.lastElectionTime = time.Now().UnixMilli()
	// 判断PrevLogIndex是否存在
	if args.PrevLogIndex >= rf.lastIncludedIndex+len(rf.log) { // PrevLogIndex不存在
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log)
		fmt.Println("Index'm ", rf.me, ",finish receive a append from, PrevLogIndex不存在 ", args.LeaderId, "log length is ", len(rf.log))
		return
	}
	if (args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm == rf.lastIncludedTerm) || (args.PrevLogIndex > rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm) {
		// 日志至少比自己新，符合要求，开始接受Append
		reply.Success = true
		//fmt.Println("args.logLength", args.PrevLogIndex+1+len(args.Entries), "my log Length:", len(rf.log))
		if args.PrevLogIndex+len(args.Entries) > rf.lastIncludedIndex+len(rf.log)-1 {
			// 发送的日志比自己的日志长，直接替换
			//fmt.Println("length before:", len(rf.log))
			rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1]
			rf.log = append(rf.log, args.Entries...)
			//fmt.Println("length after:", len(rf.log))
		} else {
			// 从前往后看是否有不同
			for i := 0; i < len(args.Entries); i++ {
				if rf.log[args.PrevLogIndex-rf.lastIncludedIndex+1+i].Term != args.Entries[i].Term {
					// 发现不同
					rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1+i]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			}
		}
		// 说明是心跳，直接接受Append，然后更新commitIndex
		if args.LeaderCommit > rf.commitIndex {
			//fmt.Println("update commitINdex")
			if args.LeaderCommit < rf.lastIncludedIndex+len(rf.log) {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastIncludedIndex + len(rf.log) - 1
			}
		}
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		rf.persist()
		fmt.Println("Index'm ", rf.me, ",finish receive a append from accept ", args.LeaderId, ",my log length is ", len(rf.log), ",argCommitIndex:", args.LeaderCommit, ",myCommitIndex:", rf.commitIndex)
		return
	} else {
		// rf.log[args.PrevLogIndex].Term != args.PrevLogTerm
		if args.PrevLogIndex <= rf.lastIncludedIndex {
			fmt.Println("update reply.ConflictTerm , args.PrevLogIndex == rf.lastIncludedIndex, rf.lastIncludedIndex ", rf.lastIncludedIndex)
			reply.ConflictTerm = rf.lastIncludedTerm
		} else if args.PrevLogIndex > rf.lastIncludedIndex {
			fmt.Println("update reply.ConflictTerm , args.PrevLogIndex == rf.lastIncludedIndex, rf.lastIncludedIndex ", rf.lastIncludedIndex, ",args.PrevLogIndex ", args.PrevLogIndex)
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		}
		if args.PrevLogIndex-rf.lastIncludedIndex == 0 {
			reply.ConflictIndex = rf.lastIncludedIndex
		} else {
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					//fmt.Println("i:", i, ",term:", rf.log[i].Term, ",command:", rf.log[i].Command)
					reply.ConflictIndex = i + rf.lastIncludedIndex
				} else {
					break
				}
			}
		}
		reply.Success = false
		fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, ",rf.log[args.PrevLogIndex].Term != args.PrevLogTerm,finish receive a append from ", args.LeaderId, "log length is ", len(rf.log), ",ConflictTerm:", reply.ConflictTerm, ",ConflictIndex:", reply.ConflictIndex, ",PrevlogIndex:", args.PrevLogIndex, ",PrevlogTerm:", args.PrevLogTerm)
		return
	}
}

// 处理投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, ",time:", time.Now().UnixMilli(), ",his term is ", args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 请求者的Term小于自己，不投票
	if args.Term < rf.currentTerm {
		//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, ",out of date, his term is ", args.Term, "not vote him")
		return
	}
	rf.mu.Lock()
	//fmt.Println("RequestVote: get lock ")
	defer rf.mu.Unlock()
	// 判断日志是否比自己新
	length := len(rf.log)
	if (length == 1 && args.LastLogTerm > rf.lastIncludedTerm) || (length != 1 && args.LastLogTerm > rf.log[length-1].Term) {
		// log newer than me
		if rf.voteFor == -1 {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.lastElectionTime = time.Now().UnixMilli()
			rf.identity = 1
			//fmt.Println("313 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
			rf.currentTerm = args.Term
			//fmt.Println("RequestVote: 312 ", oldTerm, " to ", rf.currentTerm)
			//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, ",lastLogTerm larger than me ", args.LastLogTerm, ",his term is ", args.Term, " ,vote him")
		} else {
			// check whether the term bigger than me
			if args.Term > rf.currentTerm {
				//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, ",lastLogTerm larger than me ", args.LastLogTerm, ",his term is ", args.Term, " larger than me, vote him")
				// term高于我，直接投票
				rf.voteFor = args.CandidateId
				rf.currentTerm = args.Term
				//fmt.Println("RequestVote: 322 ", oldTerm, " to ", rf.currentTerm)
				rf.lastElectionTime = time.Now().UnixMilli()
				rf.identity = 1
				//fmt.Println("330 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
				reply.VoteGranted = true
			} else {
				//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, ",lastLogTerm larger than me ", args.LastLogTerm, ",his term is ", args.Term, " smaller than me, not vote him")
			}
		}
	} else if (length == 1 && args.LastLogTerm == rf.lastIncludedTerm) || (length != 1 && args.LastLogTerm == rf.log[length-1].Term) {
		// 最后一个日志的Term相等，比较长度
		if args.LastLogIndex >= rf.lastIncludedIndex+length-1 {
			// log longger than me
			if rf.voteFor == -1 {
				reply.VoteGranted = true
				rf.voteFor = args.CandidateId
				rf.lastElectionTime = time.Now().UnixMilli()
				rf.identity = 1
				//fmt.Println("346 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
				rf.currentTerm = args.Term
				//fmt.Println("RequestVote: 341 ", oldTerm, " to ", rf.currentTerm)
				//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term,log longer than me  vote him")
			} else {
				// check whether the term bigger than me
				if args.Term > rf.currentTerm {
					// term高于我，直接投票
					rf.voteFor = args.CandidateId
					rf.currentTerm = args.Term
					//fmt.Println("RequestVote: 350 ", oldTerm, " to ", rf.currentTerm)
					rf.lastElectionTime = time.Now().UnixMilli()
					rf.identity = 1
					//fmt.Println("362 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
					reply.VoteGranted = true
					//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, term larger than me  vote him")
				} else {
					//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, term smaller than me  not vote him")
				}
			}
		} else {
			//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, log length smaller than me  not vote him")
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 发送者的Term太低 或者 发送的快照比自己的旧
	if rf.currentTerm > args.Term || args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	fmt.Println("raft ", rf.me, ",receive InstallSnapshot from ", args.LeaderId, ",args.LastIncludedIndex ", args.LastIncludedIndex, ",my lastIncludedIndex ", rf.lastIncludedIndex)
	rf.lastElectionTime = time.Now().UnixMilli()
	// 检查快照中的lastIncludedIndex是否比自己的log新
	if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log) {
		// 发送的快照中日志比自己所有的日志新
		rf.snapshot = make([]byte, len(args.Snapshot))
		copy(rf.snapshot, args.Snapshot)
		rf.log = make([]Entry, 0)
		entry := Entry{}
		entry.Term = 0
		entry.Command = nil
		rf.log = append(rf.log, entry)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		// complete replace the log
		msg := ApplyMsg{}
		msg.Command = nil
		msg.CommandValid = false
		msg.CommandIndex = -1
		msg.SnapshotValid = true
		msg.Snapshot = rf.snapshot
		msg.SnapshotIndex = rf.lastIncludedIndex
		msg.SnapshotTerm = rf.lastIncludedTerm
		rf.applyCh <- msg
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
	} else if args.LastIncludedIndex > rf.lastIncludedIndex {
		// 发送的快照比自己的快照新，但是没有Log中的新
		rf.snapshot = make([]byte, len(args.Snapshot))
		copy(rf.snapshot, args.Snapshot)
		newLog := make([]Entry, 0)
		newLog = append(newLog, rf.log[0])
		newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
		rf.log = newLog
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		// if snapshot include the log that not commit
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := ApplyMsg{}
			msg.Command = nil
			msg.CommandValid = false
			msg.CommandIndex = -1
			msg.SnapshotValid = true
			msg.Snapshot = rf.snapshot
			msg.SnapshotIndex = rf.lastIncludedIndex
			msg.SnapshotTerm = rf.lastIncludedTerm
			rf.applyCh <- msg
			rf.lastApplied = args.LastIncludedIndex
			if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
			}
		}
	}
	//fmt.Println("InstallSnapshot:", "rf.lastIncludedIndex => ", rf.lastIncludedIndex, ",rf.lastIncludedTerm => ", rf.lastIncludedTerm, ",args.lastIncludedIndex => ", args.LastIncludedIndex, ", args.lastIncludedTerm => ", args.LastIncludedTerm, ",lastApplyIndex before:", rf.lastApplied)
	// 更新ApplyIndex和CommitIndex

	//fmt.Println("InstallSnapshot:", "rf.lastIncludedIndex => ", rf.lastIncludedIndex, ",rf.lastIncludedTerm => ", rf.lastIncludedTerm, ",args.lastIncludedIndex => ", args.LastIncludedIndex, ", args.lastIncludedTerm => ", args.LastIncludedTerm, ",lastApplyIndex after:", rf.lastApplied)

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), rf.snapshot)
}

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
	// 说明应该接受了SnapShot，更新commitIndex
	if rf.nextIndex[serverIndex] < args.LastIncludedIndex+1 {
		rf.nextIndex[serverIndex] = args.LastIncludedIndex + 1
		fmt.Println("518 update rf.nextIndex[", serverIndex, "] ", rf.nextIndex[serverIndex])
	}
	if rf.matchIndex[serverIndex] < args.LastIncludedIndex {
		rf.matchIndex[serverIndex] = args.LastIncludedIndex
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pro vote request of ", args.CandidateId, ",time:", time.Now().UnixMilli(), ",his term is ", args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 请求者的Term小于自己，不投票
	if args.Term < rf.currentTerm {
		//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the provote request of ", args.CandidateId, ",out of date, his term is ", args.Term, "not vote him")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 判断日志是否比自己新
	length := len(rf.log)
	if (length == 1 && args.LastLogIndex > rf.lastIncludedIndex) || (length != 0 && args.LastLogTerm > rf.log[length-1].Term) {
		if args.Term == rf.currentTerm {
			if rf.voteFor == -1 {
				reply.VoteGranted = true
				//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pre vote request of ", args.CandidateId, ",lastLogTerm larger than me, and Index have vote ", args.LastLogTerm, ",his term is ", args.Term, " ,vote him")
			}
		} else {
			// term高于我，直接投票
			reply.VoteGranted = true
			//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pre vote request of ", args.CandidateId, ",lastLogTerm larger than me, and term higher than me ", args.LastLogTerm, ",his term is ", args.Term, " ,vote him")
		}
	} else if (length == 1 && args.LastLogIndex == rf.lastIncludedIndex) || (length != 1 && args.LastLogTerm == rf.log[length-1].Term) {
		// 最后一个日志的Term相等，比较长度
		if args.LastLogIndex >= rf.lastIncludedIndex+length-1 {
			// log longger than me
			if rf.voteFor == -1 {
				reply.VoteGranted = true
				//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pre vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term,log longer than me and Index have vote,  vote him")
			} else {
				// check whether the term bigger than me
				if args.Term > rf.currentTerm {
					// term高于我，直接投票
					reply.VoteGranted = true
					//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pre vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, term larger than me  vote him")
				} else {
					//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pre vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, term smaller than me  not vote him")
				}
			}
		} else {
			//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, "receive the pro vote request of ", args.CandidateId, "args.LastLogTerm == rf.log[length-1].Term, log length smaller than me  not vote him")
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // ok
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { // ok
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { // no
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool { // no
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
	// 模拟当前term+1,实际需要等真实投票的时候才会增加
	if prev {
		args.Term = rf.currentTerm + 1
	} else {
		args.Term = rf.currentTerm
	}
	return args
}

func (rf *Raft) sendRequestPreVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // ok
	ok := rf.peers[server].Call("Raft.RequestPreVote", args, reply)
	return ok
}

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

func (rf *Raft) handlerRequestPreVoteReply(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is", rf.currentTerm, ",receive the prevote result")
	//fmt.Println("Index'm ", rf.me, args.Term != rf.currentTerm+1, " ", reply.Term > rf.currentTerm, " ", rf.totalPreVotes >= (len(rf.peers)/2+1), " ", rf.identity != 2)
	// 当前和发送时的Term不同 或者 自己过时 或者 已经获得大部分投票 或者 当前已经不是投票者身份，丢弃
	if args.Term != rf.currentTerm+1 || reply.Term > rf.currentTerm || rf.totalPreVotes >= (len(rf.peers)/2+1) || rf.identity != 2 {
		//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is", rf.currentTerm, ",solve prevote false")
		return
	}
	//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is", rf.currentTerm, ",totalPrevote is ", rf.totalPreVotes)
	if reply.VoteGranted {
		rf.mu.Lock()
		//fmt.Println("handlerRequestPreVoteReply: get lock")
		rf.totalPreVotes++
		if rf.totalPreVotes >= (len(rf.peers)/2 + 1) {
			//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is", rf.currentTerm, ",pre vote success")
			// 预投票成功，准备发送真实投票请求
			rf.currentTerm++
			rf.totalVotes = 1
			rf.voteFor = rf.me
			//fmt.Println("handlerRequestPreVoteReply: lose lock")
			rf.mu.Unlock()
			msg := rf.makeVoteMessage(false)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					reply := new(RequestVoteReply)
					go rf.sendRequestVoteToFollower(i, msg, reply)
				}
			}
			return
		}
		//fmt.Println("handlerRequestPreVoteReply: lose lock")
		rf.mu.Unlock()
	}
}

func (rf *Raft) handlerRequestVoteReply(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	// 当前和发送时的Term不同 或者 自己过时 或者 已经获得大部分投票 或者 当前已经不是投票者身份，丢弃
	if args.Term != rf.currentTerm || reply.Term > rf.currentTerm || rf.totalVotes >= (len(rf.peers)/2+1) || rf.identity != 2 {
		return
	}
	if reply.VoteGranted {
		rf.mu.Lock()
		//fmt.Println("handlerRequestVoteReply: get lock")
		rf.totalVotes++
		if rf.totalVotes >= (len(rf.peers)/2 + 1) {
			// 选举成功，成为leader
			rf.identity = 3
			//fmt.Println("Index'm ", rf.me, ",become the leader, term is ", rf.currentTerm)
			//fmt.Println("542 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
			rf.totalVotes = 0
			rf.totalPreVotes = 0
			//fmt.Println("Index'm ", rf.me, ",term is ", rf.currentTerm, ",become the leader, my log length is ", len(rf.log))
			// 重置nextIndex和matchIndex
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log)
				fmt.Println("update rf.nextIndex[", i, "]:", rf.nextIndex[i])
				if i == rf.me {
					rf.matchIndex[i] = rf.nextIndex[i] - 1
				} else {
					rf.matchIndex[i] = 0
				}
			}
			//fmt.Println("handlerRequestVoteReply: lose lock")
			rf.mu.Unlock()
			// 发送Heartbeat
			go rf.CheckHeartbeat()
			return
		}
		//fmt.Println("handlerRequestVoteReply: lose lock")
		rf.mu.Unlock()
	}
}

func (rf *Raft) handlerAppendEntriesReply(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) { // ok
	//fmt.Println("Index'm ", rf.me, ",receive the reply of append ", serverIndex, " result is ", reply.Success, ",replyTerm ", reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != 3 {
		return
	}
	if reply.Term > rf.currentTerm {
		// 自己过时
		rf.identity = 1
		//fmt.Println("575 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
		rf.currentTerm = reply.Term
		//fmt.Println("RequestVote: 565 ", oldTerm, " to ", rf.currentTerm)
		rf.voteFor = -1 // 清空自己的投票
		rf.lastElectionTime = time.Now().UnixMilli()
		//fmt.Println("Index'm ", rf.me, ",receive the reply of append ", serverIndex, " out of date")
		return
	}
	if args.Term != rf.currentTerm {
		// 发送时的Term和当前Term不符，丢弃
		return
	}
	// 接收成功
	if reply.Success {
		//fmt.Println("reply success: last nextIndex => ", rf.nextIndex[serverIndex], ",receive length is ", args.PrevLogIndex+len(args.Entries)+1)
		rf.nextIndex[serverIndex] = args.PrevLogIndex + len(args.Entries) + 1
		fmt.Println("757 update rf.nextIndex[", serverIndex, "] ", rf.nextIndex[serverIndex])
		rf.matchIndex[serverIndex] = rf.nextIndex[serverIndex] - 1
	} else {
		//fmt.Println("Index'm ", rf.me, ",appear conflict")
		// 处理冲突，更新nextIndex
		if reply.ConflictTerm == -1 {
			if reply.ConflictIndex >= 0 {
				rf.nextIndex[serverIndex] = reply.ConflictIndex
				fmt.Println("765 update rf.nextIndex[", serverIndex, "] ", rf.nextIndex[serverIndex])
			}
		} else {
			// 在自己的log中寻找conflictTerm
			flag := false
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					rf.nextIndex[serverIndex] = rf.lastIncludedIndex + i + 1
					fmt.Println("773 update rf.nextIndex[", serverIndex, "] ", rf.nextIndex[serverIndex])
					flag = true
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}
			if !flag { // 没有找到conflictTerm
				rf.nextIndex[serverIndex] = reply.ConflictIndex
				fmt.Println("783 update rf.nextIndex[", serverIndex, "] ", rf.nextIndex[serverIndex])
				//fmt.Println("don't find ConflictTerm, set rf.nextIndex[serverIndex]:", rf.nextIndex[serverIndex])
			} else {
				//fmt.Println("find ConflictTerm, set rf.nextIndex[serverIndex]:", rf.nextIndex[serverIndex])
			}
		}
	}
}

func (rf *Raft) sendHeartbeatToFollower(serverIndex int) { // ok
	rf.mu.Lock()
	if rf.identity != 3 {
		rf.mu.Unlock()
		return
	}
	//fmt.Println("sendHeartbeatToFollower: get lock ")
	if rf.nextIndex[serverIndex] <= rf.lastIncludedIndex {
		fmt.Println("send snapshot as heartbeat, rf.nextIndex[serverIndex] ", rf.nextIndex[serverIndex], ",rf.lastIncludedIndex ", rf.lastIncludedIndex)
		// 说明需要的log已经被Leader丢弃，需要发送快照
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
	args := new(AppendEntriesArgs)
	args.PrevLogIndex = rf.nextIndex[serverIndex] - 1
	args.SendTime = time.Now().UnixMilli()
	fmt.Println("sendHeartbeatToFollower ", serverIndex, ": args.PrevLogIndex => ", args.PrevLogIndex, ",rf.lastIncludedIndex: ", rf.lastIncludedIndex)
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
	//fmt.Println("Index'm ", rf.me, ",Send heatbeat appendEntries to ", rf.me, ", The log of PrevLogIndex term is ", rf.log[args.PrevLogIndex].Term, ",command is ", rf.log[args.PrevLogIndex].Command)
	if args.PrevLogIndex+1 < len(rf.log) {
		//fmt.Println("the nextlog of args.PrevLogIndex is ", rf.log[args.PrevLogIndex+1].Command)
	}
	if len(args.Entries) != 0 {
		//fmt.Println("the start of of args.Entries is ", args.Entries[0].Command)
	}
	reply := new(AppendEntriesReply)
	//fmt.Println("sendHeartbeatToFollower to ", serverIndex, ": lose lock ")
	rf.mu.Unlock()
	state := false
	if rf.identity == 3 {
		state = rf.sendHeartbeat(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		time.Sleep(100 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendHeartbeat(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerAppendEntriesReply(serverIndex, args, reply)
}

func (rf *Raft) sendRequestVoteToFollower(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	state := false
	if rf.identity == 2 {
		state = rf.sendRequestVote(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		time.Sleep(100 * time.Millisecond)
		if rf.identity == 2 {
			state = rf.sendRequestVote(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerRequestVoteReply(serverIndex, args, reply)
}

func (rf *Raft) sendRequestPreVoteToFollower(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) { // ok
	state := false
	if rf.identity == 2 {
		state = rf.sendRequestPreVote(serverIndex, args, reply)
	} else {
		return
	}
	for state == false {
		time.Sleep(100 * time.Millisecond)
		if rf.identity == 2 {
			state = rf.sendRequestPreVote(serverIndex, args, reply)
		} else {
			return
		}
	}
	rf.handlerRequestPreVoteReply(serverIndex, args, reply)
}

// 发送AppendEntries给所有人
func (rf *Raft) sendAppendEntriesToFollower(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.identity != 3 {
		return
	}
	state := rf.sendAppendEntries(serverIndex, args, reply)
	// 发送失败重复发送
	for state == false {
		time.Sleep(100 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendAppendEntries(serverIndex, args, reply)
		} else {
			return
		}
	}
	// 处理回复
	rf.handlerAppendEntriesReply(serverIndex, args, reply)
}

// 发送AppendEntries给所有人
func (rf *Raft) sendInstallSnapshotToFollower(serverIndex int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.identity != 3 {
		return
	}
	state := rf.sendInstallSnapshot(serverIndex, args, reply)
	// 发送失败重复发送
	for state == false {
		time.Sleep(100 * time.Millisecond)
		if rf.identity == 3 {
			state = rf.sendInstallSnapshot(serverIndex, args, reply)
		} else {
			return
		}
	}
	// 处理回复
	rf.handlerInstallSnapshot(serverIndex, args, reply)
}

func (rf *Raft) CheckHeartbeat() { // ok
	for rf.killed() == false && rf.identity == 3 {
		nowTime := time.Now().UnixMilli()
		if nowTime-rf.lastHeartbeatTime > rf.heartbeatTime {
			rf.lastHeartbeatTime = nowTime
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					//fmt.Println("Index'm ", rf.me, ",send Heartbeat to ", i, ",my log length is ", len(rf.log), ",rf.lastIncludeIndex:", rf.lastIncludedIndex)
					go rf.sendHeartbeatToFollower(i)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) CheckApplyCommand() { // ok
	// 当Raft没有被Kill，就一直运行
	for rf.killed() == false {
		//fmt.Println("I'm raft ", rf.me, ", my lastApplyIndex ", rf.lastApplied, ",my commitIndex ", rf.commitIndex)
		//fmt.Println("I'm raft ", rf.me, ", preprare get the CheckApplyCommand lock")
		rf.mu.Lock()
		//fmt.Println("I'm raft ", rf.me, ", get the lock to check identity")
		if rf.identity == 3 {
			//fmt.Println("I'm raft ", rf.me, ", get the lock to check identity, is leader")
			// 检查是否能够更新commitIndex
			sortedMatchIndex := make([]int, len(rf.peers))
			copy(sortedMatchIndex, rf.matchIndex)
			sort.Ints(sortedMatchIndex)
			middleIndex := (len(rf.peers)+1)/2 - 1
			//fmt.Println("Index'm ", rf.me, ", CheckApplyCommand: get lock, lastApplu, ", rf.lastApplied, ",commitIndex:", rf.commitIndex, ",sortedMatchIndex[middleIndex]:", sortedMatchIndex[middleIndex])
			if rf.commitIndex < sortedMatchIndex[middleIndex] && rf.log[sortedMatchIndex[middleIndex]-rf.lastIncludedIndex].Term == rf.currentTerm {
				// commit可以更新，并且是当前的Term
				rf.commitIndex = sortedMatchIndex[middleIndex]
			}
		}
		rf.mu.Unlock()
		//fmt.Println("Index'm ", rf.me, ",CheckApplyCommand: lose lock")
		nowCommitIndex := rf.commitIndex
		if rf.lastApplied < nowCommitIndex {
			for i := rf.lastApplied + 1; i <= nowCommitIndex; i++ {
				//fmt.Println("start update lastApplyIndex, prepare get lock")
				rf.mu.Lock()
				//fmt.Println("start update lastApplyIndex, get lock")
				if i <= rf.lastApplied || i <= rf.lastIncludedIndex {
					//fmt.Println("i <= rf.lastApplied, lose lock")
					rf.mu.Unlock()
					break
				}
				//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",log length ", len(rf.log), ",applyIndex ", rf.lastApplied+1, ",command is ", rf.log[i-rf.lastIncludedIndex].Command)
				msg := ApplyMsg{}
				msg.CommandIndex = i
				msg.CommandValid = true
				msg.Command = rf.log[i-rf.lastIncludedIndex].Command
				msg.Snapshot = nil
				msg.SnapshotIndex = -1
				msg.SnapshotTerm = -1
				msg.SnapshotValid = false
				//fmt.Println("prepare lose lock and send msg to applych")
				rf.mu.Unlock()
				//fmt.Println("lose lock and send msg to applych")
				rf.applyCh <- msg
				//fmt.Println("I'm raft", rf.me, ",prepare get lock")
				rf.mu.Lock()
				//fmt.Println("I'm raft", rf.me, ",get lock")
				if rf.lastApplied+1 == i {
					rf.lastApplied++
				}
				rf.mu.Unlock()
				//fmt.Println("I'm raft", rf.me, ",lose lock, lastApplyId ", i, ",nowCommitIndex", nowCommitIndex)
			}
		}
		//fmt.Println("I'm raft", rf.me, ",prepare to sleep")
		time.Sleep(5 * time.Millisecond)
		//fmt.Println("I'm raft", rf.me, ",sleep finish, state", rf.killed())
	}
	//fmt.Println("I' raft ", rf.me, ",I was killed")
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) { // ok
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != 3 {
		return -1, -1, false
	}
	// tips: 包含了一个
	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.currentTerm
	// append the log
	entry := Entry{}
	entry.Term = rf.currentTerm
	entry.Command = command
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	fmt.Println("I'm ", rf.me, ",identity is ", rf.identity, ",receive a command ", command, ",log length is ", len(rf.log), "lastIncludedIndex:", rf.lastIncludedIndex)
	// 发送AppendEntry请求
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//fmt.Println("start: nextIndex is ", rf.nextIndex[i])
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				//fmt.Println("raft send snapshot")
				// 说明需要的log已经被Leader丢弃，需要发送快照
				args := new(InstallSnapshotArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.lastIncludedIndex
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.Snapshot = make([]byte, len(rf.snapshot))
				copy(args.Snapshot, rf.snapshot)
				reply := new(InstallSnapshotReply)
				fmt.Println("send snapshot in start, rf.nextIndex[serverIndex] ", rf.nextIndex[i], ",rf.lastIncludedIndex ", rf.lastIncludedIndex)
				go rf.sendInstallSnapshotToFollower(i, args, reply)
			} else {
				args := new(AppendEntriesArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.Entries = make([]Entry, rf.lastIncludedIndex+len(rf.log)-rf.nextIndex[i])
				args.SendTime = time.Now().UnixMilli()
				//fmt.Println("start: args.Entry length is ", len(args.Entries))
				copy(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:])
				args.PrevLogIndex = rf.nextIndex[i] - 1
				fmt.Println("raft send appendentry, args.PrevLogIndex ", args.PrevLogIndex, ",rf.lastIncludedIndex ", rf.lastIncludedIndex, ",rf.nextIndex[", i, "] = ", rf.nextIndex[i])
				//fmt.Println("start: args.PrevLogIndex => ", args.PrevLogIndex)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	randomSleepTime := rand.Int63n(20) + 30
	randomElectionTimeout := rand.Int63n(100) + 100
	for rf.killed() == false {
		//rf.mu.Lock()
		//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is ", rf.currentTerm, ",my log length is ", len(rf.log), ",lastApplyIndex, ", rf.lastApplied, ",commitIndex:", rf.commitIndex, ",time:", time.Now().UnixMilli(), ", lastCommand ", rf.log[len(rf.log)-1].Command)
		//rf.mu.Unlock()
		if rf.identity != 3 {
			nowTime := time.Now().UnixMilli()
			if nowTime-rf.lastElectionTime > randomElectionTimeout {
				//fmt.Println("Index'm ", rf.me, ",election timeout, rf.lastElectionTime:", rf.lastElectionTime, ",nowtime is ", nowTime, ",nowTime-rf.lastElectionTime:", nowTime-rf.lastElectionTime, ",my log length is ", len(rf.log))
				// 切换候选者身份
				rf.identity = 2
				//fmt.Println("861 Index'm ", rf.me, ",identity change from ", oldIdentity, " to ", rf.identity)
				rf.lastElectionTime = nowTime
				randomElectionTimeout = rand.Int63n(300) + 1000
				// 进行预投票
				rf.totalPreVotes = 1
				msg := rf.makeVoteMessage(true)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						//fmt.Println("Index'm ", rf.me, ",identity is ", rf.identity, ",term is ", rf.currentTerm, ",send prevote request to ", i)
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
	rf.snapshot = nil
	rf.commitIndex = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.lastElectionTime = time.Now().UnixMilli()
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.totalVotes = 0
	rf.totalPreVotes = 0
	rf.heartbeatTime = 50
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CheckApplyCommand()

	return rf
}
