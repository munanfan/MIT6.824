package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

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

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type ReplyAppendEntries struct {
	Term    int
	Success bool
}

type LogEntry struct {
	Key   string
	Value string
	Term  int
	Index int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	identity int8 // 1 => follower 2 => candidate 3=> leader

	currentTerm int        // 当前学期
	votedFor    int        // 投票，值为peers的下标
	logList     []LogEntry // 存放logEntry
	commitIndex int        // 已知的最高的commitIndex
	lastApplied int        // 已经应用到状态机的logID

	// follower数据
	lastHeartBeatTime int64 // 上一次接收到心跳的时间
	maxWaitTime       int64 // 计时器，用于检测心跳是否超时

	// candidate数据
	startCandidateTime int64 // 成为候选者的时间，用于判断此轮选举是否超时（在没有Leader产生的情况下）
	totalVotes         int

	// leader数据
	heartBeatTime int // 给其他server发送心跳的计时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.identity == 3
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 请求别人投票时的数据
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's Term
	CandidateId  int // candidate's index
	LastLogIndex int // index of candididate's last log entry
	LastLogTerm  int // the Term of last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 用于存储是否投票的信息
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票者目前的term，可帮助候选者调整自己
	VoteGranted bool // true，表示投票，false表示不投票
}

// example RequestVote RPC handler.
// 处理别人发送过来的请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	// 检查term是否相同
	if args.Term < rf.currentTerm {
		// 候选者过时，无法给他投票
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// 可能开了新一轮的投票，之前投票作废，重新投票
		rf.mu.Lock()
		if rf.identity != 1 {
			rf.identity = 1
		}
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.mu.Unlock()
	} else {
		// Term相同，比较Log长度
		length := len(rf.logList)
		if length == 0 {
			rf.mu.Lock()
			if rf.votedFor == -1 {
				//fmt.Println("i'm ", rf.identity, ", log length=0, vote him")
				if rf.identity != 1 {
					rf.identity = 1
				}
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		} else {
			// 检查term情况
			if rf.logList[length-1].Term == args.LastLogTerm && rf.logList[length-1].Index <= args.LastLogIndex {
				// 可以投票
				rf.mu.Lock()
				if rf.identity != 1 {
					rf.identity = 1
				}
				if rf.votedFor == -1 {
					//fmt.Println("i'm ", rf.identity, ", his log lnoger than me, vote him")
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
		reply.VoteGranted = false
	}
}

// handle the reply of request vote RPC
func (rf *Raft) handlerReplyOfVote(reply *RequestVoteReply) {
	if rf.identity != 2 {
		// 不是候选者身份，返回
		return
	}
	if reply.Term > rf.currentTerm {
		// 说明自己过时了，不能进行选举流程，返回Follower身份
		rf.mu.Lock()
		rf.identity = 1
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.mu.Unlock()
	} else {
		// 收到票数并计算是否成为Leader
		rf.mu.Lock()
		rf.totalVotes++
		if rf.totalVotes >= (len(rf.peers)+1)/2 {
			rf.identity = 3
			go rf.sendHearBeat()
		}
		rf.mu.Unlock()
	}
}

// 处理添加请求
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *ReplyAppendEntries) {
	if args.Term < rf.currentTerm {
		// 说明收到了过时的消息，将自己的Term回复给接收方
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// 更新接受到心跳的时间
		rf.lastHeartBeatTime = time.Now().UnixMilli()
		if args.Entries == nil {
			// 消息类型为HeartBeat
			rf.mu.Lock()
			if args.Term > rf.currentTerm {
				// 说明自己过时了，更新自己的Term
				rf.currentTerm = args.Term
			}
			if rf.identity != 1 {
				// 旧的Leader或者候选者可能在选举期间收到新任Leader的RPC消息
				rf.identity = 1
				// 清空自己的投票权
				rf.votedFor = -1
				rf.totalVotes = 0
			}
			rf.mu.Unlock()
			reply.Term = rf.currentTerm
			reply.Success = true
		} else {
			// 说明是一个AppendEntries，待添加内容
			reply.Term = rf.currentTerm
			reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHearBeatMessage(server int, args *AppendEntriesArg, reply *ReplyAppendEntries) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// start agreement on a new log entry
	index := -1
	term := -1
	if rf.identity != 3 {
		return index, term, false
	}
	// 是Leader，进行处理，

	// Your code here (2B).

	return index, term, true
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//fmt.Println("time is ", time.Now().UnixMilli(), ", I'm ", rf.me, ",I'm killed by someone")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 当身份为Leader，发送心跳HeartBeat
func (rf *Raft) sendHearBeat() {
	for rf.identity == 3 {
		// 给每一个人发送HeartBeat
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				length := len(rf.logList)
				args := AppendEntriesArg{}
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.Entries = nil
				args.LeaderCommit = rf.commitIndex
				if length == 0 {
					args.PrevLogIndex = -1
					args.PrevLogTerm = -1
				} else {
					args.PrevLogIndex = rf.logList[length-1].Index
					args.PrevLogTerm = rf.logList[length-1].Term
				}
				reply := ReplyAppendEntries{}
				state := rf.sendHearBeatMessage(i, &args, &reply)
				if state {
					if !reply.Success && reply.Term > rf.currentTerm {
						// 说明自己是过时的，可能是之前Crash的Leader重新上线导致，转换为Follower
						rf.mu.Lock()
						rf.identity = 1
						rf.currentTerm = reply.Term
						rf.totalVotes = 0
						rf.votedFor = -1
						rf.mu.Unlock()
					}
				} else {
					//fmt.Println("occur error when sending heartbeat.")
				}
			}
		}
	}
}

func (rf *Raft) printInfo() {
	fmt.Println("I'm ", rf.me, " ,currentTerm: ", rf.currentTerm, " ,my identity:", rf.identity)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 更新刷新时间
	randomSleepTime := rand.Int63n(7) + 3
	// 随机生成选举超时时长
	randomTimeOut := rand.Int63n(150) + 150
	for rf.killed() == false {
		// 随机睡眠一段时间
		time.Sleep(time.Duration(randomSleepTime) * time.Millisecond)
		//rf.printInfo()
		if rf.identity != 3 { // 自己不是Leader，才进行
			nowTime := time.Now().UnixMilli()
			if rf.identity == 1 && nowTime-rf.lastHeartBeatTime < rf.maxWaitTime {
				// 跟随者身份
				continue
			} else if rf.identity == 2 && nowTime-rf.startCandidateTime < randomTimeOut {
				// 候选者身份
				continue
			}
			// 心跳超时，需要进行新一轮Election
			if rf.identity == 2 {
				// 更新选举开始时间
				rf.startCandidateTime = nowTime
			} else {
				rf.lastHeartBeatTime = nowTime
			}
			rf.mu.Lock()
			rf.identity = 2     // 变为Candidate身份
			rf.currentTerm++    // 增加当前Term
			rf.totalVotes = 1   // 自己给自己投票
			rf.votedFor = rf.me // 将票数给自己
			rf.mu.Unlock()
			// 发送投票请求
			for i := 0; i < len(rf.peers); i++ {
				// 给除了自己的server发送信息
				if i != rf.me {
					go func(serverIndex int) {
						length := len(rf.logList)
						args := RequestVoteArgs{}
						// 填充请求数据
						args.Term = rf.currentTerm
						args.CandidateId = rf.me
						if length == 0 {
							// 心跳RPC无需此信息
							args.LastLogIndex = -1
							args.LastLogTerm = -1
						} else {
							args.LastLogIndex = rf.logList[length-1].Index
							args.LastLogTerm = rf.logList[length-1].Term
						}
						reply := RequestVoteReply{}
						state := rf.sendRequestVote(serverIndex, &args, &reply)
						if state {
							// 成功发送消息并收到回应，处理回应消息
							rf.handlerReplyOfVote(&reply)
						}
					}(i)
				}
			}
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.identity = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logList = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.lastHeartBeatTime = -1
	rf.maxWaitTime = 180
	rf.startCandidateTime = -1
	rf.heartBeatTime = 175

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
