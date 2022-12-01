package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex int
	cmdIndex    int
	mu          sync.Mutex
	me          int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderIndex = 0
	ck.mu = sync.Mutex{}
	ck.cmdIndex = 1
	ck.me = nrand()
	return ck
}

func (ck *Clerk) tryAnotherLeaderIndex() {
	ck.leaderIndex = (ck.leaderIndex + 1) % len(ck.servers)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// make request args
	args := GetArgs{}
	reply := new(GetReply)
	args.Key = key
	args.Sender = ck.me
	ck.mu.Lock()
	args.CmdIndex = ck.cmdIndex
	ck.cmdIndex++
	ck.mu.Unlock()
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		// 重发五次，不行换Server发送
		repeatTime := 0
		for ok == false {
			if repeatTime >= 5 {
				repeatTime = 0
				ck.tryAnotherLeaderIndex()
			}
			repeatTime++
			ok = ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		}
		// send success
		if reply.Err == OK || reply.Err == REPEAT {
			// 成功，或者重复提交 都算成功执行
			return reply.Value
		} else if reply.Err == NOTLEADER || reply.Err == KILLED {
			// 换成另外一个Leader
			ck.tryAnotherLeaderIndex()
		}
		// 超时自动重新发送
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.Sender = ck.me
	ck.mu.Lock()
	args.CmdIndex = ck.cmdIndex
	ck.cmdIndex++
	ck.mu.Unlock()
	reply := new(PutAppendReply)
	for {
		// check kvserver state
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		// 重发五次，不行换Server发送
		repeatTime := 0
		for ok == false {
			if repeatTime >= 5 {
				repeatTime = 0
				ck.tryAnotherLeaderIndex()
			}
			repeatTime++
			ok = ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		}
		// send success
		if reply.Err == OK || reply.Err == REPEAT {
			// 成功，或者重复提交 都算成功执行
			return
		} else if reply.Err == NOTLEADER || reply.Err == KILLED {
			// 换成另外一个Leader
			ck.tryAnotherLeaderIndex()
		}
		// 超时自动重新发送
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
