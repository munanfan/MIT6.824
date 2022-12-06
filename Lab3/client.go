package kvraft

import (
	"6.824/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me          int64
	sequenceNum int64
	leaderIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) tryAnotherLeaderIndex() {
	ck.leaderIndex = (ck.leaderIndex + 1) % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.sequenceNum = 1
	return ck
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
	args := new(GetArgs)
	reply := new(GetReply)
	args.Key = key
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	fmt.Println("ck ", ck.me, ",send a get request, sequenceNum ", args.SequenceNum)
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		// 重发五次，不行换Server发送
		for ok == false {
			ck.tryAnotherLeaderIndex()
			ok = ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		}
		// send success
		if reply.State == OK || reply.State == REPEAT {
			// 成功，或者重复提交 都算成功执行
			ck.sequenceNum++
			fmt.Println("ck ", ck.me, ",ok, sequenceNum ", args.SequenceNum)
			return reply.Response
		} else if reply.State == NOT_LEADER || reply.State == KILLED || reply.State == TIMEOUT {
			// 换成另外一个Leader
			ck.tryAnotherLeaderIndex()
			fmt.Println("ck ", ck.me, ",change another leader, sequenceNum ", args.SequenceNum)
			continue
		}
		// 超时自动重新发送
		fmt.Println("TIMEOUT, send again")
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
	// make request args
	args := new(PutAppendArgs)
	reply := new(GetReply)
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	fmt.Println("ck ", ck.me, ",send a putappend request, sequenceNum ", args.SequenceNum)
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		// 重发五次，不行换Server发送
		for ok == false {
			ck.tryAnotherLeaderIndex()
			ok = ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		}
		// send success
		if reply.State == OK || reply.State == REPEAT {
			// 成功，或者重复提交 都算成功执行
			ck.sequenceNum++
			fmt.Println("ck ", ck.me, ",ok, sequenceNum ", args.SequenceNum)
			return
		} else if reply.State == NOT_LEADER || reply.State == KILLED || reply.State == TIMEOUT {
			// 换成另外一个Leader
			ck.tryAnotherLeaderIndex()
			fmt.Println("ck ", ck.me, ",change another leader, sequenceNum ", args.SequenceNum)
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
