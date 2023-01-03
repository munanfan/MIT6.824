package kvraft

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers     []*labrpc.ClientEnd // 所有服务节点
	me          int64               // 唯一标识
	sequenceNum int64               // 请求序号
	leaderIndex int                 // 缓存的历史Leader在servers中的index
}

/*
用于生成唯一标识
*/
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

/*
修改leaderIndex以轮询server
*/
func (ck *Clerk) tryAnotherLeaderIndex() {
	ck.leaderIndex = (ck.leaderIndex + 1) % len(ck.servers)
}

// Get
/*
用于向客户端发送Get请求
*/
func (ck *Clerk) Get(key string) string {
	// 生成请求参数
	args := new(GetArgs)
	reply := new(GetReply)
	args.Key = key
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		// 重发五次，不行换Server发送
		for ok == false {
			ck.tryAnotherLeaderIndex()
			ok = ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply)
		}
		// send success
		if reply.State == OK || reply.State == REPEAT {
			// 请求成功或者重复请求，代表成功处理请求
			ck.sequenceNum++
			return reply.Response
		} else if reply.State == NotLeader || reply.State == KILLED || reply.State == TIMEOUT {
			// 尝试更换server节点
			ck.tryAnotherLeaderIndex()
		}
	}
}

// PutAppend
/*
发送更新或者添加请求
*/
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// 创建请求参数
	args := new(PutAppendArgs)
	reply := new(GetReply)
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		for ok == false {
			ck.tryAnotherLeaderIndex()
			ok = ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply)
		}
		if reply.State == OK || reply.State == REPEAT {
			// 请求成功或者重复请求，代表成功处理请求
			ck.sequenceNum++
			return
		} else if reply.State == NotLeader || reply.State == KILLED || reply.State == TIMEOUT {
			// 尝试更换server节点
			ck.tryAnotherLeaderIndex()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// MakeClerk
/*
创建客户端，用于向服务端发起请求
*/
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.sequenceNum = 1
	return ck
}
