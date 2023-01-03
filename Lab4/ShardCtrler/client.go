package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	me          int64
	leaderIndex int
	sequenceNum int64
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
	ck.me = nrand()
	ck.leaderIndex = 0
	ck.sequenceNum = 1
	return ck
}

func (ck *Clerk) tryAnotherLeader() {
	ck.leaderIndex = (ck.leaderIndex + 1) % len(ck.servers)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	ck.sequenceNum++

	for {
		var reply QueryReply
		ok := ck.servers[ck.leaderIndex].Call("ShardCtrler.Query", args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Config
			} else if reply.Err == NOT_LEADER {
				ck.tryAnotherLeader()
			}
		} else {
			ck.tryAnotherLeader()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	ck.sequenceNum++
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderIndex].Call("ShardCtrler.Join", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == NOT_LEADER {
				ck.tryAnotherLeader()
			}
		} else {
			ck.tryAnotherLeader()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	ck.sequenceNum++

	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderIndex].Call("ShardCtrler.Leave", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == NOT_LEADER {
				ck.tryAnotherLeader()
			}
		} else {
			ck.tryAnotherLeader()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.me
	args.SequenceNum = ck.sequenceNum
	ck.sequenceNum++

	for {
		var reply MoveReply
		ok := ck.servers[ck.leaderIndex].Call("ShardCtrler.Move", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == NOT_LEADER {
				ck.tryAnotherLeader()
			}
		} else {
			ck.tryAnotherLeader()
		}
		time.Sleep(10 * time.Millisecond)
	}
}
