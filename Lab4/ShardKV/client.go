package shardkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"shardctrler"
	"time"
)

// key2shard
/*
	类似哈希函数，计算key属于哪个分片
*/
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// nrand
/*
	生成唯一表示
*/
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Clerk 客户端
type Clerk struct {
	sm               *shardctrler.Clerk             // 配置节点的列表
	config           shardctrler.Config             // 当前的config
	make_end         func(string) *labrpc.ClientEnd // 用于生成类似终端的东西，可用来与ShardKV进行RPC
	groupLeaderIndex map[int]int                    // 用于记录每个Group中Leader的下标

	ClientId    int64 // 自身的客户端Id，也可被当做唯一标识
	SequenceNum int64 // 命令序号
}

// updateConfigInfo
/*
	更新Config的信息
*/
func (ck *Clerk) updateConfigInfo() {
	ck.config = ck.sm.Query(-1)
	ck.groupLeaderIndex = make(map[int]int)
}

// MakeClerk
/*
	初始化Clerk实例
*/
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.updateConfigInfo()
	ck.ClientId = nrand()
	ck.SequenceNum = 1
	return ck
}

// Get
/*
	给分布式数据库发送Get请求
*/
func (ck *Clerk) Get(key string) string {
	// 生成请求消息
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum // 分配唯一的Sequence序号
	// 分配之后递增序号，以便下次请求时使用
	ck.SequenceNum++
	for {
		// 计算key所属的分片，并获取分片所在的组号
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			serverNums := len(servers)
			// 遍历组中所有Server尝试发送请求
			for si := 0; si < serverNums; si++ {
				targetServerIndex := (ck.groupLeaderIndex[gid] + si) % serverNums
				srv := ck.make_end(servers[targetServerIndex])
				var reply GetReply
				// 发送Get请求到客户端
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.groupLeaderIndex[gid] = targetServerIndex
					return reply.Value
				}
			}
		}
		// 睡眠一定时间，避免频繁发送请求
		time.Sleep(50 * time.Millisecond)
		ck.updateConfigInfo()
	}
}

// PutAppend
/*
	给分布式数据库发送Put或者Append请求
*/
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// 构造请求消息
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	ck.SequenceNum++
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			serverNums := len(servers)
			for si := 0; si < len(servers); si++ {
				targetServerIndex := (ck.groupLeaderIndex[gid] + si) % serverNums
				srv := ck.make_end(servers[targetServerIndex])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.groupLeaderIndex[gid] = targetServerIndex
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					// 可能是自己的Config或者shardKV没有更新到的新的config
					// 跳出循环，等待一段时间
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
		ck.updateConfigInfo()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
