package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskRecordInfo struct {
	finishStatus  int
	lastTimeStamp int64
}

type Coordinator struct {
	Filename             []string         // 文件名列表
	NReduce              int              // Reduce数量
	MapTaskFinishInfo    []TaskRecordInfo // 记录map任务的信息
	ReduceTaskFinishInfo []TaskRecordInfo // 记录reduce任务的信息
	CurrentTaskType      int              // 当前的执行阶段
	Mutex                sync.Mutex       // 锁
}

// SolveHearBeat
/*
	处理worker发送来的hearbeat
*/
func (c *Coordinator) SolveHearBeat(request *Heartbeat, reply *HeartbeatReply) error {
	c.Mutex.Lock()
	// 根据不同的任务类型更新对应worker的时间戳
	if request.TaskType == 0 && c.CurrentTaskType == request.TaskType {
		c.MapTaskFinishInfo[request.TaskIndex].lastTimeStamp = time.Now().Unix()
	} else if request.TaskType == 1 && c.CurrentTaskType == request.TaskType {
		c.ReduceTaskFinishInfo[request.TaskIndex].lastTimeStamp = time.Now().Unix()
	}
	c.Mutex.Unlock()
	return nil
}

// ReceiveFinish
/*
	用于处理worker发送的任务完成请求
*/
func (c *Coordinator) ReceiveFinish(finishInfo *FinishTask, finishReply *WorkFinishReply) error {
	c.Mutex.Lock()
	// 根据不同的任务类型更新对应任务的状态
	if c.CurrentTaskType == 0 {
		c.MapTaskFinishInfo[finishInfo.TaskIndex].finishStatus = 1
		finishReply.State = 1
	} else {
		c.ReduceTaskFinishInfo[finishInfo.TaskIndex].finishStatus = 1
		finishReply.State = 1
	}
	c.Mutex.Unlock()
	return nil
}

// AllocateTask
/*
	检测是否有未完成任务
	并分配任务给worker
*/
func (c *Coordinator) AllocateTask(request *RequestTask, reply *TaskInfo) error {
	c.Mutex.Lock()
	if c.CurrentTaskType == 0 {
		//当前是Map阶段
		mapFinishFlag := true
		for i := 0; i < len(c.MapTaskFinishInfo); i++ {
			if c.MapTaskFinishInfo[i].finishStatus == 2 {
				// 检查Map任务是否完成，用于判断是否开启reduce阶段
				mapFinishFlag = false
			}
			if c.MapTaskFinishInfo[i].finishStatus == 0 {
				// 将当前任务分配给请求的work
				reply.HasTask = true
				reply.TaskIndex = i
				reply.Filename = c.Filename[i]
				reply.MapCount = -1
				reply.NReduce = c.NReduce
				reply.MapTaskType = true
				c.MapTaskFinishInfo[i].finishStatus = 2
				c.MapTaskFinishInfo[i].lastTimeStamp = time.Now().Unix()
				break
			}
		}
		if mapFinishFlag == false {
			// 仍然有map任务正在处理
			reply.MapTaskType = true
			c.Mutex.Unlock()
			return nil
		}
		if reply.HasTask == false {
			// 所有的map任务已经完成，开始进入reduce阶段
			c.CurrentTaskType = 1
		}
	} else if c.CurrentTaskType == 1 {
		// reduce阶段
		reply.MapTaskType = false
		for i := 0; i < len(c.ReduceTaskFinishInfo); i++ {
			if c.ReduceTaskFinishInfo[i].finishStatus == 0 {
				// 发现未分配的reduce任务，分配给请求方
				reply.HasTask = true
				reply.TaskIndex = i
				reply.Filename = ""
				reply.MapCount = len(c.Filename)
				reply.NReduce = c.NReduce
				c.ReduceTaskFinishInfo[i].finishStatus = 2
				c.ReduceTaskFinishInfo[i].lastTimeStamp = time.Now().Unix()
				break
			}
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckHeartBeat() {
	for {
		c.Mutex.Lock()
		// 获取当前时间
		cTimeStamp := time.Now().Unix()
		if c.CurrentTaskType == 0 {
			// check map task status
			for i := 0; i < len(c.MapTaskFinishInfo); i++ {
				if c.MapTaskFinishInfo[i].finishStatus == 2 {
					// 表示任务正在执行中，检查是否超时
					if cTimeStamp-c.MapTaskFinishInfo[i].lastTimeStamp > 10 {
						// 超时未完成任务，重置任务状态
						c.MapTaskFinishInfo[i].finishStatus = 0
					}
				}
			}
		} else {
			for i := 0; i < len(c.ReduceTaskFinishInfo); i++ {
				if c.ReduceTaskFinishInfo[i].finishStatus == 2 {
					// 表示任务正在执行中，检查是否超时
					if cTimeStamp-c.ReduceTaskFinishInfo[i].lastTimeStamp > 10 {
						// 超时未完成任务，重置任务状态
						c.ReduceTaskFinishInfo[i].finishStatus = 0
					}
				}
			}
		}
		c.Mutex.Unlock()
		time.Sleep(5 * time.Second)
	}
}

// server
/*
	开启服务
*/
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done /*
/*
	检查是否所有任务都完成
*/
func (c *Coordinator) Done() bool {
	ret := true
	// all reduce task done, return true
	c.Mutex.Lock()
	for i := 0; i < c.NReduce; i++ {
		if c.ReduceTaskFinishInfo[i].finishStatus != 1 {
			ret = false
			break
		}
	}
	c.Mutex.Unlock()
	return ret
}

// MakeCoordinator /*
/*
	创建并初始化Coordinate
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Filename = files
	c.NReduce = nReduce
	/*
		0: map task
		1: reduce task
	*/
	c.CurrentTaskType = 0
	c.MapTaskFinishInfo = make([]TaskRecordInfo, len(files))
	c.ReduceTaskFinishInfo = make([]TaskRecordInfo, nReduce)
	c.Mutex = sync.Mutex{}
	for i := 0; i < len(files); i++ {
		c.MapTaskFinishInfo[i].finishStatus = 0
		c.MapTaskFinishInfo[i].lastTimeStamp = 0
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskFinishInfo[i].finishStatus = 0
		c.ReduceTaskFinishInfo[i].lastTimeStamp = 0
	}
	// 开始监测心跳
	go c.CheckHeartBeat()
	c.server()
	return &c
}
