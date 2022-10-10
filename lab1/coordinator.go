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
	// Your definitions here.
	Filename []string
	NReduce  int
	// record the situation of task
	MapTaskFinishInfo    []TaskRecordInfo
	ReduceTaskFinishInfo []TaskRecordInfo
	// record the task type current solve
	CurrentTaskType int
	Mutex           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) SolveHearBeat(request *Heartbeat, reply *HearbeatReply) error {
	c.Mutex.Lock()
	// update the timeStamp
	if request.TaskType == 0 && c.CurrentTaskType == request.TaskType {
		// map task
		c.MapTaskFinishInfo[request.TaskIndex].lastTimeStamp = time.Now().Unix()
		//fmt.Println("receive map task " + strconv.Itoa(request.TaskIndex) + " heartbeat")
	} else if request.TaskType == 1 && c.CurrentTaskType == request.TaskType {
		// reduce task
		c.ReduceTaskFinishInfo[request.TaskIndex].lastTimeStamp = time.Now().Unix()
		//fmt.Println("receive reduce task " + strconv.Itoa(request.TaskIndex) + " heartbeat")
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) ReceiveFinish(finishInfo *FinishTask, finishReply *WorkFinishReply) error {
	//fmt.Println("receive task finish")
	c.Mutex.Lock()
	if c.CurrentTaskType == 0 {
		//fmt.Println("receive map task " + strconv.Itoa(finishInfo.TaskIndex) + " finished")
		c.MapTaskFinishInfo[finishInfo.TaskIndex].finishStatus = 1
		finishReply.State = 1
		//fmt.Println("map task " + strconv.Itoa(finishInfo.TaskIndex) + " finished")
	} else {
		//fmt.Println("receive reduce task " + strconv.Itoa(finishInfo.TaskIndex) + " finished")
		c.ReduceTaskFinishInfo[finishInfo.TaskIndex].finishStatus = 1
		finishReply.State = 1
		//fmt.Println("reduce task " + strconv.Itoa(finishInfo.TaskIndex) + " finished")
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) AllocateTask(request *RequestTask, reply *TaskInfo) error {
	// check whether map task all finish
	c.Mutex.Lock()
	if c.CurrentTaskType == 0 {
		// map task, lock the taskMap
		mapFinishFlag := true
		for i := 0; i < len(c.MapTaskFinishInfo); i++ {
			if c.MapTaskFinishInfo[i].finishStatus == 2 {
				// has task in processing
				mapFinishFlag = false
			}
			if c.MapTaskFinishInfo[i].finishStatus == 0 {
				reply.HasTask = true
				reply.TaskIndex = i
				reply.Filename = c.Filename[i]
				reply.MapCount = -1
				reply.NReduce = c.NReduce
				reply.MapTaskType = true
				c.MapTaskFinishInfo[i].finishStatus = 2
				c.MapTaskFinishInfo[i].lastTimeStamp = time.Now().Unix()
				//fmt.Println("Coordinate: allot a map task")
				break
			}
		}
		if mapFinishFlag == false {
			// has task process not finish
			reply.MapTaskType = true
			c.Mutex.Unlock()
			return nil
		}
		if reply.HasTask == false {
			//fmt.Println("all map task finish, start process reduce task")
			c.CurrentTaskType = 1
		}
	}
	if c.CurrentTaskType == 1 {
		// reduce task, lock the taskMap
		reply.MapTaskType = false
		for i := 0; i < len(c.ReduceTaskFinishInfo); i++ {
			if c.ReduceTaskFinishInfo[i].finishStatus == 0 {
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
		//fmt.Println("Coordinate: allot reduce task " + strconv.Itoa(reply.TaskIndex))
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckHeartBeat() {
	for {
		//fmt.Println("Coordinate: checking Heartbeat")
		c.Mutex.Lock()
		// get current timestamp
		cTimeStamp := time.Now().Unix()
		if c.CurrentTaskType == 0 {
			// check map task status
			for i := 0; i < len(c.MapTaskFinishInfo); i++ {
				if c.MapTaskFinishInfo[i].finishStatus == 2 {
					// stand by processing
					if cTimeStamp-c.MapTaskFinishInfo[i].lastTimeStamp > 10 {
						// reset the status to not solve
						c.MapTaskFinishInfo[i].finishStatus = 0
						//fmt.Println("map task " + strconv.Itoa(i) + " timeout")
					}
				}
			}
		} else {
			for i := 0; i < len(c.ReduceTaskFinishInfo); i++ {
				if c.ReduceTaskFinishInfo[i].finishStatus == 2 {
					// stand by processing
					if cTimeStamp-c.ReduceTaskFinishInfo[i].lastTimeStamp > 10 {
						// reset the status to not solve
						c.ReduceTaskFinishInfo[i].finishStatus = 0
						//fmt.Println("reduce task " + strconv.Itoa(i) + " timeout")
					}
				}
			}
		}
		c.Mutex.Unlock()
		time.Sleep(5 * time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// init the coordinator
	c.Filename = files
	c.NReduce = nReduce
	// 0 standby map task; 1 standby reduce task.
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
	// start check heartbeat
	go c.CheckHeartBeat()
	c.server()
	return &c
}
