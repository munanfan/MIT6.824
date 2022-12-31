package mr

import (
	"os"
)
import "strconv"

type RequestTask struct {
}

type FinishTask struct {
	TaskIndex int
}

type Heartbeat struct {
	TaskIndex int
	TaskType  int
}

type TaskInfo struct {
	HasTask     bool
	Filename    string
	MapTaskType bool
	TaskIndex   int
	NReduce     int
	MapCount    int
}

type HeartbeatReply struct {
	State int
}

type WorkFinish struct {
	TaskIndex int
}

type WorkFinishReply struct {
	State int
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
