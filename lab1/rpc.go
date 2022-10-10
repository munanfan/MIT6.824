package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// request message type
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

type HearbeatReply struct {
	State int
}

type WorkFinish struct {
	TaskIndex int
}

type WorkFinishReply struct {
	State int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
