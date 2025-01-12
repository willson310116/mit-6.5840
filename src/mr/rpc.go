package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type Task struct {
	Type      TaskType
	FilePath  string
	Id        int
	NumReduce int
}

type TaskRequest struct {
	WorkerPid int
}

type TaskReply struct {
	Task Task
}

type TaskDoneRequest struct {
	WorkerPid int
}

type TaskDoneReply struct {
	// nothing to reply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
