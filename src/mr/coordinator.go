package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Mode int

const (
	InMAP Mode = iota
	InREDUCE
	DONE
)

type Coordinator struct {
	// Your definitions here.
	nMap        int
	nReduce     int
	Mode        Mode
	TaskChannel chan Task
	TaskAssined map[int]Task
	doneMap     int
	doneReduce  int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SwtichMode() {
	// check every Map tasks are done
	if c.Mode == InMAP {
		c.Mode = InREDUCE
		// create Reduce tasks
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:      REDUCE,
				Id:        i,
				NumReduce: c.nReduce}
			c.TaskChannel <- task
		}
	} else if c.Mode == InREDUCE {
		c.Mode = DONE
	}
	return
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	if c.doneMap == c.nMap {
		c.SwtichMode()
	}
	assignedTask := <-c.TaskChannel
	fmt.Printf("Server assigning %v to pid %v\n", assignedTask.FilePath, args.WorkerPid)
	reply.Task = assignedTask
	c.TaskAssined[args.WorkerPid] = assignedTask
	return nil
}

func (c *Coordinator) NotifyTaskDone(args *TaskDoneRequest, reply *TaskDoneReply) error {
	if task, ok := c.TaskAssined[args.WorkerPid]; ok {
		fmt.Printf("Notified %v is done from pid: %v\n", task.Type, args.WorkerPid)
		switch task.Type {
		case MAP:
			c.doneMap += 1
		case REDUCE:
			c.doneReduce += 1
		default:
			fmt.Println(reply)
			fmt.Printf("Received invalid task\n")
		}
	} else {
		fmt.Printf("No task assgined to pid: %v\n", args.WorkerPid)
	}
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		Mode:        InMAP,
		doneMap:     0,
		doneReduce:  0,
		TaskChannel: make(chan Task, nReduce),
		TaskAssined: make(map[int]Task)}

	// create Map tasks and send into task channel
	for i, file := range files {
		task := Task{
			Type:      MAP,
			FilePath:  file,
			Id:        i,
			NumReduce: nReduce}
		c.TaskChannel <- task
	}

	// Your code here.
	// close(c.TaskChannel)

	c.server()
	return &c
}
