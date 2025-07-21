package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type runningTask struct {
	taskInfo
	starTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	tasks       chan taskInfo
	wgMap       sync.WaitGroup
	wgReduce    sync.WaitGroup
	taskReturn  chan string
	mu          sync.RWMutex
	maxWorkerId int
	running     map[string]runningTask
	jobComplete bool
}

func (c *Coordinator) timeout() {
	for range time.Tick(1 * time.Second) {
		c.mu.Lock()
		for _, task := range c.running {
			if time.Since(task.starTime) >= 10*time.Second {
				newTask := taskInfo{
					Task:        task.Task,
					Input:       task.Input,
					ReduceCount: task.ReduceCount,
					workerId:    c.maxWorkerId,
				}
				c.maxWorkerId++
				c.tasks <- newTask
			}
		}
		c.mu.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	select {
	case task := <-c.tasks:
		if task.Task == taskReduce {
			c.wgMap.Wait() // 等待所有Map任务完成
		}
		reply.taskInfo = task
		c.mu.RLock()
		defer c.mu.RUnlock()
		if t, ok := c.running[task.Input]; ok {
			msg := fmt.Errorf("[Error] Master: duplicate assign task :%s", t.Input)
			log.Panic(msg)
		}
		c.running[task.Input] = runningTask{taskInfo: task, starTime: time.Now()}
	case <-time.After(3 * time.Second):
		reply.Task = taskWait
	}
	return nil
}

func (c *Coordinator) ReturnTask(args *ReturnTaskArgs, reply *ReturnTaskReply) error {
	switch args.Task {
	case taskMap:
		c.wgMap.Done()
	case taskReduce:
		c.wgReduce.Done()
	default:
		msg := fmt.Errorf("[Warnning] Master: error value: %v", args.Task)
		log.Print(msg.Error())
		return msg
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.taskReturn <- args.Input
	delete(c.running, args.Input)
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
	// Your code here.
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.jobComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		tasks:       make(chan taskInfo, len(files)),
		wgMap:       sync.WaitGroup{},
		wgReduce:    sync.WaitGroup{},
		taskReturn:  make(chan string, len(files)),
		mu:          sync.RWMutex{},
		maxWorkerId: 0,
		running:     make(map[string]runningTask),
		jobComplete: false,
	}

	c.wgReduce.Add(nReduce)

	// Your code here.
	go func() {
		c.wgReduce.Wait()
		c.mu.Lock()
		c.jobComplete = true
		c.mu.Unlock()
	}()
	go c.timeout()
	c.server()
	return &c
}
