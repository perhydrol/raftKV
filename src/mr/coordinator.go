package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type runningTask struct {
	taskInfo
	starTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	tasks            chan taskInfo
	wgMap            sync.WaitGroup
	wgReduce         sync.WaitGroup
	nReduce          int
	mu               sync.RWMutex
	mapTasksCount    int32
	reduceTasksCount int32
	maxWorkerId      int
	running          map[string]runningTask
	jobComplete      bool
}

func (c *Coordinator) timeout() {
	for range time.Tick(1 * time.Second) {
		c.mu.Lock()
	OuterLoop:
		for _, task := range c.running {
			if time.Since(task.starTime) >= 10*time.Second {
				log.Printf("[Warning] Master: timeout: task %s timed out.", task.Input)
				newTask := taskInfo{
					Task:        task.Task,
					Input:       task.Input,
					ReduceCount: task.ReduceCount,
					WorkerId:    c.maxWorkerId,
				}
				c.maxWorkerId++
				for {
					select {
					case c.tasks <- newTask:
						continue OuterLoop
					default:
						log.Printf("[Info] Master: timeout: task queue full, sleeping 2s.")
						c.mu.Unlock()
						time.Sleep(2 * time.Second)
						c.mu.Lock()
					}
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) allMapDone() {
	c.wgMap.Wait()
	log.Print("[Info] Master: allMapDone: all map tasks finished.")
	c.mu.Lock()
	defer c.mu.Unlock()
OuterLoop:
	for i := 0; i < c.nReduce; i++ {
		log.Printf("[Info] Master: allMapDone: creating reduce task %d.", i)
		idString := fmt.Sprintf("%d", i)
		newTask := taskInfo{
			Task:        taskReduce,
			Input:       idString,
			ReduceCount: c.nReduce,
			WorkerId:    c.maxWorkerId,
		}
		c.maxWorkerId++
		for {
			select {
			case c.tasks <- newTask:
				continue OuterLoop
			default:
				log.Print("[Info] Master: allMapDone: task queue full, sleeping 2s.")
				c.mu.Unlock()
				time.Sleep(2 * time.Second)
				log.Print("[Info] Master: allMapDone: retrying to assign reduce task.")
				c.mu.Lock()
			}
		}
	}
	log.Print("[Info] Master: allMapDone: all reduce tasks created.")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case task := <-c.tasks:
		log.Printf("[Info] Master: GetTask: assigned task %s.", task.Input)
		reply.Input = task.Input
		reply.ReduceCount = task.ReduceCount
		reply.Task = task.Task
		reply.WorkerId = task.WorkerId
		if t, ok := c.running[task.Input]; ok {
			msg := fmt.Errorf("duplicate task assignment: %s", t.Input)
			log.Panicf("[Error] Master: GetTask: %v", msg)
		}
		c.running[task.Input] = runningTask{taskInfo: task, starTime: time.Now()}
	case <-time.After(3 * time.Second):
		log.Print("[Info] Master: GetTask: no task available.")
		reply.Task = taskWait
	}
	return nil
}

func (c *Coordinator) ReturnTask(args *ReturnTaskArgs, reply *ReturnTaskReply) error {
	switch args.Task {
	case taskMap:
		log.Printf("[Info] Master: ReturnTask: map task %s completed.", args.Input)
		atomic.AddInt32(&c.mapTasksCount, -1)
		if atomic.LoadInt32(&c.mapTasksCount) >= 0 {
			log.Printf("[Info] Master: ReturnTask: map task finished, remaining: %d.", atomic.LoadInt32(&c.mapTasksCount))
			c.wgMap.Done()
		}
	case taskReduce:
		log.Printf("[Info] Master: ReturnTask: reduce task %s completed.", args.Input)
		atomic.AddInt32(&c.reduceTasksCount, -1)
		if atomic.LoadInt32(&c.reduceTasksCount) >= 0 {
			log.Printf("[Info] Master: ReturnTask: reduce task finished, remaining: %d.", atomic.LoadInt32(&c.reduceTasksCount))
			c.wgReduce.Done()
		}
	default:
		msg := fmt.Errorf("invalid task type: %v", args.Task)
		log.Printf("[Warning] Master: ReturnTask: %v", msg)
		return msg
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
		log.Fatalf("[Error] Master: server: listen error: %v", e)
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
	log.Printf("[Info] Master: MakeCoordinator: starting coordinator with nReduce=%d.", nReduce)
	c := Coordinator{
		tasks:            make(chan taskInfo, len(files)),
		wgMap:            sync.WaitGroup{},
		wgReduce:         sync.WaitGroup{},
		nReduce:          nReduce,
		mu:               sync.RWMutex{},
		mapTasksCount:    int32(len(files)),
		reduceTasksCount: int32(nReduce),
		maxWorkerId:      0,
		running:          make(map[string]runningTask),
		jobComplete:      false,
	}

	c.wgMap.Add(len(files))
	for _, input := range files {
		newTask := taskInfo{
			Task:        taskMap,
			Input:       input,
			ReduceCount: nReduce,
			WorkerId:    c.maxWorkerId,
		}
		c.maxWorkerId++
		c.tasks <- newTask
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
	go c.allMapDone()
	c.server()
	return &c
}
