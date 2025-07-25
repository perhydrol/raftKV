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
	completeMap      sync.Map
	nReduce          int
	mapTasksCount    int32
	reduceTasksCount int32
	maxWorkerId      int32
	running          sync.Map
	jobComplete      int32
	closeChan        chan interface{}
}

func (c *Coordinator) timeout() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.running.Range(func(key, value any) bool {
				k, ok := key.(int)
				if !ok {
					log.Panicf("[Error] Master: timeout: key cannot be resolved. key: %v", key)
					return true
				}

				if value == nil {
					return true
				}
				// 使用类型断言前先获取值的副本，避免并发修改
				v, ok := value.(runningTask)
				if !ok {
					log.Panicf("[Error] Master: timeout: value cannot be resolved. value: %v", value)
					return true
				}

				if time.Since(v.starTime) >= 10*time.Second {
					log.Printf("[Info] Master: timeout: worker id %d is timed out, retrying...", k)
					newTask := taskInfo{
						Task:        v.Task,
						Input:       v.Input,
						ReduceCount: v.ReduceCount,
						WorkerId:    int(atomic.LoadInt32(&c.maxWorkerId)),
					}
					atomic.AddInt32(&c.maxWorkerId, 1)
					c.tasks <- newTask
				}
				return true
			})
		case <-c.closeChan:
			return
		}
	}
}

func (c *Coordinator) delRunning(input string) {
	c.running.Range(
		func(key, value any) bool {
			if value == nil {
				return true
			}
			// 使用类型断言前先获取值的副本，避免并发修改
			v, ok := value.(runningTask)
			if !ok {
				log.Panicf("[Error] Master: delRunning: value cannot be resolved. value: %v", value)
				return true
			}
			if v.Input == input {
				c.running.Delete(key)
			}
			return true
		})
}

func (c *Coordinator) allMapDone() {
	c.wgMap.Wait()
	log.Print("[Info] Master: allMapDone: all map tasks finished.")
	for i := 0; i < c.nReduce; i++ {
		log.Printf("[Info] Master: allMapDone: creating reduce task %d.", i)
		idString := fmt.Sprintf("%d", i)
		newTask := taskInfo{
			Task:        taskReduce,
			Input:       idString,
			ReduceCount: c.nReduce,
			WorkerId:    int(atomic.LoadInt32(&c.maxWorkerId)),
		}
		atomic.AddInt32(&c.maxWorkerId, 1)
		c.tasks <- newTask

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
	select {
	case task := <-c.tasks:
		log.Printf("[Info] Master: GetTask: assigned task %s.task.WorkerId: %d", task.Input, task.WorkerId)
		reply.Input = task.Input
		reply.ReduceCount = task.ReduceCount
		reply.Task = task.Task
		reply.WorkerId = task.WorkerId
		newRunning := runningTask{taskInfo: task, starTime: time.Now()}
		if t, ok := c.running.LoadOrStore(task.WorkerId, newRunning); ok {
			msg := fmt.Errorf("duplicate task assignment: %s", t.(runningTask).Input)
			log.Panicf("[Error] Master: GetTask: %v", msg)
		}
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
		c.delRunning(args.Input)
	case taskReduce:
		log.Printf("[Info] Master: ReturnTask: reduce task %s completed.", args.Input)
		atomic.AddInt32(&c.reduceTasksCount, -1)
		if atomic.LoadInt32(&c.reduceTasksCount) >= 0 {
			log.Printf("[Info] Master: ReturnTask: reduce task finished, remaining: %d.", atomic.LoadInt32(&c.reduceTasksCount))
			c.wgReduce.Done()
		}
		c.delRunning(args.Input)
	default:
		msg := fmt.Errorf("invalid task type: %v", args.Task)
		log.Printf("[Warning] Master: ReturnTask: %v", msg)
		return msg
	}
	c.running.Delete(args.WorkerId)
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
	return atomic.LoadInt32(&c.jobComplete) > 0
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
		mapTasksCount:    int32(len(files)),
		reduceTasksCount: int32(nReduce),
		maxWorkerId:      0,
		running:          sync.Map{},
		jobComplete:      0,
		closeChan:        make(chan interface{}),
	}

	c.wgMap.Add(len(files))
	for _, input := range files {
		newTask := taskInfo{
			Task:        taskMap,
			Input:       input,
			ReduceCount: nReduce,
			WorkerId:    int(atomic.LoadInt32(&c.maxWorkerId)),
		}
		atomic.AddInt32(&c.maxWorkerId, 1)
		c.tasks <- newTask
	}
	c.wgReduce.Add(nReduce)

	// Your code here.
	go func() {
		c.wgReduce.Wait()
		log.Print("[Info] Master: Job has done!")
		close(c.closeChan)
		atomic.AddInt32(&c.jobComplete, 1)
	}()
	go c.timeout()
	go c.allMapDone()
	c.server()
	return &c
}
