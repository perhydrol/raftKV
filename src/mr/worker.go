package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var id int
var reduceCount int
var phase taskType

const (
	json_model = iota
	reduce_model
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		log.Print("[Info] Worker: Worker: Get a new task")
		task := GetTask()
		switch task.Task {
		case taskMap:
			log.Printf("[Info] Worker: Worker: get a map task, reduceCount: %d, id:%d", task.ReduceCount, task.WorkerId)
			id = task.WorkerId
			reduceCount = task.ReduceCount
			phase = taskMap
			output := RunMap(mapf, task.Input)
			ReturnTask(task.Input, output)
		case taskReduce:
			log.Printf("[Info] Worker: Worker: get a reduce task, reduceCount: %d, id:%d", task.ReduceCount, task.WorkerId)
			id = task.WorkerId
			reduceCount = task.ReduceCount
			phase = taskReduce
			output := RunReduce(reducef, task.Input)
			ReturnTask(task.Input, output)
		case taskWait:
			time.Sleep(time.Second)
		default:
			log.Printf("[Error] Worker: Worker: Error return value: %v, Worker will close", task)
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func panicLog(msg error) {
	log.Fatal(msg.Error())
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func GetTask() taskInfo {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		log.Print("[Warning] Worker: GetTask: failed to get input task. Worker will try again.")
		return taskInfo{
			Task: taskWait,
		}
	}
	return taskInfo(reply)
}

func kvToFile(kvInput <-chan KeyValue, outputFileName string, modle int, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.OpenFile(outputFileName+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		msg := fmt.Errorf("file to create %s. Option(err: %w)", outputFileName, err)
		panicLog(fmt.Errorf("[Error] Worker: kvToFile: %w", msg))
	}
	defer file.Close()
	buf := bufio.NewWriter(file)
	switch modle {
	case json_model:
		enc := json.NewEncoder(buf)
		for kv := range kvInput {
			if err := enc.Encode(kv); err != nil {
				msg := fmt.Errorf("file to write %s in %s. Option(err: %w)", kv, outputFileName, err)
				panicLog(fmt.Errorf("[Error] Worker: kvToFile: %w", msg))
			}
		}
	case reduce_model:
		for kv := range kvInput {
			fmt.Fprintf(buf, "%v %v\n", kv.Key, kv.Value)
		}
	}
	if err := buf.Flush(); err != nil {
		msg := fmt.Errorf("buf: file to flush in %s. Option(err: %w)", outputFileName, err)
		panicLog(fmt.Errorf("[Error] Worker: kvToFile: %w", msg))
	}
	if err := os.Rename(outputFileName+".tmp", outputFileName); err != nil {
		log.Printf("[Error] Worker: kvToFile: rename %s to %s filed", outputFileName+".tmp", outputFileName)
	}
}

func RunMap(mapf func(string, string) []KeyValue, input string) (output []string) {
	filename := input
	file, err := os.Open(filename)
	if err != nil {
		msg := fmt.Errorf("cannot open %v", filename)
		panicLog(fmt.Errorf("[Error] Worker: RunMap: %w", msg))
	}
	content, err := io.ReadAll(file)
	if err != nil {
		msg := fmt.Errorf("cannot read %v", filename)
		panicLog(fmt.Errorf("[Error] Worker: RunMap: %w", msg))
	}
	file.Close()
	kva := mapf(filename, string(content))

	wg := sync.WaitGroup{}
	portationChan := make([]chan KeyValue, reduceCount)
	for i := 0; i < reduceCount; i++ {
		if portationChan[i] == nil {
			portationChan[i] = make(chan KeyValue, 1)
		}
		wg.Add(1)
		outFileName := fmt.Sprintf("mr-%d-%d", id, i)
		output = append(output, outFileName)
		go kvToFile(portationChan[i], outFileName, json_model, &wg)
	}

	for _, kv := range kva {
		portationId := ihash(kv.Key) % reduceCount
		portationChan[portationId] <- kv
	}
	for _, c := range portationChan {
		close(c)
	}
	wg.Wait()
	return
}

func getPortationFiles(input string) (portationFiles []string) {
	log.Printf("[Info] Worker: getPortationFiles: portation id is :%s", input)
	pattern := regexp.MustCompile(`^mr-\d+-\d+$`)
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			msg := fmt.Errorf("failed to search portation %s file. Option(err: %w)", input, err)
			panicLog(fmt.Errorf("[Error] Worker: getPortationFiles: %w", msg))
		}

		if !d.Type().IsRegular() {
			return nil
		}

		fileName := filepath.Base(path)
		filePortationId := strings.Split(fileName, "-")
		if pattern.MatchString(fileName) && filePortationId[len(filePortationId)-1] == input {
			portationFiles = append(portationFiles, fileName)
		}
		return nil
	})
	if err != nil {
		msg := fmt.Errorf("failed to walk dir. Option(err: %w)", err)
		panicLog(fmt.Errorf("[Error] Worker: getPortationFiles: %w", msg))
	}
	return
}

func readKvFile(output chan<- KeyValue, fileName string) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		msg := fmt.Errorf("fileName %s. Option(err: %w)", fileName, err)
		panicLog(fmt.Errorf("[Error] Worker: readKvFile: %w", msg))
	}
	defer file.Close()
	buf := bufio.NewReader(file)
	dec := json.NewDecoder(buf)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		output <- kv
	}
}

func RunReduce(reducef func(string, []string) string, input string) (outputList []string) {
	portationFiles := getPortationFiles(input)
	intermediate := make([]KeyValue, 0)
	readChan := make(chan KeyValue, 100)
	readWg := sync.WaitGroup{}
	for _, fileName := range portationFiles {
		readWg.Add(1)
		fileName := fileName
		go func() {
			defer readWg.Done()
			readKvFile(readChan, fileName)
		}()
	}

	go func() {
		readWg.Wait()
		close(readChan)
	}()
	for kv := range readChan {
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%s", input)
	outputList = append(outputList, oname)
	wg := sync.WaitGroup{}
	wg.Add(1)
	outputChan := make(chan KeyValue, 100)
	go kvToFile(outputChan, oname, reduce_model, &wg)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		outputChan <- KeyValue{Key: intermediate[i].Key, Value: output}

		i = j
	}
	close(outputChan)
	wg.Wait()
	return
}

func ReturnTask(input string, output []string) {
	args := ReturnTaskArgs{
		Input:    input,
		Output:   output,
		Task:     phase,
		WorkerId: id,
	}
	reply := ReturnTaskReply{}
	if ok := call("Coordinator.ReturnTask", &args, &reply); ok {
		log.Printf("[Info] Worker: ReturnTask: input %s success return, completely task.", input)
	} else {
		log.Fatalf("[Error] Worker: ReturnTask: input %s failed return.", input)
	}
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("[Error] Worker: call: dialing fail. Option(err: %v)", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
