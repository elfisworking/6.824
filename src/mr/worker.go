package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wt * WorkerTask) GetWorkTask() {
	cwa := NoArgs{}
	newWt := WorkerTask{}
	success := call("Coordinator.CreateWorkerTask", &cwa, &newWt)
	if !success {
		newWt.State = StopState
	}
	if newWt.State == MapState {
		wt.ReduceNum = newWt.ReduceNum
		wt.MapNUm = newWt.MapNUm
		wt.State = newWt.State
		wt.MapID = newWt.MapID
		wt.FileName = newWt.FileName
		wt.MapTaskCnt = newWt.MapTaskCnt
	} else if newWt.State == ReduceState {
		wt.State = newWt.State
		wt.ReduceID = newWt.ReduceID
		wt.ReduceTaskCnt = newWt.ReduceTaskCnt
		wt.MapNUm = newWt.MapNUm
		wt.ReduceNum = newWt.ReduceNum
	} else if newWt.State == WaitState {
		wt.State = newWt.State
	} else {
		wt.State = newWt.State
	}
}

func (wt * WorkerTask) ReportWorkerTask(err error) {
	wra := WorkerReportArgs{
		MapID: wt.MapID,
		ReduceID: wt.ReduceID,
		State: wt.State,
		IsSuccess: true,
	}
	if wt.State == MapState {
		wra.MapTaskCnt = wt.MapTaskCnt
	} else {
		wra.ReduceTaskCnt = wt.ReduceTaskCnt
	}
	wrr := NoReply{}
	if err != nil {
		wra.IsSuccess = false
	}
	call("Coordinator.HandlerWorkerReport", &wra, &wrr)
}

func (wt * WorkerTask)DoMapWork() {
	file, err := os.Open(wt.FileName)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Printf("cannnot open %v\n", wt.FileName)
		return 
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Printf("cannot read %v\n", wt.FileName)
		return 
	}
	file.Close()
	kvs := wt.MapFunction(wt.FileName, string(content))
	intermediate := make([][]KeyValue, wt.ReduceNum)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % wt.ReduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < wt.ReduceNum; idx++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", wt.MapID, idx)
		file, err := os.Create(intermediateFileName)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("cannot create %v\n", intermediateFileName)
			return 
		}
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("cannot write file: %v\n", intermediateFileName)
			return 
		}
		file.Close()
	}
	// fmt.Printf("finish a map wrok\n")
	wt.ReportWorkerTask(nil)

}

func (wt *WorkerTask) DoReduceWork() {
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < wt.MapNUm; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, wt.ReduceID)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf(err.Error())
			wt.ReportWorkerTask(err)
			fmt.Printf("open file %v fail\n", filename)
			return 
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("read file %v fail\n", filename)
		}
		file.Close()
		kvs := make([]KeyValue, 0)
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("json file %v fail\n", filename)
			return 
		}
		for _, kv := range kvs {
			_, ok := kvsReduce[kv.Key]
			if !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	ReduceResult := make([]string, 0)
	for key, val := range kvsReduce {
		ReduceResult = append(ReduceResult, fmt.Sprintf("%v %v\n", key, wt.ReduceFunction(key, val)))
	}
	outFileName := fmt.Sprintf("mr-out-%d", wt.ReduceID)
	err := ioutil.WriteFile(outFileName, []byte(strings.Join(ReduceResult, "")), 0644)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Sprintf("write %v fail:", outFileName)
	}
	// fmt.Printf("finish a reduce work\n")
	wt.ReportWorkerTask(nil)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wt := WorkerTask{
		MapFunction: mapf,
		ReduceFunction: reducef,
	}
	for  {
		wt.GetWorkTask()
		if wt.State == MapState {
			wt.DoMapWork()
		} else if wt.State == ReduceState {
			wt.DoReduceWork()
		} else if wt.State == StopState {
			break
		} else if wt.State == WaitState {
			time.Sleep(300 * time.Millisecond)
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
