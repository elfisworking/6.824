package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Flag struct {
	processing bool
	finished bool
}

type Coordinator struct {
	// Your definitions here.
	FileNames []string
	MapFlags []Flag
	ReduceFalgs []Flag
	MapTaskCnts []int
	ReduceTaskCnts []int
	MapAllDone bool
	ReducaAllDone bool
	MapNum int
	ReduceNum int
	Mut sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (w *Coordinator)HandleTimeOut(arg * NoArgs, reply * NoReply) error {
	for {
		w.Mut.Lock()
		// fmt.Printf("do HandleTime out\n")
		if w.MapAllDone && w.ReducaAllDone {
			w.Mut.Unlock()
			break
		}
		time.Sleep(30 * time.Millisecond)
		if !w.MapAllDone {
			for idx := 0; idx < w.MapNum; idx++ {
				if w.MapFlags[idx].finished == false {
					w.MapFlags[idx].processing = false
				}
			}
		} else {
			for idx := 0; idx < w.ReduceNum; idx++ {
				if w.ReduceFalgs[idx].finished == false {
					w.ReduceFalgs[idx].processing = false
				}
			}
		}
		w.Mut.Unlock()
		time.Sleep(2000 * time.Microsecond)
	}
	return nil
}

func (w *Coordinator) CreateWorkerTask(args *NoArgs, workerTask *WorkerTask) error {
	w.Mut.Lock()
	defer w.Mut.Unlock()
	if !w.MapAllDone {
		// fmt.Println("CreateWotkerTask")
		for idx := 0; idx < w.MapNum; idx++ {
			if !w.MapFlags[idx].processing && !w.MapFlags[idx].finished {
				workerTask.ReduceNum = w.ReduceNum
				workerTask.MapNUm = w.MapNum
				workerTask.State = MapState
				// fmt.Println("set MapState")
				workerTask.MapID = idx
				workerTask.FileName = w.FileNames[idx]
				workerTask.MapTaskCnt = w.MapTaskCnts[idx]
				w.MapFlags[idx].processing = true
				return nil
			}
		}
		workerTask.State = WaitState
		return nil
	}
	if !w.ReducaAllDone {
		for idx := 0; idx < w.ReduceNum; idx++ {
			if !w.ReduceFalgs[idx].processing && !w.ReduceFalgs[idx].finished {
				workerTask.State = ReduceState
				workerTask.ReduceNum = w.ReduceNum
				workerTask.MapNUm = w.MapNum
				workerTask.ReduceID = idx
				w.ReduceTaskCnts[idx]++
				workerTask.ReduceTaskCnt = w.ReduceTaskCnts[idx]
				w.ReduceFalgs[idx].processing = true
				return nil
			}
		}
		workerTask.State = WaitState
		return nil
	}
	workerTask.State = WaitState
	return nil
}

func (w * Coordinator) HandlerWorkerReport(wr * WorkerReportArgs, task * NoReply) error {
	w.Mut.Lock()

	if wr.IsSuccess {
		if wr.State == MapState {
			if wr.MapTaskCnt == w.MapTaskCnts[wr.MapID] {
				w.MapFlags[wr.MapID].finished = true
				w.MapFlags[wr.MapID].processing = false
			}
		} else {
			if wr.ReduceTaskCnt == w.ReduceTaskCnts[wr.ReduceID] {
				w.ReduceFalgs[wr.ReduceID].finished = true
				w.ReduceFalgs[wr.ReduceID].processing = false
			}
		}
	} else {
		if wr.State == MapState {
			if w.MapFlags[wr.MapID].finished == false {
				w.MapFlags[wr.MapID].processing = false
			}
		} else {
			if w.ReduceFalgs[wr.ReduceID].finished == false {
				w.ReduceFalgs[wr.ReduceID].processing = false
			}
		}
	}
	for id := 0; id < w.MapNum; id++ {
		if !w.MapFlags[id].finished {
			break
		} else if id == w.MapNum - 1 {
			w.MapAllDone = true
			// fmt.Printf("all map work done over\n")
		}
	}

	for id := 0; id < w.ReduceNum; id++ {
		if !w.ReduceFalgs[id].finished {
			break
		} else if id == w.ReduceNum - 1 {
			w.ReducaAllDone = true
			// fmt.Printf("all reduce work done over\n")
		}
	} 
	w.Mut.Unlock()
	return nil
}






//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done(args *NoArgs, reply *MasterDoneReply) error {
	m.Mut.Lock()
	reply.Done = m.MapAllDone && m.ReducaAllDone
	m.Mut.Unlock()
	return nil
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames: files,
		MapFlags: make([]Flag, len(files), len(files)),
		ReduceFalgs: make([]Flag, nReduce, nReduce),
		MapNum: len(files),
		ReduceNum: nReduce,
		MapAllDone: false,
		ReducaAllDone: false,
		MapTaskCnts: make([]int, len(files)),
		ReduceTaskCnts: make([]int, nReduce),
	}
	args, reply := NoArgs{}, NoReply{}
	go c.HandleTimeOut(&args, &reply)
	c.server()
	return &c
}
