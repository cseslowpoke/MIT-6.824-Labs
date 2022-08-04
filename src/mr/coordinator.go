package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Data struct {
	Name string
	ID   int
}
type Coordinator struct {
	// Your definitions here.
	mapData    []Data
	reduceData []int
	nReduce    int
	tasks      int
	now        int
	mapDone    bool
}

func (c *Coordinator) MapCheck(data Data) {
	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		f, err := os.Open("mr-" + strconv.Itoa(data.ID) + "-" + strconv.Itoa(i))
		if f != nil {
			clock.Lock()
			c.now--
			clock.Unlock()
			return
		}
		if err != nil {
			continue
		}
	}
	clock.Lock()
	c.now--
	c.mapData = append(c.mapData, data)
	clock.Unlock()
}
func (c *Coordinator) ReduceCheck(data int) {
	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		f, err := os.Open("mr-out-" + strconv.Itoa(data))
		if f != nil {
			clock.Lock()
			c.now--
			clock.Unlock()
			return
		}
		if err != nil {
			continue
		}
	}
	clock.Lock()
	c.now--
	c.reduceData = append(c.reduceData, data)
	clock.Unlock()
}

var m *sync.RWMutex
var clock *sync.RWMutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RpcHandlers(args *Args, reply *Reply) error {
	m.RLock()
	clock.Lock()
	check := c.now
	mapDataLen := len(c.mapData)
	reduceDataLen := len(c.reduceData)
	mapDone := c.mapDone
	clock.Unlock()
	if ((mapDataLen == 0 && check == 0) || mapDone) && reduceDataLen > 0 {
		clock.Lock()
		tmp := c.reduceData[0]
		c.mapDone = true
		reply.IsMap = false
		reply.ID = tmp
		reply.NReduce = c.tasks
		if len(c.reduceData) > 1 {
			c.reduceData = c.reduceData[1:]
		} else {
			c.reduceData = nil
		}
		clock.Unlock()
		clock.Lock()
		c.now++
		clock.Unlock()
		go c.ReduceCheck(tmp)
	} else if mapDataLen > 0 {
		clock.Lock()
		tmp := c.mapData[0]
		clock.Unlock()
		reply.Name = tmp.Name
		reply.NReduce = c.nReduce
		reply.ID = tmp.ID
		reply.IsMap = true
		clock.Lock()
		if len(c.mapData) > 1 {
			c.mapData = c.mapData[1:]
		} else {
			c.mapData = nil
		}
		clock.Unlock()
		clock.Lock()
		c.now++
		clock.Unlock()
		go c.MapCheck(tmp)
	}
	m.RUnlock()
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
func (c *Coordinator) Done() bool {
	clock.Lock()
	now := c.now
	mapDataLen := len(c.mapData)
	reduceDataLen := len(c.reduceData)
	// mapDone := c.mapDone
	clock.Unlock()
	// fmt.Print(mapDone)
	// fmt.Printf("%d %d %d\n", now, mapDataLen, reduceDataLen)
	if mapDataLen == 0 && reduceDataLen == 0 && now == 0 {
		fmt.Print("end")
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m = new(sync.RWMutex)
	clock = new(sync.RWMutex)
	// Your code here.
	var data []Data
	var data2 []int
	for i := range files {
		data = append(data, Data{files[i], i + 1})
	}
	for i := 1; i <= nReduce; i++ {
		data2 = append(data2, i)
	}
	c := Coordinator{mapData: data, reduceData: data2, nReduce: nReduce, tasks: len(files), now: 0, mapDone: false}
	c.server()
	return &c
}
