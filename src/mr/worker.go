package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// // CallExample()
	args := Args{}
	reply := Reply{}
	for {
		call("Coordinator.RpcHandlers", &args, &reply)
		if reply.IsMap {
			Map(mapf, args, reply)
		} else {
			Reduce(reducef, args, reply)
		}
		args = Args{}
		reply = Reply{}
		time.Sleep(time.Second)
	}

}

func Reduce(reducef func(string, []string) string, args Args, reply Reply) {
	kva := make(map[string][]string)
	for i := 1; i <= reply.NReduce; i++ {
		file, err := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ID))
		if err != nil {
			log.Fatalf("cannot open mr-"+strconv.Itoa(reply.ID)+"-"+strconv.Itoa(i), reply.Name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}
	data := make(map[string]string)
	for i := range kva {
		tmp := reducef(i, kva[i])
		data[i] = tmp
	}
	file, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot create file")
	}
	for i := range data {
		file.WriteString(i + " " + data[i] + "\n")
	}
	os.Rename(file.Name(), "mr-out-"+strconv.Itoa(reply.ID))
}

func Map(mapf func(string, string) []KeyValue, args Args, reply Reply) {
	file, err := os.Open(reply.Name)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Name)
	}
	file.Close()
	kva := mapf(reply.Name, string(content))
	enc := []json.Encoder{}
	for i := 1; i <= reply.NReduce; i++ {
		file, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot create file")
		}
		enc = append(enc, *json.NewEncoder(file))
		os.Rename(file.Name(), "mr-"+strconv.Itoa(reply.ID)+"-"+strconv.Itoa(i))
	}
	for i := range kva {

		hash := ihash(kva[i].Key) % reply.NReduce
		enc[hash].Encode(kva[i])
	}
}

//

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
	return err == nil
}
