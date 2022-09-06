package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type KeyValue struct {
	Key   string
	Value string
}

// Worker 节点启动后循环，从 master 获取任务，执行然后上报。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		req := GetTaskRequest{}
		req.X = 0
		rep := GetTaskResponse{}
		call("Master.GetTask", &req, &rep)
		// 接收到 map 任务，对该任务进行处理
		if rep.TaskType == Map {
			filenames := HandleMap(mapf, rep.MFileName, rep.ReduceNumber, rep.TaskName)
			mapReq := WorkStatusRequest{
				TaskName: rep.TaskName,
				FilesName: filenames,
			}
			mapRep := ResponseWorkStatus {
				X: 0,
			}
			call("Master.Report", &mapReq, &mapRep)
		}else if rep.TaskType == Reduce {
			//接收到 reduce 任务，对该任务进行处理
			HandleReduce(reducef, rep.RFileName)
			reduceReq := WorkStatusRequest {
				TaskName: rep.TaskName,
				FilesName: make([]string, 0),
			}
			reduceRep := ResponseWorkStatus {
				X: 0,
			}
			call("Master.Report", &reduceReq, &reduceRep)
		}else if rep.TaskType == Sleep {
			time.Sleep(time.Millisecond * 20)
		}else {
			log.Fatal("get task is not map sleep and reduce")
		}
	}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// HandleMap 处理 map 任务
func HandleMap(mapf func(string, string) []KeyValue, filename string, fileNum int, taskName string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 要将中间结果保存发送给 master 同一分配处理
	middleFiles := make([]string, fileNum)
	files := make([]*os.File, fileNum)

	for i := 0; i < fileNum; i++ {
		oname := "mr" + "-" + taskName + "-" + strconv.Itoa(i)
		ofile,_ := os.Create(oname)
		files[i] = ofile
		middleFiles[i] = oname
	}

	/* TODO */
	for _,kv:=range intermediate{
		index:=ihash(kv.Key)%fileNum
		enc:=json.NewEncoder(files[index])
		enc.Encode(&kv)
	}

	return middleFiles
}

func HandleReduce(reducef func(string, []string) string, filenames []string)string{
	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}
	for i := 0; i < len(filenames); i++ {
		files[i],_ = os.Open(filenames[i])
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	oname := "mr-out-"
	index:=filenames[0][strings.LastIndex(filenames[0],"_")+1:]
	oname=oname+index
	ofile, _ := os.Create(oname)

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return oname
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
