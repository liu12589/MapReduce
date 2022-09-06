package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Processing = 1
	NonExecution = 2
	Executed = 3
	Working = 1
	Timeout = 2
)

// master 结点功能：这里的任务既包含 map 任务又包含 reduce 任务
// 1、存储需要执行的任务
// 2、记录正在执行的任务

// Task 记录当前正在执行的任务
type Task struct {
	Name string
	Type int
	Status int
	mFileName string // 如果是 map 任务，记录分配给该任务的文件名字
	rFileName int    // 如果是 reduce 任务，记录分配给该任务的文件组编号
}

// 全局递增变量，作为每个 task 的名字
var taskNumber int = 0

type Master struct {
	mrecord map[string] int //记录需要map的文件， Processing表示正在执行， NonExecution表示未完成，Executed表示已完成
	rrecord map[int] int    //记录需要reduce的文件，Processing表示正执行，NonExecution表示未完成,Executed表示已经完成
	reduceFile map[int][]string //记录中间生成的文件
	taskMap map[string]*Task // 任务池，记录当前正在执行的任务
	mcount int // 记录当前已完成的 map 的任务数量
	rcount int // 记录已经完成的 reduce 任务数量
	mapFinished bool // 标志 map 任务是否已经完成
	reduceNumber int //需要执行的 reduce 数量
	mutex sync.Mutex
}

func (this *Master) GetTask (args *GetTaskRequest, reply *GetTaskResponse) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	// worker 请求任务时会有两个参数，一个是已完成任务的结果需要保存。第二个就是X.0需要给其分配任务
	// 这里是保存结果 reply
	reply.RFileName = make([]string, 0)
	reply.ReduceNumber = this.reduceNumber
	reply.MFileName = ""
	reply.TaskName = strconv.Itoa(taskNumber)
	taskNumber += 1
	// 然后分配任务,map 任务结束后分配 reduce 任务
	if this.mapFinished {
		for v, _ := range this.rrecord {
			flag := this.rrecord[v]
			if flag == Processing || flag == Executed {
				continue
			} else {
				this.rrecord[v] = Processing
				for _, filename := range this.reduceFile[v] {
					reply.RFileName = append(reply.RFileName, filename)
				}
				reply.TaskType = Reduce
				task := &Task{
					Name: reply.TaskName,
					Type: reply.TaskType,
					Status: Working,
					mFileName: reply.MFileName,
					rFileName: -1,
				}
				// 将该任务添加到当前正在执行的任务池中
				this.taskMap[reply.TaskName] = task

				// 开启一个协程监听该任务是否成功
				go this.HandleTimeout(reply.TaskName)
				return nil
			}
		}
		reply.TaskType = Sleep
		return nil
	} else {
		for v,_ := range this.mrecord {
			flag := this.mrecord[v]
			if flag == Processing || flag == Executed {
				continue
			} else {
				this.mrecord[v] = Processing
				reply.MFileName = v
				reply.TaskType = Map
				task := &Task{
					Name:      reply.TaskName,
					Type:      reply.TaskType,
					Status:    Working,
					mFileName: reply.MFileName,
					rFileName: -1,
				}
				// 将该任务添加到当前正在执行的任务池中
				this.taskMap[reply.TaskName] = task

				// 开启一个协程监听该任务是否成功
				go this.HandleTimeout(reply.TaskName)
				return nil
			}
		}
		reply.TaskType = Sleep
		return nil
	}
	return nil
}

func (this *Master) Report(args *WorkStatusRequest, reply *ResponseWorkStatus) error {
	reply.X = 1
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if task, ok := this.taskMap[args.TaskName]; ok {
		flag := task.Status
		if flag == Timeout {
			// 如果该任务已经被判为超时，则从该任务池中删除
			delete(this.taskMap, args.TaskName)
			return nil
		}
		taskType := task.Type
		if taskType == Map {
			filename := task.mFileName
			this.mrecord[filename] = Executed
			this.mcount +=1
			// 已完成的map数量等于要完成的数量时，标记当前map任务已经全部完成
			if this.mcount == len(this.mrecord){
				this.mapFinished = true
			}
			// 这里是将该 work 完成的 map 结果添加到 reducefile 中
			for _, v := range args.FilesName {
				index := strings.LastIndex(v, "_")
				num, err := strconv.Atoi(v[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				this.reduceFile[num] = append(this.reduceFile[num], v)
			}
			delete(this.taskMap, task.Name)
			return nil
		}else if taskType == Reduce {
			filename := task.rFileName
			this.rrecord[filename] = Executed
			this.rcount += 1
			delete(this.taskMap, task.Name)
			return nil
		}else {
			log.Fatal("task type is not map and reduce")
		}
	}
	log.Printf("%s task is not int master record\n", args.TaskName)
	return nil
}

// HandleTimeout 心跳检测机制
// 该协程先睡十秒钟，然后检测该任务是否完成，如果未完成则进行相应操作
func (this *Master) HandleTimeout(taskName string) {
	time.Sleep(time.Second * 10)
	// 注意这里要对容器中的数据进行操作时要加锁
	this.mutex.Lock()
	defer this.mutex.Unlock()
	// 检测任务池中该任务执行情况
	if task, ok := this.taskMap[taskName]; ok {
		task.Status = Timeout

		if task.Type == Map {
			filename := task.mFileName
			if this.mrecord[filename] == Processing {
				this.mrecord[filename] = NonExecution
			}
		}else if task.Type == Reduce {
			filename := task.rFileName
			if this.rrecord[filename] == Processing {
				this.rrecord[filename] = NonExecution
			}
		}
	}
}

// 开启一个协程用于监听 worker 请求
func (this *Master) server() {
	rpc.Register(this)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("listen successed")
	go http.Serve(l, nil)
}

// Done 调用该函数检测任务是否全部完成
func (m *Master) Done() bool {
	ret := false
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.rcount==m.reduceNumber {
		ret = true
	}
	return ret
}

// MakeMaster 初始化一个新的 Master
func MakeMaster(files []string, nReduce int) *Master{
	m := Master{
		mrecord: make(map[string]int),//记录需要map的文件，0表示未执行，1表示正在执行,2表示已经完成
		rrecord: make(map[int]int),//记录需要reduce的文件，0表示未执行，1表示正在执行,2表示已经完成
		reduceFile: make(map[int][]string),//记录中间文件
		taskMap :make(map[string]*Task),
		mcount :0,//记录已经完成map的任务数量
		rcount :0,//记录已经完成的reduce的任务数量
		mapFinished:false,//
		reduceNumber :nReduce,//需要执行的reduce的数量
		mutex : sync.Mutex{},
	}
	for _,f:=range files{
		m.mrecord[f]=0
	}
	for i:=0;i<nReduce;i++{
		m.rrecord[i]=0
	}
	m.server()
	return &m
}
