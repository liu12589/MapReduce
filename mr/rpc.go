package mr

import (
	"os"
	"strconv"
)

// rpc主要有两个功能
// 1、worker 获取任务
// 2、worker 上报任务完成

// GetTaskRequest 请求任务结构体，本身不携带信息，只要收到消息就发送
type GetTaskRequest struct {
	X int
}

// GetTaskResponse 对于回复任务请求
type GetTaskResponse struct {
	MFileName string
	TaskName string
	RFileName []string
	TaskType int       // 任务类型，不知道任务是 map 还是 reduce。因此需要区分 1:Map;2:Reduce;3:Sleep
	ReduceNumber int   // 中间文件分组的数量
}

// WorkStatusRequest work 任务上报
type WorkStatusRequest struct {
	FilesName []string
	TaskName string
}

// ResponseWorkStatus 回复 work 上报状态请求的。这里不需要回复故不携带信息
type ResponseWorkStatus struct {
	X int
}

const (
	Map = 1
	Reduce = 2
	Sleep = 3
)

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
