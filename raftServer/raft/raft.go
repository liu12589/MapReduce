package raft

import (
	"sync"
	"time"
)
import "../labrpc"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int             //记录节点当前的状态
type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower Status = iota
	Candidate
	Leader
)
const (
	MinVoteTime      = 70
	MoreVoteTime     = 170
	HeartBeatTimeout = 30
	AppliedSleep     = 10
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	currentTerm int        //记录当前任期
	votedFor    int        //当前任期把票投给了谁
	logs        []LogEntry //记录日志

	commitIndex int // 已经写入的最高日志项的索引
	lastApplied int // 已经提交的最高日志项的索引

	// leader 使用
	nextIndex  []int // 发送给每台服务器的下一个日志条目索引，初始化为leaderLast log index + 1
	matchIndex []int // 每台服务器最高的日志索引号

	status   Status
	voteNum    int       // 获得的总票数
	votedTimer time.Time //每次收到心跳后更新防止follow进行选举
	applyChan  chan ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.status != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.persist()
		return index, term, true
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()

	// initialize from status persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}