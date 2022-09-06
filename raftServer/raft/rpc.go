package raft

type RequestVoteArgs struct {
	Term         int // 请求投票的任期
	CandidateId  int // 请求投票的编号
	LastLogIndex int // 请求投票的最新日志编号
	LastLogTerm  int // 请求投票的罪行日志任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       //投票方的term
	VoteGranted bool      //是否投给该竞选人
}

type AppendEntriesArgs struct {
	Term         int        // leader 的当前任期
	LeaderId     int        // leader的id
	PrevLogIndex int        // 已经记录的日志最后一个索引
	PrevLogTerm  int        // 最后一个已经记录日志的任期
	Entries      []LogEntry // 要存储的日志条目
	LeaderCommit int        // 最后一个被大多数机器都复制的日志index
}

type AppendEntriesReply struct {
	Term     int
	Success  bool // 是否成功
	UpNextIndex int  // false 是匹配，true 是不匹配
}

type InstallSnapshotArgs struct {
	Term             int   // 请求方的任期
	LeaderId         int   // 请求方的LeaderID
	LastIncludeIndex int   // 快照最后applied的日志下标
	LastIncludeTerm  int   // 快照最后applied的当前任期
	Data             []byte // 快照存储的数据
}

type InstallSnapshotReply struct {
	Term int
}

