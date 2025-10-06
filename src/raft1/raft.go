package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const debug = true

const (
	electionTimeOut = 300
	heartTimeOut    = 50
)

type nodeState int32

const (
	leader nodeState = iota
	follower
	candidate
)

func (rf *Raft) logPrintf(format string, a ...interface{}) {
	if !debug {
		return
	} else {
		newFormat := fmt.Sprintf("[%s] Id:%d Term:%d State:%d lastLogIndex:%d commitIndex:%d: %s\n",
			time.Now(), rf.me, rf.currentTerm, rf.state, rf.log.EndIndex, rf.commitIndex, format)
		fmt.Printf(newFormat, a...)
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	currentTerm int
	votedFor    int
	log         logList

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state nodeState

	electionTimer *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh   chan raftapi.ApplyMsg
	heartDict map[int]bool
}

type logList struct {
	Log        []Entry
	EndIndex   int
	BeginIndex int
	Size       int
}

func (l *logList) Get(logIndex int) Entry {
	if (logIndex - l.BeginIndex) < 0 {
		msg := fmt.Sprintf("logIndex: %d, l.BeginIndex: %d\n", logIndex, l.BeginIndex)
		fmt.Println(msg)
		panic(msg)
	}
	return l.Log[logIndex-l.BeginIndex]
}

func (l *logList) Set(logIndex int, entry Entry) {
	l.Log[logIndex-l.BeginIndex] = entry
	l.BeginIndex = l.Log[0].Index
	l.EndIndex = l.Log[len(l.Log)-1].Index
	l.Size = len(l.Log)
}

func (l *logList) Append(logs []Entry) {
	l.Log = append(l.Log, logs...)
	l.BeginIndex = l.Log[0].Index
	l.EndIndex = l.Log[len(l.Log)-1].Index
	l.Size = len(l.Log)
}

func (l *logList) AppendList(target int, logs []Entry) {
	l.Log = l.Log[:target]
	l.Append(logs)
}

func (l *logList) GetLast() Entry {
	return l.Log[len(l.Log)-1]
}

func (l *logList) GetBegin() Entry {
	return l.Log[0]
}

func (l *logList) GetSlice(begin, end int) []Entry {
	if end == -1 {
		return l.Log[begin:]
	}
	return l.Log[begin:end]
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of conficting entry
	XIndex  int // index of first conficting entry or XTerm
	XLen    int // length of loss logs
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.GetAppendEntries", args, reply)
	return ok
}

func (rf *Raft) GetAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf("get a AppendEntries RPC. leader:%d, Term:%d, PrevLogIndex:%d, PrevLogTerm:%d.", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		rf.logPrintf("refuse becouse rpc's term < rf.Term")
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, args.Term)
		reply.Term = args.Term
		// 进入下一个任期，刷新投票
		rf.changeState(follower, -1, args.Term)
		reply.Success = rf.replyAppendEntries(args, reply)
		return
	}
	refuse := args.PrevLogIndex > rf.log.GetLast().Index ||
		args.PrevLogTerm != rf.log.Get(args.PrevLogIndex).Term
	if refuse {
		rf.logPrintf("refuse AppendEntries RPC: rf.log.GetLast().Index: %d, rf.log.GetLast().Term: %d", rf.log.GetLast().Index, rf.log.GetLast().Term)
		reply.Term = rf.currentTerm
		reply.Success = rf.replyAppendEntries(args, reply)
		return
	}
	rf.logPrintf("conform AppendEntries RPC")
	reply.Term = rf.currentTerm
	// 任期不变，不允许刷新投票
	rf.resetElection()
	reply.Success = rf.replyAppendEntries(args, reply)
}

func (rf *Raft) replyAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (isSuccess bool) {
	// 正常合并
	if rf.currentTerm == args.Term && args.PrevLogIndex <= rf.log.GetLast().Index {
		prevLogEntry := rf.log.Get(args.PrevLogIndex)
		if prevLogEntry.Term == args.PrevLogTerm {
			rf.logPrintf("acquire Entry from leader(%d): PrevLogIndex: %d, rpcEntiresSize: %d, rf.log.Last.Index(before append): %d, commitIndex: %d",
				args.LeaderId, args.PrevLogIndex, len(args.Entries), rf.log.GetLast().Index, args.LeaderCommit)
			if args.Entries != nil {
				rf.log.AppendList(args.PrevLogIndex+1, args.Entries)
			}
			rf.commitLogBeforIndex(args.LeaderCommit, args.Term)
			rf.persist()
			return true
		}
	}
	// 校验失败，需要检查冲突项目
	rf.logPrintf("refuse Entry from leader(%d): PrevLogIndex: %d, rpcEntiresSize: %d, rf.log.Last.Index(before append): %d, commitIndex: %d",
		args.LeaderId, args.PrevLogIndex, len(args.Entries), rf.log.GetLast().Index, args.LeaderCommit)
	if args.PrevLogIndex <= rf.log.GetLast().Index {
		reply.XTerm = rf.log.Get(args.PrevLogIndex).Term
		reply.XIndex = 0
		for i := rf.log.GetLast().Index; i >= rf.log.GetBegin().Index; i-- {
			if rf.log.Get(i).Term == reply.XTerm {
				reply.XIndex = rf.log.Get(i).Index
			}
		}
		reply.XLen = 0
	} else {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = args.PrevLogIndex - rf.log.GetLast().Index
	}
	return false
}

func (rf *Raft) commitLogBeforIndex(leaderCommit int, leaderTerm int) {
	if leaderCommit <= rf.commitIndex {
		rf.logPrintf("Waring: rf.commitIndex: %d ~ leaderCommit: %d commited and logSize: %d", rf.commitIndex, leaderCommit, rf.log.Size)
		return
	}
	isHasLeaderTermLog := false
	for i := rf.log.EndIndex; i >= rf.log.BeginIndex; i-- {
		if rf.log.Get(i).Term == leaderTerm {
			isHasLeaderTermLog = true
			break
		}
	}
	// 没有与leader同步过日志，不允许提交
	if !isHasLeaderTermLog {
		rf.logPrintf("The log has not been synchronized with the leader, so submission is not allowed.")
		return
	}
	rf.logPrintf("index begin: %d ~ end: %d commited and logSize: %d", rf.commitIndex, leaderCommit, rf.log.Size)
	for i := rf.commitIndex; i <= leaderCommit && i <= rf.log.GetLast().Index; i++ {
		if i == 0 {
			continue
		}
		rf.commitIndex = i
		rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: rf.log.Get(i).Command, CommandIndex: rf.log.Get(i).Index}
		rf.lastApplied = i
	}
	rf.persist()
}

func (rf *Raft) changeState(to nodeState, votedFor int, term int) {
	rf.logPrintf("changeState: from %d to %d, votedFor: %d, term: %d", rf.state, to, votedFor, term)

	rf.resetElection()
	rf.votedFor = votedFor
	rf.heartDict = make(map[int]bool)
	if term < rf.currentTerm {
		msg := fmt.Sprintf("ERROR: want to reduce term.(newTerm:%d, oldTerm:%d)", term, rf.currentTerm)
		rf.logPrintf(msg)
		panic(msg)
	} else {
		rf.currentTerm = term
	}
	switch to {
	case follower:
		rf.state = follower
	case leader:
		rf.logPrintf("become a leader")
		rf.state = leader
		lastLogIndex := rf.log.GetLast().Index
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.persist()
		go rf.leaderHeart()
	case candidate:
		rf.logPrintf("become a candidate")
		rf.state = candidate
		go rf.election()
	default:
		panic("Wrong state.")
	}
	rf.persist()
}

func (rf *Raft) leaderHeart() {
	// 获取当前时间戳（纳秒精度）
	timestamp := uint64(time.Now().UnixNano())
	ticker := time.NewTicker(time.Duration(heartTimeOut) * time.Millisecond)
	defer ticker.Stop()
	// 组合时间戳和随机数
	goID := timestamp ^ 0xFFFF // 使用低16位随机数
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		rf.logPrintf("send leaderHeart.(%s)", goID)
		for i := range rf.peers {
			if i == rf.me || rf.heartDict[i] {
				continue
			}
			i := i
			rf.heartDict[i] = true
			go func() {
				rf.mu.Lock()
				heartPacket := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogTerm:  rf.log.Get(rf.nextIndex[i] - 1).Term,
					PrevLogIndex: rf.log.Get(rf.nextIndex[i] - 1).Index,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				for !rf.sendAppendEntries(i, &heartPacket, &reply) {
					rf.mu.Lock()
					if rf.killed() || rf.state != leader {
						rf.heartDict[i] = false
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.heartDict[i] = false
				if !reply.Success && reply.Term > rf.currentTerm {
					rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
					// 进入下一个任期，刷新投票
					rf.changeState(follower, -1, reply.Term)
				} else {
					rf.updateNextIndex(i, reply, heartPacket)
				}
			}()
		}
		rf.mu.Unlock()
		<-ticker.C
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

type persistData struct {
	CurrentTerm int
	VotedFor    int
	Log         logList
	CommitIndex int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 需要保证运行时持有锁
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	data := persistData{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		CommitIndex: rf.commitIndex,
		Log:         rf.log,
	}
	rf.logPrintf("the log will be persisted (EndIndex: %d, BeginIndex: %d, Size: %d)", rf.log.EndIndex, rf.log.BeginIndex, rf.log.Size)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(data); err != nil {
		rf.mu.Lock()
		rf.logPrintf("persist failed: %v", err)
		rf.mu.Unlock()
		panic(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	pdata := persistData{}
	if err := d.Decode(&pdata); err != nil {
		rf.logPrintf(err.Error())
		panic(err)
	} else {
		rf.currentTerm = pdata.CurrentTerm
		rf.votedFor = pdata.VotedFor
		rf.log = pdata.Log
		// rf.commitIndex = pdata.CommitIndex
		rf.logPrintf("readPersist and recover")
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// 任期是最重要的，任期大于自己一定会投票，任期小于自己一定不投票。
// 什么时候会拒绝投票：
// 1. 首先任期小于或等于自己 2. 在任期不够大的情况下，已投票或投票对象是其他候选者
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf("income a requestVote. argTerm:%d,Candidate:%d", args.Term, args.CandidateId)
	hasVotedForOther := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	rfLastLog := rf.log.GetLast()
	upToDateLog := args.LastLogTerm > rfLastLog.Term ||
		(args.LastLogTerm == rfLastLog.Term && args.LastLogIndex >= rfLastLog.Index)
	if (args.Term > rf.currentTerm && upToDateLog) || (args.Term == rf.currentTerm && !hasVotedForOther && upToDateLog) {
		rf.logPrintf("Vote to :%d", args.CandidateId)
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.changeState(follower, args.CandidateId, args.Term)
		return
	} else {
		rf.logPrintf("NOT Vote to :%d", args.CandidateId)
		// 即便不投票，也需要更新任期。以促使足够新的节点能够尽快当选
		rf.currentTerm = max(rf.currentTerm, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) election() {
	getVoteCount := 1
	rf.mu.Lock()
	rf.logPrintf("begin a new election.")
	requestVote := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.GetLast().Index,
		LastLogTerm:  rf.log.GetLast().Term,
	}
	rf.mu.Unlock()
	replyChan := make(chan *RequestVoteReply, len(rf.peers))
	wg := sync.WaitGroup{}
	defer func() {
		wg.Wait()
		close(replyChan)
	}()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, requestVote, reply); ok {
				replyChan <- reply
			}
		}()
	}
	for !rf.killed() {
		select {
		case reply := <-replyChan:
			rf.mu.Lock()
			// 避免之前的信息延迟后到达，错误的当选。只有任期匹配同时节点确实为候选人才当选
			if reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == candidate {
				getVoteCount++
				if getVoteCount > len(rf.peers)/2 {
					rf.logPrintf("Get majority votes")
					// 不允许刷新投票
					rf.changeState(leader, rf.votedFor, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
					// 进入下一个任期，刷新投票
					rf.changeState(follower, -1, reply.Term)
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()
		case <-time.After(time.Duration(electionTimeOut) * time.Millisecond):
			return
		}
	}
}

func HashUsingGob(v interface{}) (uint32, error) {
	var buf bytes.Buffer
	// 创建 gob encoder
	enc := gob.NewEncoder(&buf)
	// 编码值
	if err := enc.Encode(v); err != nil {
		return 0, err // 可能因类型不支持而失败
	}
	// 使用 FNV-1a 哈希（快速、确定性）
	h := fnv.New32a()
	h.Write(buf.Bytes())
	return h.Sum32(), nil
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader || rf.killed() {
		return -1, -1, false
	}
	term = rf.currentTerm
	index = rf.log.GetLast().Index + 1
	if debug {
		hash, err := HashUsingGob(command)
		if err != nil {
			rf.logPrintf("cannot HASH.")
			hash = 0
		}
		rf.logPrintf("get a new entry: index: %d, term: %d, isLeader: %v, hash: %v", index, term, rf.state == leader, hash)
	}
	newEntry := []Entry{{Term: term, Index: index, Command: command}}
	rf.log.Append(newEntry)
	rf.persist()
	go rf.appendLog(index)
	// Your code here (3B).

	return index, term, true
}

func (rf *Raft) appendLog(logIndex int) {
	rf.mu.Lock()
	successReply := make(chan struct{}, len(rf.peers))
	wg := sync.WaitGroup{}
	go rf.commitLog(logIndex, successReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// rf.nextIndex[i] = logIndex
		i := i
		wg.Add(1)
		go rf.sendLog(i, successReply, &wg)
	}
	defer func() {
		rf.mu.Unlock()
		wg.Wait()
		close(successReply)
	}()
}

func (rf *Raft) commitLog(logIndex int, successReply <-chan struct{}) {
	majority := len(rf.peers) / 2
	successCount := 1
	ticker := time.NewTicker(electionTimeOut * time.Millisecond)
	for !rf.killed() {
		select {
		case <-successReply:
			successCount++
			if successCount > majority {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state == leader && !rf.killed() {
					rf.logPrintf("logIndex: %d get majority and commitIndex: %d", logIndex, rf.commitIndex)
					rf.commitLogBeforIndex(logIndex, rf.currentTerm)
				}
				return
			}
		case <-ticker.C:
			rf.mu.Lock()
			if rf.killed() || rf.state != leader {
				rf.logPrintf("not a leader, quit commitLog func")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(server int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{}
	if rf.state == leader {
		rf.logPrintf("gen a new AppendEntriesArgs. rf.nextIndex[%d]: %d", server, rf.nextIndex[server])
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log.Get(rf.nextIndex[server] - 1).Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		args.Entries = append(args.Entries, rf.log.GetSlice(rf.nextIndex[server], -1)...)
	}
	return args
}

func (rf *Raft) updateNextIndex(server int, appendEntiresReply AppendEntriesReply, appendEntiresArgs AppendEntriesArgs) {
	// 过期的响应
	if appendEntiresArgs.Term < rf.currentTerm {
		return
	}
	defer func() { rf.logPrintf("update rf.nextIndex[%d]: %d", server, rf.nextIndex[server]) }()
	rf.logPrintf("server:{id: %d, term: %d, success: %v, XTerm: %d, XIndex: %d, XLen: %d}",
		server, appendEntiresReply.Term, appendEntiresReply.Success, appendEntiresReply.XTerm, appendEntiresReply.XIndex, appendEntiresReply.XLen)
	if appendEntiresReply.Success {
		if len(appendEntiresArgs.Entries) != 0 {
			rf.nextIndex[server] = appendEntiresArgs.Entries[len(appendEntiresArgs.Entries)-1].Index + 1
		}
		return
	}
	if appendEntiresReply.Term > rf.currentTerm {
		// 进入下一个任期，刷新投票
		rf.changeState(follower, -1, appendEntiresReply.Term)
		return
	} else {
		if appendEntiresReply.XTerm != -1 {
			isHasXTerm := false
			for i := rf.log.GetLast().Index; i >= rf.log.GetBegin().Index; i-- {
				if rf.log.Get(i).Term == appendEntiresReply.XTerm {
					rf.logPrintf("(updateNextIndex) rf has term, rf.nextIndex[%d] = %d", server, i+1)
					rf.nextIndex[server] = i + 1
					isHasXTerm = true
					break
				}
			}
			if !isHasXTerm {
				rf.logPrintf("(updateNextIndex) rf does't has term, rf.nextIndex[%d] = %d", server, appendEntiresReply.XIndex)
				rf.nextIndex[server] = appendEntiresReply.XIndex
			}
		} else {
			temp := appendEntiresArgs.PrevLogIndex - appendEntiresReply.XLen
			rf.nextIndex[server] = temp + 1
			rf.logPrintf("(updateNextIndex) rf has hole, rf.log.GetLast().Index = %d, rf.nextIndex[%d] = %d",
				rf.log.GetLast().Index, server, temp+1)
		}
	}
}

func (rf *Raft) sendLog(server int, successReply chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	rf.mu.Lock()
	isLeader := rf.state == leader
	rf.mu.Unlock()
	for !rf.killed() && isLeader {
		appendEntriesArgs := rf.genAppendEntriesArgs(server)
		appendEntiresReply := AppendEntriesReply{}
		if !rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntiresReply) {
			rf.mu.Lock()
			isLeader = rf.state == leader
			rf.mu.Unlock()
			continue
		}
		if appendEntiresReply.Success {
			successReply <- struct{}{}
			rf.mu.Lock()
			rf.updateNextIndex(server, appendEntiresReply, appendEntriesArgs)
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Lock()
			isLeader = rf.state == leader
			if appendEntiresReply.Term > rf.currentTerm {
				// 进入下一个任期，刷新投票
				rf.changeState(follower, -1, appendEntiresReply.Term)
			} else if isLeader {
				rf.updateNextIndex(server, appendEntiresReply, appendEntriesArgs)
			}
			rf.mu.Unlock()
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElection() {
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}
	rf.logPrintf("Reset time.")
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(time.Duration(electionTimeOut+rand.Intn(100)) * time.Millisecond)
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != leader {
				rf.changeState(candidate, rf.me, rf.currentTerm+1)
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		mu:            sync.Mutex{},
		currentTerm:   0,
		votedFor:      -1,
		log:           logList{},
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		state:         follower,
		electionTimer: time.NewTimer(time.Duration(rand.Intn(100)) * time.Millisecond),
		applyCh:       applyCh,
		heartDict:     make(map[int]bool),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	if persister.RaftStateSize() != 0 {
		rf.readPersist(persister.ReadRaftState())
		for i := range peers {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.log.EndIndex + 1
		}
	} else {
		// Your initialization code here (3A, 3B, 3C).
		rf.log.Append([]Entry{{Term: 1, Index: 0, Command: nil}})
		for i := range peers {
			rf.matchIndex[i] = 1
			rf.nextIndex[i] = 1
		}
	}
	// initialize from state persisted before a crash
	rf.logPrintf("Init.")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
