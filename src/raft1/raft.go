package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"go.uber.org/zap"
)

const debug = true

const (
	electionTimeOut = 600
	heartTimeOut    = 10
)

type nodeState int32

const (
	leader nodeState = iota
	follower
	candidate
)

func (rf *Raft) logPrintf(msg string, a ...interface{}) {
	if !debug {
		return
	} else {
		states := [...]string{"FOLLOWER", "CANDIDATE", "LEADER"}
		stateStr := "UNKNOWN"
		if int(rf.state) < len(states) {
			stateStr = states[rf.state]
		}

		rf.logger.With(
			"Srv", rf.me,
			"Term", rf.currentTerm,
			"State", stateStr,
			"LIdx", rf.log.EndIndex,
			"LTerm", rf.log.GetLast().Term,
			"Commit", rf.commitIndex,
			"IncIdx", rf.log.LastIncludedIndex,
			"IncTerm", rf.log.LastIncludedTerm,
		).Infof(msg, a...)
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
	applyCh      chan raftapi.ApplyMsg
	getNewItemIn sync.Cond

	logger *zap.SugaredLogger
}

func (rf *Raft) initLogger() {
	config := zap.NewDevelopmentConfig()
	config.DisableStacktrace = true
	l, _ := config.Build()
	rf.logger = l.Sugar()
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Entries      []Entry
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int // term of conficting entry
	XIndex  int // index of first conficting entry or XTerm
	XLen    int // length of loss logs
	Success bool
}

type InstallSnapshotArgs struct {
	Snapshot          []byte
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Term              int
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.GetSnapshot", args, reply)
	return ok
}

func (rf *Raft) GetSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.logPrintf("TERM UPDATE: T%d → T%d (from S%d) - converting to follower", rf.currentTerm, args.Term, args.LeaderId)
		reply.Term = args.Term
		// 进入下一个任期，刷新投票
		rf.changeState(follower, -1, args.Term)
		reply.Term = rf.currentTerm
		rf.resetElection()
	}
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		reply.Success = true
		return
	}
	if rf.log.InstallSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot) {
		rf.logPrintf("InstallSnapshot from S%d [Index:%d Term:%d]", args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
		rf.resetElection()
		reply.Success = true
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		applyMsg := raftapi.ApplyMsg{SnapshotValid: true, Snapshot: args.Snapshot, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
		rf.applyCh <- applyMsg
		rf.persist()
	} else {
		rf.logPrintf("InstallSnapshot from S%d [Index:%d Term:%d] failed", args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
		reply.Success = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.GetAppendEntries", args, reply)
	return ok
}

func (rf *Raft) GetAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	defer rf.unlock()
	rf.logPrintf("RECV AppendEntries from S%d [Term:%d PrevLogIdx:%d PrevLogTerm:%d EntriesCount:%d LeaderCommit:%d]",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	if args.Term < rf.currentTerm {
		rf.logPrintf("REJECT AppendEntries: stale term (leader T%d < local T%d)", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.logPrintf("TERM UPDATE: T%d → T%d (from S%d) - converting to follower", rf.currentTerm, args.Term, args.LeaderId)
		reply.Term = args.Term
		// 进入下一个任期，刷新投票
		rf.changeState(follower, -1, args.Term)
		rf.resetElection()
	}

	// 检查 1: PrevLogIndex 是否被快照清除 (Log too old/snapshotted)
	// PrevLogIndex < 当前日志数组的起始索引 (LastIncludedIndex)
	if args.PrevLogIndex < rf.log.GetBeginIndex() {
		rf.logPrintf("REJECT AppendEntries: PrevLogIndex %d < LastIncludedIndex %d. Leader must send snapshot.",
			args.PrevLogIndex, rf.log.GetBeginIndex())
		reply.Term = rf.currentTerm
		reply.Success = false

		// 告知 Leader 应该从快照后的第一条日志开始同步
		reply.XIndex = rf.log.GetBeginIndex() + 1
		reply.XTerm = rf.log.LastIncludedTerm
		rf.resetElection()
		return
	}
	if args.PrevLogIndex > rf.log.GetLast().Index {
		rf.logPrintf("REJECT AppendEntries: log mismatch, PrevLogIndex %d is out of bounds (last index is %d)", args.PrevLogIndex, rf.log.GetLast().Index)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1                        // 表示没有冲突的 term
		reply.XLen = rf.log.GetLast().Index + 1 // Leader 可以直接跳到这个位置
		rf.resetElection()
		return
	}

	// PrevLogIndex 在范围内，再检查 Term
	rfPrevIndex, err := rf.log.GetIndexTerm(args.PrevLogIndex)
	if err != nil {
		rf.logPrintf(err.Error())
		panic(err)
	}
	if args.PrevLogTerm != rfPrevIndex {
		rf.logPrintf("REJECT AppendEntries: log mismatch at index %d (leader term: %d, local term: %d)",
			args.PrevLogIndex, args.PrevLogTerm, rfPrevIndex)

		reply.Term = rf.currentTerm
		reply.Success = false

		// 优化：快速回退
		// 找到冲突任期的第一个日志条目的索引
		reply.XTerm = rfPrevIndex
		firstIndexOfTerm := args.PrevLogIndex
		for firstIndexOfTerm > rf.log.GetBegin().Index {
			if tempIndex, err := rf.log.GetIndexTerm(firstIndexOfTerm - 1); err != nil {
				rf.logPrintf(err.Error())
				panic(err)
			} else if tempIndex != reply.XTerm {
				break
			}
			firstIndexOfTerm--
		}
		reply.XIndex = firstIndexOfTerm
		rf.resetElection()
		return
	}
	// 通过任期和日志检查
	rf.logPrintf("ACCEPT AppendEntries from S%d [Term:%d EntriesCount:%d] and leaderCommitIndex: %d", args.LeaderId, args.Term, len(args.Entries), args.LeaderCommit)
	reply.Term = rf.currentTerm
	// 任期不变，不允许刷新投票
	rf.resetElection()
	if args.Entries != nil {
		if err := rf.log.AppendList(args.PrevLogIndex+1, args.Entries); err != nil {
			rf.logPrintf(err.Error())
		}
	}
	if rf.log.GetLast().Term == args.Term {
		rf.commitLogBeforeIndex(args.LeaderCommit)
	} else {
		rf.logPrintf("WARNING Server not sync with leader, skip commit (ServerEndTerm:%d)", rf.log.GetLast().Term)
	}
	reply.Success = true
	rf.persist()
}

func (rf *Raft) commitLogBeforeIndex(leaderCommit int) {
	if leaderCommit <= rf.commitIndex {
		rf.logPrintf("COMMIT: already committed up to %d, leaderCommit %d. No new commits.", rf.commitIndex, leaderCommit)
		return
	}
	lastLogIndex := rf.log.EndIndex
	commitTo := min(leaderCommit, lastLogIndex)
	rf.logPrintf("COMMIT: committing entries from index %d to %d (leaderCommit: %d)", rf.commitIndex+1, commitTo, leaderCommit)
	applyMsgs := make([]raftapi.ApplyMsg, 0, commitTo-rf.commitIndex)
	for i := rf.commitIndex + 1; i <= commitTo; i++ {
		entry := rf.log.Get(i)
		applyMsg := raftapi.ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
		applyMsgs = append(applyMsgs, applyMsg)
	}
	for _, applyMsg := range applyMsgs {
		rf.applyCh <- applyMsg
	}
	rf.commitIndex = commitTo
	rf.lastApplied = commitTo
	rf.persist()
}

func (rf *Raft) changeState(to nodeState, votedFor int, term int) {
	rf.logPrintf("changeState: from %d to %d, votedFor: %d, term: %d", rf.state, to, votedFor, term)

	rf.votedFor = votedFor
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
		rf.lock()
		if rf.state != leader {
			rf.unlock()
			return
		}
		rf.logPrintf("send leaderHeart.(%s)", goID)
		peers_i := len(rf.peers)
		me := rf.me
		rf.unlock()
		for i := range peers_i {
			if i == me {
				continue
			}
			i := i
			go func() {
				starTime := time.Now()
				rf.lock()
				// follower日志差距过大,需要快照同步
				if rf.log.Snapshot != nil && rf.nextIndex[i]-1 < rf.log.GetBeginIndex() {
					rf.logPrintf("send snapshot to server %d", i)
					rf.unlock()
					rf.SendSnapshot(i)
				} else {
					prevLogTerm, err := rf.log.GetIndexTerm(rf.nextIndex[i] - 1)
					if err != nil {
						rf.logPrintf(err.Error())
						panic(err)
					}
					heartPacket := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogTerm:  prevLogTerm,
						PrevLogIndex: rf.nextIndex[i] - 1,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					rf.unlock()
					reply := AppendEntriesReply{}
					for !rf.sendAppendEntries(i, &heartPacket, &reply) {
						rf.lock()
						if rf.killed() ||
							rf.state != leader ||
							time.Now().After(starTime.Add(heartTimeOut*2*time.Millisecond)) {
							rf.unlock()
							return
						}
						rf.unlock()
					}
					rf.lock()
					defer rf.unlock()
					if !reply.Success && reply.Term > rf.currentTerm {
						rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
						// 进入下一个任期，刷新投票
						rf.changeState(follower, -1, reply.Term)
						rf.resetElection()
					} else {
						rf.updateNextIndex(i, reply, heartPacket)
					}
				}
			}()
		}
		<-ticker.C
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.lock()
	defer rf.unlock()
	return rf.currentTerm, rf.state == leader
}

type persistData struct {
	Log         logList
	CurrentTerm int
	VotedFor    int
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
		Log:         rf.log,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(data); err != nil {
		rf.logPrintf("PERSIST FAILED: encoding error: %v", err)
	}
	raftstate := w.Bytes()
	if len(rf.log.Snapshot) == 0 {
		rf.logPrintf("PERSIST: state saved (Log EndIdx:%d, BeginIdx:%d, Size:%d)", rf.log.EndIndex, rf.log.GetBeginIndex(), rf.log.Size)
		rf.persister.Save(raftstate, nil)
	} else {
		rf.logPrintf("PERSIST: state saved (Log EndIdx:%d, BeginIdx:%d, Size:%d) and snapshot", rf.log.EndIndex, rf.log.GetBeginIndex(), rf.log.Size)
		rf.persister.Save(raftstate, rf.log.Snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.lock()
	defer rf.unlock()
	pdata := persistData{}
	if err := d.Decode(&pdata); err != nil {
		rf.logPrintf("READ PERSIST ERROR: %v", err)
		panic(err)
	} else {
		rf.currentTerm = pdata.CurrentTerm
		rf.votedFor = pdata.VotedFor
		rf.log = pdata.Log
		rf.logPrintf("READ PERSIST: state recovered (Term:%d, VotedFor:%d, Log EndIdx:%d)", rf.currentTerm, rf.votedFor, rf.log.EndIndex)
		if rf.log.Snapshot != nil {
			applyMsg := raftapi.ApplyMsg{SnapshotValid: true, Snapshot: rf.log.Snapshot, SnapshotTerm: rf.log.LastIncludedTerm, SnapshotIndex: rf.log.LastIncludedIndex}
			rf.applyCh <- applyMsg
		}
		rf.commitIndex = rf.log.LastIncludedIndex
		rf.lastApplied = rf.log.LastIncludedIndex
	}
	// Your code here (3C).
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.lock()
	defer rf.unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.lock()
	defer rf.unlock()
	if index > rf.log.EndIndex {
		rf.logPrintf("Snapshot index:%d is out of log range:%d", index, rf.log.EndIndex)
		return
	}
	rf.logPrintf("Snapshot index:%d is valid", index)
	term, err := rf.log.GetIndexTerm(index)
	if err != nil {
		rf.logPrintf("Snapshot index %d invalid: %v", index, err)
		return
	}
	rf.log.InstallSnapshot(index, term, snapshot)
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)
	rf.persist()
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
	VoteGranted bool
	Term        int
}

// example RequestVote RPC handler.
// 任期是最重要的，任期大于自己即便不投票，任期小于自己一定不投票。
// 什么时候会拒绝投票：
// 1. 首先任期小于或等于自己 2. 在任期不够大的情况下，已投票或投票对象是其他候选者
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lock()
	defer rf.unlock()
	rf.logPrintf("income a requestVote. argTerm:%d,Candidate:%d", args.Term, args.CandidateId)
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}
	hasVotedForOther := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	rfLastLog := rf.log.GetLast()
	upToDateLog := args.LastLogTerm > rfLastLog.Term ||
		(args.LastLogTerm == rfLastLog.Term && args.LastLogIndex >= rfLastLog.Index)
	if (args.Term > rf.currentTerm && upToDateLog) || (args.Term == rf.currentTerm && !hasVotedForOther && upToDateLog) {
		rf.logPrintf("Vote to :%d", args.CandidateId)
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.changeState(follower, args.CandidateId, args.Term)
		rf.resetElection()
		return
	} else {
		rf.logPrintf("NOT Vote to :%d", args.CandidateId)
		// 即便不投票，也需要更新任期。以促使足够新的节点能够尽快当选
		reply.VoteGranted = false
		if args.Term > rf.currentTerm {
			rf.changeState(follower, -1, args.Term) // 虽然不投票,但需要转为follower
		}
		reply.Term = args.Term
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
	rf.lock()
	rf.logPrintf("begin a new election.")
	beginTerm := rf.currentTerm
	requestVote := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.GetLast().Index,
		LastLogTerm:  rf.log.GetLast().Term,
	}
	rf.unlock()
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
			for !rf.sendRequestVote(i, requestVote, reply) && !rf.killed() {
			}
			replyChan <- reply
		}()
	}
	for !rf.killed() {
		select {
		case reply := <-replyChan:
			rf.lock()
			// 避免之前的信息延迟后到达，错误的当选。只有任期匹配同时节点确实为候选人才当选
			if reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == candidate {
				getVoteCount++
				if getVoteCount > len(rf.peers)/2 {
					rf.logPrintf("Get majority votes")
					// 不允许刷新投票
					rf.changeState(leader, rf.votedFor, rf.currentTerm)
					rf.resetElection()
					rf.unlock()
					return
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
					// 进入下一个任期，刷新投票
					rf.changeState(follower, -1, reply.Term)
					rf.resetElection()
					rf.unlock()
					return
				}
			}
			rf.unlock()
		case <-time.After(time.Duration(electionTimeOut) * time.Millisecond):
			rf.lock()
			if rf.state != candidate || rf.currentTerm != beginTerm {
				rf.unlock()
				return
			}
			rf.unlock()
		}
	}
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
	rf.lock()
	defer rf.unlock()
	if rf.state != leader || rf.killed() {
		return -1, -1, false
	}
	term = rf.currentTerm
	index = rf.log.GetLast().Index + 1
	if debug {
		rf.logPrintf("get a new entry: index: %d, term: %d, isLeader: %v",
			index, term, rf.state == leader)
	}
	newEntry := []Entry{{Term: term, Index: index, Command: command}}
	rf.log.Append(newEntry)
	rf.getNewItemIn.Broadcast()
	rf.persist()
	// Your code here (3B).

	return index, term, true
}

func (rf *Raft) commitLog(successReply <-chan int) {
	waitCommit := make(map[int]int)
	majority := len(rf.peers) / 2
	for !rf.killed() {
		select {
		case ItemIndex := <-successReply:
			waitCommit[ItemIndex]++
			if waitCommit[ItemIndex]+1 > majority {
				rf.lock()
				if rf.state == leader && !rf.killed() {
					ItemTerm, err := rf.log.GetIndexTerm(ItemIndex)
					if err != nil {
						rf.logPrintf(err.Error())
						panic(err)
					}
					if ItemTerm == rf.currentTerm {
						rf.commitLogBeforeIndex(ItemIndex)
						rf.logPrintf("logIndex: %d get majority and commitIndex: %d", ItemIndex, rf.commitIndex)
					}
				}
				rf.unlock()
			}
		}
	}
}

// 注意确保运行时持有锁
func (rf *Raft) genAppendEntriesArgs(server int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	if rf.state == leader {
		rf.logPrintf("gen a new AppendEntriesArgs. rf.nextIndex[%d]: %d", server, rf.nextIndex[server])
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  -1,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		prevLogTerm, err := rf.log.GetIndexTerm(rf.nextIndex[server] - 1)
		if err != nil {
			rf.logPrintf(err.Error())
			panic(err)
		}
		args.PrevLogTerm = prevLogTerm
		args.Entries = append(args.Entries, rf.log.GetSlice(rf.nextIndex[server], -1)...)
	}
	return args
}

func (rf *Raft) updateNextIndex(server int, appendEntriesReply AppendEntriesReply, appendEntiresArgs AppendEntriesArgs) {
	// 过期的响应
	if appendEntiresArgs.Term < rf.currentTerm {
		return
	}
	defer func() { rf.logPrintf("update rf.nextIndex[%d]: %d", server, rf.nextIndex[server]) }()
	rf.logPrintf("server:{id: %d, term: %d, success: %v, XTerm: %d, XIndex: %d, XLen: %d}",
		server, appendEntriesReply.Term, appendEntriesReply.Success, appendEntriesReply.XTerm, appendEntriesReply.XIndex, appendEntriesReply.XLen)
	if appendEntriesReply.Success {
		if len(appendEntiresArgs.Entries) != 0 {
			rf.nextIndex[server] = appendEntiresArgs.Entries[len(appendEntiresArgs.Entries)-1].Index + 1
		}
		return
	}
	// 失败：快速回退逻辑

	// 最小可接受的 nextIndex
	minNextIndex := rf.log.GetBegin().Index + 1
	newNextIndex := rf.nextIndex[server] // 默认不改变

	if appendEntriesReply.XTerm != -1 {
		// Case 2: Term 冲突 (Log Mismatch)
		// 1. Leader 查找自己日志中最后一个 XTerm 出现的索引 (lastXTermIndex)
		lastXTermIndex := -1
		for i := rf.log.GetLast().Index; i >= rf.log.GetBegin().Index; i-- {
			logTerm, err := rf.log.GetIndexTerm(i)
			if err != nil {
				rf.logPrintf(err.Error())
				panic(err)
			}
			if logTerm == appendEntriesReply.XTerm {
				lastXTermIndex = i
				break
			}
		}

		if lastXTermIndex != -1 {
			// 2a. Leader 包含 XTerm：nextIndex 设置为 lastXTermIndex + 1
			newNextIndex = lastXTermIndex + 1
			rf.logPrintf("(updateNextIndex) rf has term %d, rf.nextIndex[%d] = %d",
				appendEntriesReply.XTerm, server, newNextIndex)
		} else {
			// 2b. Leader 不包含 XTerm：nextIndex 设置为 XIndex
			newNextIndex = appendEntriesReply.XIndex
			rf.logPrintf("(updateNextIndex) rf does not have term %d, rf.nextIndex[%d] = %d",
				appendEntriesReply.XTerm, server, newNextIndex)
		}
	} else {
		// Case 1: PrevLogIndex 越界 (Log Out of Bounds)
		// Leader 直接跳到 XLen (跟随者日志的下一条日志索引，即跟随者日志长度)
		newNextIndex = appendEntriesReply.XLen
		rf.logPrintf("(updateNextIndex) PrevLogIndex out of bounds, rf.nextIndex[%d] = %d",
			server, newNextIndex)
	}

	if newNextIndex < minNextIndex {
		newNextIndex = minNextIndex
	}
	rf.nextIndex[server] = newNextIndex
}

func (rf *Raft) SendSnapshot(server int) {
	rf.lock()
	if rf.killed() || rf.state != leader || rf.nextIndex[server] > rf.log.LastIncludedIndex {
		rf.unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Snapshot:          rf.log.Snapshot,
	}
	rf.unlock()
	var reply InstallSnapshotReply
	for !rf.sendInstallSnapshot(server, &args, &reply) {
		rf.lock()
		if rf.killed() || rf.state != leader || rf.nextIndex[server] > rf.log.LastIncludedIndex {
			rf.unlock()
			return
		}
		rf.unlock()
	}
	if reply.Success {
		rf.lock()
		if !rf.killed() && rf.state == leader && rf.nextIndex[server] <= rf.log.LastIncludedIndex {
			rf.logPrintf("success to install snapshot to server-%d and nextIndex[%d] update to %d (snapshotLastIndex:%d, snapshotLastTerm:%d)",
				server, server, rf.log.LastIncludedIndex+1, args.LastIncludedIndex, args.LastIncludedTerm)
			rf.nextIndex[server] = rf.log.LastIncludedIndex + 1
		}
		rf.unlock()
		return
	} else {
		if reply.Term > rf.currentTerm {
			rf.lock()
			rf.changeState(follower, -1, reply.Term)
			rf.resetElection()
			rf.unlock()
			return
		}
		rf.logPrintf("WARNING failed to install snapshot to server-%d (snapshotLastIndex:%d, snapshotLastTerm:%d)",
			server, args.LastIncludedIndex, args.LastIncludedTerm)
	}
}

// Send logs to server, one goroutine per server
func (rf *Raft) sendLog(server int, successReply chan<- int) {
sendLogMainLoop:
	for !rf.killed() {
		rf.lock()
		for rf.nextIndex[server] == rf.log.EndIndex+1 {
			rf.getNewItemIn.Wait()
		}
		if rf.state != leader || rf.nextIndex[server] == rf.log.EndIndex+1 {
			rf.unlock()
			continue
		}

		// leader已经找不到同步点Index,需要使用快照直接同步
		if rf.log.Snapshot != nil && rf.nextIndex[server] <= rf.log.LastIncludedIndex {
			rf.logPrintf("send snapshot to server %d", server)
			rf.unlock()
			rf.SendSnapshot(server)
			continue sendLogMainLoop
		}
		// Generate AppendEntries request
		appendEntriesArgs := rf.genAppendEntriesArgs(server)
		if len(appendEntriesArgs.Entries) == 0 {
			rf.unlock()
			continue
		}
		lastIndex := appendEntriesArgs.Entries[len(appendEntriesArgs.Entries)-1].Index
		rf.logPrintf("send Last Index: %d", lastIndex)
		rf.unlock()
		// Send RPC (outside lock)
		// flyingIndex = appendEntriesArgs.Entries[len(appendEntriesArgs.Entries)-1].Index
		var reply AppendEntriesReply
		for !rf.sendAppendEntries(server, &appendEntriesArgs, &reply) {
			rf.lock()
			if rf.state != leader || rf.killed() {
				rf.unlock()
				continue sendLogMainLoop
			}
			rf.unlock()
		}
		// Process response (ensure received data matches sent term)
		rf.lock()
		if reply.Success && reply.Term == rf.currentTerm && rf.state == leader {
			successReply <- lastIndex
			rf.logPrintf("index %d get a successful reply", lastIndex)
			rf.updateNextIndex(server, reply, appendEntriesArgs)
		} else {
			if reply.Term > rf.currentTerm {
				rf.changeState(follower, -1, reply.Term)
				rf.resetElection()
				rf.unlock()
				continue
			}
			if rf.state == leader {
				rf.updateNextIndex(server, reply, appendEntriesArgs)
			}
		}
		rf.unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElection() {
	rf.logPrintf("Reset time.")
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(time.Duration(electionTimeOut+rand.Intn(300)) * time.Millisecond)
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		select {
		case <-rf.electionTimer.C:
			if rf.killed() {
				return
			}
			rf.lock()
			if rf.state != leader {
				rf.changeState(candidate, rf.me, rf.currentTerm+1)
			}
			rf.resetElection()
			rf.unlock()
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
	}
	rf.initLogger()
	rf.log.Snapshot = nil
	peerLen := len(peers)
	rf.getNewItemIn = *sync.NewCond(&rf.mu)
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
	rf.resetElection()
	// initialize from state persisted before a crash
	rf.logPrintf("init")
	// start ticker goroutine to start elections
	go rf.ticker()
	successfulReply := make(chan int, len(peers))
	for i := range peerLen {
		if i == me {
			continue
		}
		go rf.sendLog(i, successfulReply)
	}
	go rf.commitLog(successfulReply)
	return rf
}
