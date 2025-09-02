package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

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
	newFormat := fmt.Sprintf("[%s] Id:%d Term:%d State:%d : %s\n", time.Now(), rf.me, rf.currentTerm, rf.state, format)
	fmt.Printf(newFormat, a...)
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
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state nodeState

	electionTimer      *time.Timer
	resetElectionTimer chan struct{}
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) updateTerm(newTerm int) {
	if newTerm < rf.currentTerm {
		msg := fmt.Sprintf("ERROR: want to reduce term.(newTerm:%d, oldTerm:%d)", newTerm, rf.currentTerm)
		rf.logPrintf(msg)
		panic(msg)
	} else {
		rf.currentTerm = newTerm
	}
}

type Entry struct {
	Term  int
	Index int
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
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.GetAppendEntries", args, reply)
	return ok
}

func (rf *Raft) GetAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logPrintf("get a AppendEntries RPC. leader:%d, Term:%d, PrevLogIndex:%d, PrevLogTerm:%d.", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, args.Term)
		reply.Success = true
		reply.Term = args.Term
		go rf.changeState(follower, -1, args.Term)
		return
	}
	refuse := args.Term < rf.currentTerm ||
		args.PrevLogIndex > rf.log[len(rf.log)-1].Index || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term
	if refuse {
		rf.logPrintf("refuse AppendEntries RPC")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.logPrintf("conform AppendEntries RPC")
	reply.Success = true
	reply.Term = rf.currentTerm
	go rf.changeState(follower, -1, rf.currentTerm)
}

func (rf *Raft) changeState(to nodeState, votedFor int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf("changeState: from %d to %d, votedFor: %d, term: %d", rf.state, to, votedFor, term)

	select {
	case rf.resetElectionTimer <- struct{}{}:
	default:
	}
	rf.votedFor = votedFor
	if to != candidate {
		rf.updateTerm(term)
	}
	switch to {
	case follower:
		rf.state = follower
	case leader:
		if rf.state == leader {
			break
		}
		rf.logPrintf("become a leader")
		rf.state = leader
		go rf.leaderHeart()
	case candidate:
		if rf.state != leader {
			rf.logPrintf("become a candidate")
			rf.state = candidate
			rf.currentTerm++
			go rf.election()
		}
	default:
		panic("Wrong state.")
	}
}

func (rf *Raft) leaderHeart() {
	// 获取当前时间戳（纳秒精度）
	timestamp := uint64(time.Now().UnixNano())

	// 组合时间戳和随机数
	goID := timestamp ^ 0xFFFF // 使用低16位随机数
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == leader {
			rf.logPrintf("send leaderHeart.(%s)", goID)
			heartPacket := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.log[len(rf.log)-1].Index,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				i := i
				go func() {
					reply := AppendEntriesReply{}
					for !rf.sendAppendEntries(i, &heartPacket, &reply) {
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !reply.Success && reply.Term > rf.currentTerm {
						rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
						go rf.changeState(follower, -1, reply.Term)
					}
				}()
			}
		} else {
			rf.logPrintf("not a leader, quit leaderHeart.")
			rf.mu.Unlock()
			return
		}
		<-time.After(time.Duration(heartTimeOut) * time.Millisecond)
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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
	rfLastLog := &rf.log[len(rf.log)-1]
	upToDateLog := args.LastLogTerm > rfLastLog.Term ||
		(args.LastLogTerm == rfLastLog.Term && args.LastLogIndex >= rfLastLog.Index)
	if (args.Term > rf.currentTerm && upToDateLog) || (args.Term == rf.currentTerm && !hasVotedForOther && upToDateLog) {
		rf.logPrintf("Vote to :%d", args.CandidateId)
		reply.VoteGranted = true
		reply.Term = args.Term
		go rf.changeState(follower, args.CandidateId, args.Term)
		return
	} else {
		rf.logPrintf("NOT Vote to :%d", args.CandidateId)
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
	rf.logPrintf("begin a new election.")
	getVoteCount := 1
	rf.mu.Lock()
	requestVote := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
	for {
		select {
		case reply := <-replyChan:
			rf.mu.Lock()
			// 避免之前的信息延迟后到达，错误的当选。只有任期匹配同时节点确实为候选人才当选
			if reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == candidate {
				getVoteCount++
				if getVoteCount > len(rf.peers)/2 {
					rf.logPrintf("Get majority votes")
					go rf.changeState(leader, -1, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.logPrintf("Find a bigger Term: oldTerm:%d newTerm:%d. changeState to follower.", rf.currentTerm, reply.Term)
					go rf.changeState(follower, -1, reply.Term)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
			go rf.changeState(candidate, rf.me, rf.currentTerm+1)
			select {
			case rf.resetElectionTimer <- struct{}{}:
			default:
			}
		case <-rf.resetElectionTimer:
			rf.resetElection()
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
		mu:                 sync.Mutex{},
		currentTerm:        0,
		votedFor:           -1,
		log:                make([]Entry, 0),
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make([]int, len(peers)),
		matchIndex:         make([]int, len(peers)),
		state:              follower,
		electionTimer:      time.NewTimer(time.Duration(rand.Intn(100)) * time.Millisecond),
		resetElectionTimer: make(chan struct{}, 1),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = append(rf.log, Entry{Term: 0, Index: 0})
	for i := range peers {
		rf.matchIndex[i] = 1
		rf.nextIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.logPrintf("Init.")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
