package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
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
	electionTimeOut time.Duration = 3000 * time.Millisecond
	heartTimeOut                  = 100 * time.Millisecond
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

	electionTimer *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	refuse := args.Term < rf.currentTerm ||
		args.PrevLogIndex > rf.log[len(rf.log)-1].Index || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term
	if refuse {
		rf.logPrintf("refuse AppendEntries RPC")
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.logPrintf("conform AppendEntries RPC and term bigger")
		reply.Success = true
		rf.currentTerm = reply.Term
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		rf.changeState(follower)
		return
	}
	rf.logPrintf("conform AppendEntries RPC")
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(electionTimeOut)
	rf.mu.Unlock()
}

func (rf *Raft) changeState(to nodeState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(electionTimeOut)
	switch to {
	case leader:
		if rf.state == candidate {
			rf.logPrintf("Become a leader.")
			rf.state = leader
			go rf.leaderHeart()
		}
	case follower:
		rf.logPrintf("Become a follower.")
		rf.state = follower
		rf.votedFor = -1
	case candidate:
		rf.logPrintf("Become a candidate.")
		rf.state = candidate
		rf.votedFor = rf.me
		rf.currentTerm++
	}
}

func (rf *Raft) leaderHeart() {
	rf.mu.Lock()
	heartArgs := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[len(rf.log)-1].Index,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	rf.logPrintf("Begin to seed Heart.")
	for range time.Tick(heartTimeOut) {
		if atomic.LoadInt32((*int32)(&rf.state)) != int32(leader) {
			return
		}
		for peer := range rf.peers {
			go func(peer int) {
				reply := &AppendEntriesReply{}
				for {
					if atomic.LoadInt32((*int32)(&rf.state)) != int32(leader) {
						return
					} else {
						if rf.sendAppendEntries(peer, heartArgs, reply) {
							if reply.Term > heartArgs.Term {
								rf.changeState(follower)
							}
							return
						}
					}
				}
			}(peer)
		}
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
	rf.logPrintf("income a requestVote. argTerm:%d,Candidate:%d", args.Term, args.CandidateId)
	hasVotedForOther := rf.votedFor != -1 || rf.votedFor != args.CandidateId
	rfLastLog := &rf.log[len(rf.log)-1]
	upToDateLog := args.LastLogTerm > rfLastLog.Term ||
		(args.LastLogTerm == rfLastLog.Term && args.LastLogIndex >= rfLastLog.Index)
	if args.Term > rf.currentTerm || (!hasVotedForOther && upToDateLog) {
		rf.logPrintf("Vote to :%d", args.CandidateId)
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(electionTimeOut) //投票成功了，需要重置选举超时
		rf.mu.Unlock()
		rf.changeState(follower)
		return
	} else {
		rf.logPrintf("NOT Vote to :%d", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

func (rf *Raft) election(ctx context.Context) {
	rf.changeState(candidate) // 负责处理term、votedFor、重置计时器
	rf.mu.Lock()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	biggerTermChan := make(chan int, len(rf.peers))
	getVoteChan := make(chan struct{}, len(rf.peers))
	wg := sync.WaitGroup{}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, req, reply) {
				if reply.Term > req.Term {
					rf.logPrintf("find a bigger Term. NewTerm:%d, OldTerm:%d", reply.Term, req.Term)
					biggerTermChan <- reply.Term
				}
				if reply.VoteGranted {
					rf.logPrintf("Get a new Vote.")
					getVoteChan <- struct{}{}
				}
			}
		}(peer)
	}
	go func() {
		voteCount := 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-getVoteChan:
				voteCount++
				rf.logPrintf("Get a vote. voteCount:%d, half:%d", voteCount, len(rf.peers)/2)
				if voteCount > len(rf.peers)/2 {
					rf.changeState(leader)
					return
				}
			case term := <-biggerTermChan:
				rf.mu.Lock()
				rf.currentTerm = term
				rf.mu.Unlock()
				rf.changeState(follower)
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		time.Sleep(500 * time.Millisecond)
		rf.logPrintf("electionFunc's channel close.")
		close(biggerTermChan)
		close(getVoteChan)
	}()
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

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 350)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		<-rf.electionTimer.C
		if rf.state != leader {
			rf.logPrintf("Begin election.")
			ctx, _ := context.WithTimeout(context.Background(), electionTimeOut)
			go func() {
				rf.election(ctx)
			}()
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
		log:           make([]Entry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		state:         follower,
		electionTimer: time.NewTimer(10 * time.Millisecond),
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
