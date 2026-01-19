package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type StateType int

var isLeader StateType = 1
var isFollower StateType = 2
var isCandidate StateType = 3

var heartBeatTick time.Duration = 300 * time.Millisecond
var electionTick time.Duration = 1000 * time.Millisecond

func initLogger(me int) *zap.Logger {
	config := zap.NewProductionConfig()
	config.DisableStacktrace = true
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	l, _ := config.Build()
	return l.With(zap.Int("Srv", me))
}

func (rf *Raft) logPrintf() *zap.Logger {
	if false {
		return zap.NewNop()
	}
	states := [...]string{"FOLLOWER", "CANDIDATE", "LEADER"}
	stateStr := "UNKNOWN"
	if int(rf.state) < len(states) {
		stateStr = states[rf.state]
	}

	return rf.logger.With(
		zap.Int64("Term", int64(rf.currentTerm)),
		zap.String("State", stateStr),
		zap.Int("LIdx", rf.log.endIndex()),
		zap.Int("LTerm", rf.log.endTerm()),
		zap.Int("Commit", rf.commitIndex),
		zap.Int("Loffset", rf.log.offset),
	)
}

type ticker struct {
	t        *time.Ticker
	interval time.Duration
}

func initTicker(outTime time.Duration) ticker {
	return ticker{t: time.NewTicker(outTime), interval: outTime}
}

func (t *ticker) reset() {
	t.t.Stop()
	select {
	case <-t.t.C:
	default:
	}
	t.t.Reset(t.interval)
}

func (t *ticker) newOutTime(o time.Duration) {
	t.interval = o
	t.reset()
}

func (t *ticker) tick() <-chan time.Time {
	return t.t.C
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         raftLog

	commitIndex int
	lastApplied int

	state StateType

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan raftapi.ApplyMsg

	logger    *zap.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc

	// 用来向所有follower传递信号
	sendToFollowers chan<- any

	// 用于接受所有follower的返回信号
	followerResp <-chan resp

	ticker ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
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

type coreStatus struct {
	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	state StateType
	me    int
}

func (rf *Raft) getRaftCoreStatus() coreStatus {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	cs := coreStatus{
		currentTerm: rf.currentTerm,
		votedFor:    rf.votedFor,
		commitIndex: rf.commitIndex,
		lastApplied: rf.lastApplied,
		state:       rf.state,
		me:          rf.me,
	}
	return cs
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
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
}

type SendLogArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      Entry
}

type SendLogReply struct {
	Term    int
	Success bool
	NodeID  int
}

type HeartBeatArgs struct {
	SendLogArgs
}
type HeartBeatReply struct {
	SendLogReply
}

func (rf *Raft) ReceiveLog(args *SendLogArgs, reply *SendLogReply) {

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

func (rf *Raft) tickOutData() {}

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
	rf.ctxCancel()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.ticker.tick():
			rf.tickOutData()
		case <-rf.ctx.Done():
			return
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
		peers:     peers,
		persister: persister,
		me:        me,
		mu:        sync.RWMutex{},

		currentTerm: 0,
		votedFor:    -1,

		commitIndex: 0,
		lastApplied: 0,

		state: isFollower,

		applyCh: applyCh,

		logger: initLogger(me),
		ticker: initTicker(electionTick),
	}
	rf.ctx, rf.ctxCancel = context.WithCancel(context.Background())
	rf.log = newRaftLog(rf.logger)

	sub := make([]chan any, len(peers))
	sendToFollowers := make(chan any)
	followerResp := make(chan resp, len(peers))
	rf.sendToFollowers = sendToFollowers
	rf.followerResp = followerResp

	go broadcast(rf.ctx, sendToFollowers, sub)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.run()

	for i := range peers {
		if i == me {
			continue
		}
		NewFollower(
			rf.ctx,
			me,
			0,
			1,
			0,
			i,
			rf.log.get,
			rf.log.endIndex,
			rf.peers[i].Call,
			rf.getRaftCoreStatus,
			sub[i],
			followerResp,
			rf.logger,
		)
	}

	return rf
}

func broadcast(ctx context.Context, put <-chan any, sub []chan any) {
	defer func() {
		for i := range sub {
			close(sub[i])
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-put:
			// sub中的接受chan是无阻塞的
			for i := range sub {
				sub[i] <- msg
			}
		}
	}
}
