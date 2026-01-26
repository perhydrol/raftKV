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
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	_ "net/http/pprof"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/petermattis/goid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	fmt.Println("profile 启动")
	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
}

type StateType int

const (
	isFollower  StateType = iota // 0
	isCandidate                  // 1
	isLeader                     // 2
)

const (
	heartBeatTick = 500
	electionTick  = 1000
)

type changeState struct {
	from StateType
	to   StateType
	term int
}

func initLogger(me int) *zap.Logger {
	config := zap.NewDevelopmentConfig()

	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	config.DisableStacktrace = true
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := config.Build()
	if err != nil {
		panic(err)
	}
	return l.With(zap.Int("Srv", me), zap.Int64("gid", goid.Get()))
}

// 调用时确保持有锁
func (rf *Raft) logPrintf() *zap.Logger {
	if false {
		return zap.NewNop()
	}
	states := [...]string{"FOLLOWER", "CANDIDATE", "LEADER"}
	stateStr := "UNKNOWN"
	state := StateType(rf.state)
	if int(state) < len(states) {
		stateStr = states[state]
	}

	return rf.logger.With(
		zap.Int64("Term", rf.currentTerm),
		zap.String("State", stateStr),
		zap.Int("LIDx", rf.log.endIndex()),
		zap.Int("LTerm", rf.log.endTerm()),
		zap.Int("Commit", rf.log.getCommitIndex()),
		zap.Int("Loffset", rf.log.offset),
	)
}

type ticker struct {
	mu       sync.Mutex
	t        *time.Ticker
	interval int
}

func initTicker(outTime int) ticker {
	return ticker{mu: sync.Mutex{}, t: time.NewTicker(time.Duration(outTime+rand.Intn(200)) * time.Millisecond), interval: outTime}
}

func (t *ticker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.t.Stop()
	select {
	case <-t.t.C:
	default:
	}
	t.t.Reset(time.Duration(t.interval+rand.Intn(200)) * time.Millisecond)
}

func (t *ticker) newOutTime(o int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.t.Stop()
	select {
	case <-t.t.C:
	default:
	}
	t.interval = o
	t.t.Reset(time.Duration(t.interval+rand.Intn(200)) * time.Millisecond)
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
	currentTerm int64
	votedFor    int
	log         *raftLog

	lastApplied int

	// 用来统计选票，每次term更新记得归零
	getVoteCount int

	state int64

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

	// 本次心跳响应计数，用于leader感知自己是否被分区
	heartBeatSuccessCount atomic.Int32

	ticker ticker
}

func (rf *Raft) getCurrentTerm() int64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) getState() int64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state
}

// raft 内部函数，调用时必须持有写锁
func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = int64(term)
	rf.votedFor = -1 // 任期变了，选票需要重置
	rf.heartBeatSuccessCount.Store(0)
	rf.getVoteCount = 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return int(rf.currentTerm), StateType(rf.state) == isLeader
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
		currentTerm: int(rf.currentTerm),
		votedFor:    rf.votedFor,
		commitIndex: rf.log.getCommitIndex(),
		lastApplied: rf.lastApplied,
		state:       StateType(rf.state),
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
	Term         int
	CandidateID  int
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf().Debug("接收新的投票信号", zap.Int("CandidateID", args.CandidateID))

	currentTerm := int(rf.currentTerm)
	state := StateType(rf.state)

	var cs *changeState //如果投票请求导致节点状态变化，需要通知follower管理器
	defer func() {
		if cs != nil {
			rf.sendToFollowers <- *cs
		}
	}()
	// 默认回复
	reply.VoteGranted = false
	reply.Term = currentTerm

	if args.Term < currentTerm {
		rf.logPrintf().Debug("拒绝投票，因为任期更小", zap.Int("argsTerm", args.Term), zap.Int("CandidateID", args.CandidateID))
		return
	}

	// 处理更高任期 (变为 Follower)
	// 注意：即使 args.Term > currentTerm，我们也不一定投票给它（还需要检查日志）
	// 但我们需要更新自己的任期状态
	if args.Term > currentTerm {
		rf.updateTerm(args.Term)
		if state != isFollower {
			cs = &changeState{
				from: state,
				to:   isFollower,
				term: args.Term,
			}
			rf.state = int64(isFollower)
		}
		rf.persist()                       // 状态改变，必须持久化
		rf.ticker.newOutTime(electionTick) // 即便不投票，当集群存在较新的term时也需要重置记时器
	}

	reply.Term = args.Term

	// 检查日志是否足够新 (Log Matching Property)
	// 定义：最后一条日志任期更大，或者任期相同但在索引上更长
	lastLogIndex := rf.log.endIndex()
	lastLogTerm := rf.log.endTerm()

	isLogUpToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// 决定是否投票
	// 条件：(没投过票或已经投给了这个人) 并且日志足够新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && isLogUpToDate {
		rf.votedFor = args.CandidateID
		if state != isFollower {
			cs = &changeState{
				from: state,
				to:   isFollower,
				term: args.Term,
			}
			rf.state = int64(isFollower)
		}
		rf.persist() // 先持久化，再回复！

		rf.ticker.newOutTime(electionTick)

		reply.VoteGranted = true
		rf.logPrintf().Info("投票成功", zap.Int("Candidate", args.CandidateID), zap.Int("Term", args.Term))
		return
	}
	rf.logPrintf().Debug("拒绝投票", zap.Int("CandidateID", args.CandidateID))
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
	Index   int
	Success bool
	NodeID  int
}

type HeartBeatArgs struct {
	SendLogArgs
}
type HeartBeatReply struct {
	SendLogReply
}

func (rf *Raft) ReceiveHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf().Debug("接收心跳信号", zap.Int("leaderID", args.LeaderID), zap.Int("heartBeatTerm", args.Term))

	currentTerm := int(rf.currentTerm)
	state := StateType(rf.state)

	var cs *changeState
	defer func() {
		if cs != nil {
			rf.sendToFollowers <- *cs
		}
	}()
	reply.NodeID = rf.me

	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	rf.logPrintf().Debug("确认心跳Term有效", zap.Int("leaderID", args.LeaderID))
	rf.ticker.newOutTime(electionTick) // 无论如何，只要term有效就重置记时器

	if args.Term > currentTerm {
		rf.updateTerm(args.Term)
		if state != isFollower {
			cs = &changeState{
				from: state,
				to:   isFollower,
				term: args.Term,
			}
			rf.state = int64(isFollower)
		}
		rf.persist() // 状态改变，必须持久化
	}
	reply.Term = args.Term

	prevLog, err := rf.log.get(args.PrevLogIndex)
	if err != nil {
		rf.logPrintf().Warn("无法获取prevLog", zap.Int("prevLogIndex", args.PrevLogIndex), zap.Error(err))
		reply.Success = false
		return
	}
	if prevLog.Index == args.PrevLogIndex && prevLog.Term == args.PrevLogTerm {
		reply.Index = args.Entries.Index
		reply.Success = true
		rf.log.setCommit(args.LeaderCommit)
		rf.persist()
		return
	}
	reply.Success = false
}

func (rf *Raft) ReceiveLog(args *SendLogArgs, reply *SendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrintf().Debug("接收新的日志同步信号")

	currentTerm := int(rf.currentTerm)
	state := StateType(rf.state)

	var cs *changeState
	defer func() {
		if cs != nil {
			rf.sendToFollowers <- *cs
		}
	}()
	reply.NodeID = rf.me

	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	rf.ticker.newOutTime(electionTick) // 无论如何，只要term有效就重置记时器

	if args.Term > currentTerm {
		rf.updateTerm(args.Term)
		if state != isFollower {
			cs = &changeState{
				from: state,
				to:   isFollower,
				term: args.Term,
			}
			rf.state = int64(isFollower)
		}
		rf.persist() // 状态改变，必须持久化
	}

	reply.Term = args.Term

	prevLog, err := rf.log.get(args.PrevLogIndex)
	if err != nil {
		rf.logPrintf().Warn("无法获取prevLog", zap.Int("prevLogIndex", args.PrevLogIndex), zap.Error(err))
		reply.Success = false
		return
	}
	if prevLog.Index == args.PrevLogIndex && prevLog.Term == args.PrevLogTerm {
		rf.log.append(args.Entries)
		reply.Index = args.Entries.Index
		reply.Success = true
		rf.log.setCommit(args.LeaderCommit)
		rf.persist()
		rf.logPrintf().Debug("确认新的日志", zap.Int("index", args.Entries.Index))
		return
	}

	reply.Success = false
	rf.logPrintf().Debug(
		"由于prevLog不匹配，拒绝新的日志",
		zap.Int("index", args.Entries.Index),
		zap.Int("prevLogIndex", prevLog.Index),
		zap.Int("prevLogTerm", prevLog.Term),
		zap.Int("argsPrevLogIndex", args.PrevLogIndex),
		zap.Int("argsPrevLogTerm", args.PrevLogTerm),
	)
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
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.state != int64(isLeader) || rf.killed() {
		return -1, -1, false
	}
	rf.log.newLog(&command, int(rf.currentTerm))
	index := rf.log.endIndex()
	term := rf.log.endTerm()
	isLeader := rf.state == int64(isLeader)
	rf.sendToFollowers <- newLog{}
	rf.logPrintf().Info("外部传入新的日志", zap.Int("index", index))
	return index, term, isLeader
}

func (rf *Raft) testHearBeat(beginTerm int64) {
	var cs *changeState
	defer func() {
		if cs != nil {
			rf.sendToFollowers <- *cs
		}
	}()
	ticker := time.NewTicker((electionTick + 800) * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rf.mu.RLock()
		if rf.currentTerm != beginTerm {
			rf.mu.RLocker()
			return
		}
		rf.mu.RUnlock()
		if rf.heartBeatSuccessCount.Load() < int32(len(rf.peers)/2) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			newTerm := int(rf.currentTerm) + 1
			if rf.state != int64(isFollower) {
				rf.updateTerm(newTerm)
				cs = &changeState{
					from: StateType(rf.state),
					to:   isFollower,
					term: newTerm,
				}
				rf.state = int64(isFollower)
				rf.persist() // 状态改变，必须持久化
				rf.logPrintf().Warn("心跳长期无响应，退回follower")
			}
			return
		}
	}
}

func (rf *Raft) tickOutTime() {
	rf.mu.RLock()
	state := StateType(rf.state)
	currentTerm := int(rf.currentTerm)
	switch state {
	case isLeader:
		msg := HeartBeatArgs{
			SendLogArgs: SendLogArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.log.endIndex(),
				PrevLogTerm:  rf.log.endTerm(),
				LeaderCommit: rf.log.getCommitIndex(),
				Entries:      Entry{},
			},
		}
		rf.logPrintf().Info("发送心跳")
		go rf.testHearBeat(rf.currentTerm)
		rf.mu.RUnlock()
		rf.sendToFollowers <- msg
		rf.ticker.reset()
	case isCandidate, isFollower:
		rf.mu.RUnlock()
		rf.ticker.reset()
		rf.election()
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
	rf.ctxCancel()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	currentTerm := int(rf.currentTerm)
	newTerm := currentTerm + 1
	rf.currentTerm = int64(newTerm)
	rf.getVoteCount = 1
	requestVote := RequestVoteArgs{
		Term:         newTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.log.endIndex(),
		LastLogTerm:  rf.log.endTerm(),
	}
	state := StateType(rf.state)
	rf.sendToFollowers <- changeState{
		from: state,
		to:   isCandidate,
		term: newTerm,
	}
	rf.state = int64(isCandidate)
	rf.logPrintf().Info("开启选举")
	rf.mu.Unlock()
	rf.sendToFollowers <- requestVote
}

func (rf *Raft) processVoteReply(m RequestVoteReply) {
	rf.mu.Lock()
	state := StateType(rf.state)
	currentTerm := int(rf.currentTerm)
	if state == isCandidate && m.Term == currentTerm {
		if m.VoteGranted {
			rf.getVoteCount++
			if rf.getVoteCount > len(rf.peers)/2 {
				rf.logPrintf().Info("成为领导者节点")
				rf.state = int64(isLeader)
				rf.sendToFollowers <- changeState{
					from: isCandidate,
					to:   isLeader,
					term: currentTerm,
				}
				rf.ticker.newOutTime(heartBeatTick)
				rf.log.newLog(nil, currentTerm)
				rf.sendToFollowers <- newLog{}
				rf.logPrintf().Debug("领导者发送一个空日志")
			}
		} else {
			if m.Term > currentTerm {
				rf.updateTerm(m.Term)
				rf.state = int64(isFollower)
				rf.sendToFollowers <- changeState{
					from: isCandidate,
					to:   isFollower,
					term: m.Term,
				}
				rf.persist() // 状态改变，必须持久化
				rf.ticker.newOutTime(electionTick)
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) processReply(m SendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := int(rf.currentTerm)
	state := StateType(rf.state)

	if m.Term < currentTerm {
		return
	}
	if m.Term > currentTerm {
		rf.updateTerm(m.Term)
		rf.sendToFollowers <- changeState{
			from: state,
			to:   isFollower,
			term: m.Term,
		}
		rf.state = int64(isFollower)
		rf.persist() // 状态改变，必须持久化
		rf.ticker.newOutTime(electionTick)
		return
	}

	rf.log.logAccept(m)
	rf.persist()
}

func (rf *Raft) processHeartBeatReply(m HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var cs *changeState
	defer func() {
		if cs != nil {
			rf.sendToFollowers <- *cs
		}
	}()
	if m.Term != int(rf.currentTerm) {
		if m.Term > int(rf.currentTerm) && rf.state != int64(isFollower) {
			rf.updateTerm(m.Term)
			cs = &changeState{
				from: StateType(rf.state),
				to:   isFollower,
				term: m.Term,
			}
			rf.state = int64(isFollower)
			rf.persist() // 状态改变，必须持久化
		}
		return
	}
	if m.Success {
		rf.heartBeatSuccessCount.Add(1)
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.ticker.tick():
			rf.tickOutTime()
		case msg := <-rf.followerResp:
			switch m := msg.payload.(type) {
			case *RequestVoteReply:
				rf.processVoteReply(*m)
			case *SendLogReply:
				rf.processReply(*m)
			case *HeartBeatReply:
				rf.processHeartBeatReply(*m)
			default:
				rf.logger.Panic("未知类型", zap.Any("value", m), zap.String("type", fmt.Sprintf("%T", m)))
			}
		case <-rf.ctx.Done():
			rf.logger.Info("主线程退出")
			return
			// default:
			// 	rf.logger.Debug("主线程正常运行")
			// 	time.Sleep(10 * time.Millisecond)
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

		lastApplied: 0,

		state: 0, // isFollower

		applyCh: applyCh,

		logger: initLogger(me),
		ticker: initTicker(electionTick),
	}
	rf.ctx, rf.ctxCancel = context.WithCancel(context.Background())
	rf.log = newRaftLog(rf.ctx, len(peers), applyCh, rf.logger)

	sub := make([]chan any, len(peers))
	for i := range sub {
		// 对于扇出模式，使用一个小的缓冲区可以避免 broadcast goroutine 在某个 follower 短暂繁忙时被阻塞。
		sub[i] = make(chan any, 1)
	}
	sendToFollowers := make(chan any)
	followerResp := make(chan resp, len(peers))
	rf.sendToFollowers = sendToFollowers
	rf.followerResp = followerResp

	go broadcast(rf.ctx, sendToFollowers, sub, me)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.run()
	go rf.debug()

	for i := range peers {
		if i == me {
			continue
		}
		newPeer(
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
			rf.getCurrentTerm,
			rf.getState,
			sub[i],
			followerResp,
			rf.logger,
		)
	}

	rf.logPrintf().Info("raft core启动")
	return rf
}

func broadcast(ctx context.Context, put <-chan any, sub []chan any, me int) {
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
				if i == me {
					continue
				}
				sub[i] <- msg
			}
		}
	}
}

func (rf *Raft) debug() {
	//tick := time.NewTicker(10 * time.Second)
	//for {
	//	<-tick.C
	//	select {
	//	case rf.sendToFollowers <- struct{}{}:
	//	case <-time.After(1000 * time.Millisecond):
	//		rf.logger.Warn("follower阻塞")
	//	}
	//}
}
