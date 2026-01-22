package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type resp struct {
	peerId  int
	payload any
}

type newLog struct{}

type peer struct {
	mu         sync.Mutex
	me         int
	nextIndex  int
	matchIndex int
	peerId     int

	// sentCommit 是当前向跟随者发送的最高提交索引。
	sentCommit uint64

	// 如果进度最近处于活跃状态，则RecentActive为真。从相应追随者接收到任何消息都表明该进度处于活跃状态。
	// 在选举超时后，RecentActive可重置为假。
	// 在领导者节点上，此值始终为真。
	RecentActive bool

	// 用来读取raft日志的接口
	getCoreLog func(i int) (Entry, error)

	// 获取raft core目前最新的日志index
	getCoreLastIndex func() int

	// 存储发送数据rpc接口，注意参数需要是指针类型
	call func(svcMeth string, args any, reply any) bool

	getCoreStatus func() coreStatus

	// 存储等待发送的非日志请求（心跳、拉票）
	pendingLog []any

	// 传入拉票请求、心跳、新日志信号
	input <-chan any

	// 将 grpc reply 返回给 core
	output chan<- resp

	// 一个内部chan，用来唤醒send
	wakeup chan struct{}

	getCurrentTerm func() int64
	getState       func() int64

	logger       *zap.Logger
	subCtx       context.Context
	subCtxCancel context.CancelFunc
	subCtxDone   atomic.Value

	ctx context.Context
}

func newPeer(
	ctx context.Context,
	me, term, nextIndex, matchIndex, peerId int,
	getCoreLog func(i int) (Entry, error),
	getCoreLastIndex func() int,
	call func(svcMeth string, args any, reply any) bool,
	getCoreStatus func() coreStatus,
	getCurrentTerm func() int64,
	getState func() int64,
	input <-chan any,
	output chan<- resp,
	logger *zap.Logger,
) {
	f := peer{
		mu:               sync.Mutex{},
		me:               me,
		nextIndex:        nextIndex,
		matchIndex:       matchIndex,
		peerId:           peerId,
		getCoreLog:       getCoreLog,
		getCoreLastIndex: getCoreLastIndex,
		call:             call,
		getCoreStatus:    getCoreStatus,
		wakeup:           make(chan struct{}, 1),
		getCurrentTerm:   getCurrentTerm,
		getState:         getState,
		input:            input,
		output:           output,
		ctx:              ctx,
	}
	f.subCtx, f.subCtxCancel = context.WithCancel(ctx)
	f.subCtxDone.Store(f.subCtx.Done())
	f.logger = logger.With(zap.Int("peerID", peerId))
	go f.sendMsg()
	go f.send()
	go f.close(ctx)
}

// 将rpc包装为一个chan，true为发送成功，false为发送失败
func (f *peer) rpcChan(svcMeth string, args any, reply any) <-chan bool {
	// 保留一个缓存以防止goroutine泄露
	retChan := make(chan bool, 1)
	go func() {
		retryCount := 0
		for !f.call(svcMeth, args, reply) {
			retryCount++
			if retryCount >= 10.0 {
				f.logger.Warn("rpc 请求发送失败")
				retChan <- false
				return
			}
			time.Sleep(time.Duration(1<<retryCount) * time.Millisecond)
		}
		retChan <- true
	}()
	return retChan
}

func (f *peer) send() {
	for range f.wakeup {
		for f.maybeSendAppend() {
		}
	}
}

func (f *peer) findConflict(arg SendLogArgs, reply SendLogReply) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 首先确定是否需要处理冲突，如果任期不一致，将情况交给core判断
	if reply.Term == int(f.getCurrentTerm()) {
		// TODO 需要实现快速回退逻辑
		f.nextIndex = arg.PrevLogIndex - 1
	}
}

// 尝试排空待发送队列，返回 true 表明成功取得数据并发送，返回 false 表示队列为空
func (f *peer) maybeSendAppend() bool {
	var msg any
	f.mu.Lock()
	if StateType(f.getState()) == isFollower {
		f.mu.Unlock()
		return false
	}
	switch {
	// 存在特殊信息需要发送
	case len(f.pendingLog) != 0:
		f.logger.Debug("发送心跳或者拉票请求")
		msg = f.pendingLog[0]
		f.pendingLog = f.pendingLog[1:]

		// 释放内存：网络之前可能发生拥堵导致大量日志堆积，现在缓存被清空说明网络正常，可以释放占用的内存
		if len(f.pendingLog) == 0 && cap(f.pendingLog) >= 100 {
			f.pendingLog = make([]any, 0, 10)
		}
	// 存在需要同步的日志
	case f.getCoreLastIndex() >= f.nextIndex:
		prevEntity, err := f.getCoreLog(f.nextIndex - 1)
		f.logger.Debug("发送日志", zap.Int("index", f.nextIndex), zap.Int("prevIndex", prevEntity.Index))
		if err != nil {
			f.logger.Error("无法获取日志", zap.Error(err))
			f.mu.Unlock()
			return false
		}
		entity, err := f.getCoreLog(f.nextIndex)
		if err != nil {
			f.logger.Error("无法获取日志", zap.Error(err))
			f.mu.Unlock()
			return false
		}
		msg = SendLogArgs{
			Term:         int(f.getCurrentTerm()),
			LeaderID:     f.me,
			PrevLogIndex: prevEntity.Index,
			PrevLogTerm:  prevEntity.Term,
			LeaderCommit: f.getCoreStatus().commitIndex,
			Entries:      entity,
		}
		f.sentCommit = uint64(entity.Index)
	default:
		f.logger.Debug("peer的发送函数被唤醒，但没有消息可发")
		f.mu.Unlock()
		return false
	}
	f.mu.Unlock()

	wrapCall := func(svcMeth string, args any, reply any, subCtxDone <-chan struct{}) bool {
		select {
		case ok := <-f.rpcChan(svcMeth, args, reply):
			if !ok {
				return false
			}
		case <-subCtxDone:
			return false
		}
		r := resp{
			peerId:  f.peerId,
			payload: reply,
		}
		select {
		case f.output <- r:
		case <-subCtxDone:
			return false
		}
		return true
	}

	switch m := msg.(type) {
	case SendLogArgs:
		reply := SendLogReply{}
		ch := f.subCtxDone.Load().(<-chan struct{})
		ok := wrapCall("Raft.ReceiveLog", &m, &reply, ch)
		if ok {
			if reply.Success {
				f.mu.Lock()
				f.nextIndex = m.Entries.Index + 1
				f.matchIndex = m.Entries.Index
				f.mu.Unlock()
			} else {
				f.findConflict(m, reply)
			}
		}
		// 再次检索发送，即便已经为空也无伤大雅
		return true
	case RequestVoteArgs:
		reply := RequestVoteReply{}
		ch := f.subCtxDone.Load().(<-chan struct{})
		ok := wrapCall("Raft.RequestVote", &m, &reply, ch)
		return ok
	case HeartBeatArgs:
		reply := HeartBeatReply{}
		ch := f.subCtxDone.Load().(<-chan struct{})
		ok := wrapCall("Raft.ReceiveHeartBeat", &m, &reply, ch)
		if ok && !reply.Success {
			f.findConflict(m.SendLogArgs, reply.SendLogReply)
		}
		return true
	}
	return false
}

func (f *peer) close(ctx context.Context) {
	<-ctx.Done()
	close(f.wakeup)
	f.subCtxCancel()
	// close(f.output)
}

func (f *peer) sendMsg() {
	states := [...]string{"FOLLOWER", "CANDIDATE", "LEADER"}
	defer f.logger.Warn("peer事件循环退出", zap.Int("peerId", f.peerId))
	for msg := range f.input {
		switch m := msg.(type) {
		case RequestVoteArgs, HeartBeatArgs:
			f.logger.Debug("peer接收到投票或心跳")
			f.mu.Lock()
			f.pendingLog = append(f.pendingLog, m)
			f.mu.Unlock()
			select {
			case f.wakeup <- struct{}{}:
			case <-f.ctx.Done():
			default:
			}
		case changeState:
			if m.term < int(f.getCurrentTerm()) {
				continue
			}
			f.mu.Lock()
			f.logger.Debug(
				"peer接收到节点状态改变",
				zap.Int("followTerm", int(f.getCurrentTerm())),
				zap.Int("msgTerm", m.term),
				zap.String("from", states[m.from]),
				zap.String("to", states[m.to]),
			)
			f.subCtxCancel()
			f.subCtx, f.subCtxCancel = context.WithCancel(f.ctx)
			f.subCtxDone.Store(f.subCtx.Done())
			switch m.to {
			case isLeader:
				f.nextIndex = f.getCoreLastIndex() + 1
				f.matchIndex = 0
			case isCandidate:
				f.pendingLog = make([]any, 0, 10)
			case isFollower:
				f.pendingLog = make([]any, 0, 10)
			default:
				f.logger.Panic("未知类型")
			}
			f.mu.Unlock()
		case newLog:
			// 普通log
			f.logger.Debug("peer接收到新的log")
			select {
			case f.wakeup <- struct{}{}:
			case <-f.ctx.Done():
			default:
			}
		case struct{}:
			// 探测信号
		default:
			f.logger.Panic("未知类型", zap.Any("value", m), zap.String("type", fmt.Sprintf("%T", m)))
		}
	}
}
