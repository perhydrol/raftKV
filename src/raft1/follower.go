package raft

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type resp struct {
	followerId int
	payload    any
}

type changeState struct {
	from            StateType
	to              StateType
	term            int
	leaderLastIndex int
}

type newLog struct{}

type follower struct {
	mu         sync.Mutex
	me         int
	term       int
	nextIndex  int
	matchIndex int
	followerId int

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

	// 确认自身raft核心目前的身份
	status StateType

	getCoreStatus func() coreStatus

	// 存储等待发送的非日志请求（心跳、拉票）
	pendingLog []any

	// 传入拉票请求、心跳、新日志信号
	input <-chan any

	// 将 grpc reply 返回给 core
	output chan<- resp

	// 一个内部chan，用来唤醒send
	wakeup chan struct{}

	logger        *zap.Logger
	rootCtx       context.Context
	rootCtxCancel context.CancelFunc
	subCtx        context.Context
	subCtxCancel  context.CancelFunc
}

// 返回一个chan用来接受follower的reply
func NewFollower(
	ctx context.Context,
	me, term, nextIndex, matchIndex, followerId int,
	getCoreLog func(i int) (Entry, error),
	getCoreLastIndex func() int,
	call func(svcMeth string, args any, reply any) bool,
	getCoreStatus func() coreStatus,
	input <-chan any,
	output chan<- resp,
	logger *zap.Logger,
) {
	f := follower{
		mu:               sync.Mutex{},
		me:               me,
		term:             term,
		nextIndex:        nextIndex,
		matchIndex:       matchIndex,
		followerId:       followerId,
		getCoreLog:       getCoreLog,
		getCoreLastIndex: getCoreLastIndex,
		call:             call,
		getCoreStatus:    getCoreStatus,
		wakeup:           make(chan struct{}, 1),
		input:            input,
		output:           output,
		logger:           logger,
	}
	f.rootCtx, f.rootCtxCancel = context.WithCancel(ctx)
	f.subCtx, f.subCtxCancel = context.WithCancel(f.rootCtx)
	go f.sendMsg()
	go f.send()
	go f.close(ctx)
}

// 将rpc包装为一个chan，true为发送成功，false为发送失败
func (f *follower) rpcChan(svcMeth string, args any, reply any) <-chan bool {
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

func (f *follower) send() {
	for range f.wakeup {
		for f.maybeSendAppend() {
		}
	}
}

func (f *follower) findConflict(arg SendLogArgs, reply SendLogReply) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 首先确定是否需要处理冲突，如果任期不一致，将情况交给core判断
	if reply.Term == f.term {
		// TODO 需要实现快速回退逻辑
		f.nextIndex = arg.PrevLogIndex - 1
	}
}

// 尝试排空待发送队列，返回 true 表明成功取得数据并发送，返回 false 表示队列为空
func (f *follower) maybeSendAppend() bool {
	var msg any
	f.mu.Lock()
	if f.status == isFollower {
		f.mu.Unlock()
		return false
	}
	switch {
	// 存在特殊信息需要发送
	case len(f.pendingLog) != 0:
		msg = f.pendingLog[0]
		f.pendingLog = f.pendingLog[1:]

		// 释放内存：网络之前可能发生拥堵导致大量日志堆积，现在缓存被清空说明网络正常，可以释放占用的内存
		if len(f.pendingLog) == 0 && cap(f.pendingLog) >= 100 {
			f.pendingLog = make([]any, 0, 10)
		}
	// 存在需要同步的日志
	case f.getCoreLastIndex() >= f.nextIndex:
		prevEntity, err := f.getCoreLog(f.nextIndex - 1)
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
			Term:         f.term,
			LeaderID:     f.me,
			PrevLogIndex: prevEntity.Index,
			PrevLogTerm:  prevEntity.Term,
			LeaderCommit: f.getCoreStatus().commitIndex,
			Entries:      entity,
		}
		f.sentCommit = uint64(entity.Index)
	default:
		f.logger.Info("follower的发送函数被唤醒，但没有消息可发")
		f.mu.Unlock()
		return false
	}
	f.mu.Unlock()

	wrapCall := func(svcMeth string, args any, reply any) bool {
		select {
		case ok := <-f.rpcChan(svcMeth, args, reply):
			if !ok {
				return false
			}
		case <-f.subCtx.Done():
			return false
		}
		r := resp{
			followerId: f.followerId,
			payload:    reply,
		}
		select {
		case f.output <- r:
		case <-f.subCtx.Done():
			return false
		}
		return true
	}

	switch m := msg.(type) {
	case SendLogArgs:
		reply := SendLogReply{}
		ok := wrapCall("Raft.ReceiveLog", &m, &reply)
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
		ok := wrapCall("Raft.RequestVote", &m, &reply)
		return ok
	case HeartBeatArgs:
		reply := HeartBeatReply{}
		ok := wrapCall("Raft.ReceiveLog", &m, &reply)
		if ok && !reply.Success {
			f.findConflict(m.SendLogArgs, reply.SendLogReply)
		}
		// 心跳发送失败，不再尝试
		return false
	}
	return false
}

func (f *follower) close(ctx context.Context) {
	<-ctx.Done()
	close(f.wakeup)
	close(f.output)
}

func (f *follower) sendMsg() {
	for msg := range f.input {
		switch m := msg.(type) {
		case RequestVoteArgs, HeartBeatArgs:
			f.mu.Lock()
			f.pendingLog = append(f.pendingLog, m)
			f.mu.Unlock()
			select {
			case f.wakeup <- struct{}{}:
			case <-f.rootCtx.Done():
			default:
			}
		case changeState:
			f.mu.Lock()
			f.subCtxCancel()
			f.subCtx, f.subCtxCancel = context.WithCancel(f.rootCtx)
			switch m.to {
			case isLeader:
				f.status = isLeader
				f.term = m.term
				f.nextIndex = m.leaderLastIndex + 1
				f.matchIndex = 0
			case isCandidate:
				f.status = isCandidate
				f.term = m.term
				f.pendingLog = make([]any, 0, 10)
			case isFollower:
				f.status = isFollower
				f.term = m.term
				f.pendingLog = make([]any, 0, 10)
			}
			f.mu.Unlock()
		case newLog:
			// 普通log
			select {
			case f.wakeup <- struct{}{}:
			case <-f.rootCtx.Done():
			default:
			}
		default:
			f.logger.Panic("一个未知的类型")
		}
	}
}
