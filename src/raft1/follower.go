package raft

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type resp struct {
	followerId int
	payload    any
}

type follower struct {
	mu         sync.Mutex
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
	getIndex func(i int) (Entry, error)

	// 存储发送数据rpc接口，注意参数需要是指针类型
	call func(svcMeth string, args any, reply any) bool

	// 确认自身raft核心目前的身份
	status StateType

	getCoreStatus func() coreStatus

	// 存储等待发送的请求
	pendingLog []any

	// 每次尝试发送log之前将首先发送心跳并清空
	// 每次更新时直接替换
	heartBeat *Entry

	input  <-chan any
	output chan<- any

	// 一个内部chan，用来唤醒send
	weakup chan struct{}

	logger        *zap.Logger
	rootCtx       context.Context
	rootCtxCancel context.CancelFunc
	subCtx        context.Context
	subCtxCancel  context.CancelFunc
}

// 将rpc包装为一个chan
func (f *follower) rpcChan(svcMeth string, args any, reply any) <-chan struct{} {
	retChan := make(chan struct{})
	for !f.call(svcMeth, args, reply) {
	}
	return retChan
}

func (f *follower) send() {
	for range f.weakup {
		var msg any
		f.mu.Lock()
		switch {
		case f.heartBeat != nil:
			msg = *f.heartBeat
			f.heartBeat = nil
		case len(f.pendingLog) != 0:
			msg = f.pendingLog[0]
			f.pendingLog = f.pendingLog[1:]

			// 释放内存：网络之前可能发生拥堵导致大量日志堆积，现在缓存被清空说明网络正常，可以释放占用的内存
			if len(f.pendingLog) == 0 && cap(f.pendingLog) >= 100 {
				f.pendingLog = make([]any, 0, 10)
			}
		default:
			f.logger.Warn("follower的发送函数被唤醒，但没有消息可发")
		}
		f.mu.Unlock()

		switch m := msg.(type) {
		case SendLogArgs:
			reply := SendLogReply{}
			select {
			case <-f.rpcChan("Raft.ReceiveLog", &m, &reply):
			case <-f.subCtx.Done():
			}
			r := resp{
				followerId: f.followerId,
				payload:    reply,
			}
			select {
			case f.output <- r:
			case <-f.subCtx.Done():
			}
		case RequestVoteArgs:
			reply := RequestVoteReply{}
			select {
			case <-f.rpcChan("Raft.RequestVote", &m, &reply):
			case <-f.subCtx.Done():
			}
			r := resp{
				followerId: f.followerId,
				payload:    reply,
			}
			select {
			case f.output <- r:
			case <-f.subCtx.Done():
			}
		}
	}
}

func (f *follower) HeartBeat(ent Entry) {
	f.mu.Lock()
	f.heartBeat = &ent
	f.mu.Unlock()
	select {
	case f.weakup <- struct{}{}:
	case <-f.rootCtx.Done():
	default:
	}
}

func (f *follower) close() {
	f.rootCtxCancel()
	close(f.weakup)
	close(f.output)
}

func (f *follower) sendMsg() {
	for msg := range f.input {
		switch m := msg.(type) {
		case Entry:
			f.mu.Lock()
			f.pendingLog = append(f.pendingLog, m)
			f.mu.Unlock()
			select {
			case f.weakup <- struct{}{}:
			case <-f.rootCtx.Done():
			default:
			}
		case StateType:
			f.mu.Lock()
			f.subCtxCancel()
			f.subCtx, f.subCtxCancel = context.WithCancel(f.rootCtx)
			switch m {
			case isLeader:
				f.status = isLeader
			case isCandidate:
				f.status = isCandidate
				f.heartBeat = nil
				f.pendingLog = make([]any, 0, 10)
			case isFollower:
				f.status = isFollower
				f.heartBeat = nil
				f.pendingLog = make([]any, 0, 10)
			}
			f.mu.Unlock()
		default:
			f.logger.Panic("一个未知的类型")
		}
	}
}
