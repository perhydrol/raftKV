package raft

import (
	"context"
	"fmt"
	"sync"

	"6.5840/raftapi"
	"go.uber.org/zap"
)

type Entry struct {
	Command *interface{}
	Term    int
	Index   int
}

type raftLog struct {
	mu        sync.RWMutex
	logData   []Entry
	offset    int // 未安装快照时为1,安装完成后为快照最后一个index+1.
	committed int

	// applying 是应用程序被指示应用于其状态机的最高日志位置。其中一些条目可能正处于应用过程中，尚未达到已应用状态。
	// 使用方式：在接收 Ready 结构体时，该字段会递增。
	// 不变性：applied <= applying && applying <= committed
	// applying int

	// applied 是应用程序成功应用于其状态机的最高日志位置。
	// 使用方式：在 Ready 结构体中已提交条目被应用（无论是同步还是异步）后推进时，该字段会递增。
	// 不变性：applied <= committed
	applied int

	// 统计每个index已经确认follower ack的次数。不需要持久化
	logRespCount map[int]int

	applyCh chan raftapi.ApplyMsg
	peerLen int

	selfCh chan struct{}
	ctx    context.Context
	logger *zap.Logger
}

func newRaftLog(ctx context.Context, peerLen int, applyCh chan raftapi.ApplyMsg, logger *zap.Logger) *raftLog {
	rl := raftLog{
		mu:           sync.RWMutex{},
		logData:      []Entry{},
		offset:       1,
		committed:    0,
		applied:      0,
		logRespCount: map[int]int{},
		applyCh:      applyCh,
		peerLen:      peerLen,
		ctx:          ctx,
		selfCh:       make(chan struct{}, 1),
		logger:       logger,
	}
	go rl.apply()

	// 如果从崩溃中恢复，则继续处理log
	rl.selfCh <- struct{}{}
	return &rl
}

func (rl *raftLog) getIndex(i int) (int, error) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	entry, err := rl.get(i)
	if err != nil {
		return -1, err
	}
	return entry.Index, nil
}

func (rl *raftLog) installSnapshot(index int, term int, snapshot []byte) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return true
}

// 获取指定index log的任期
func (rl *raftLog) getTerm(i int) (int, error) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	entry, err := rl.get(i)
	if err != nil {
		return -1, err
	}
	return entry.Term, nil
}

func (rl *raftLog) get(i int) (Entry, error) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	index := i - rl.offset
	if index < 0 || index >= len(rl.logData) {
		return Entry{}, fmt.Errorf("目标日志不存在: %d", i)
	}
	return rl.logData[index], nil
}

// 注意：是闭区间
// 仅限raftLog自行调用，使用时需要确保持有rl读锁
func (rl *raftLog) getSlice(begin, end int) ([]Entry, error) {
	if len(rl.logData) == 0 {
		return []Entry{}, nil
	}

	if begin < rl.offset {
		begin = rl.offset
	}
	if end >= rl.offset+len(rl.logData) {
		end = rl.offset + len(rl.logData) - 1
	}

	if begin > end {
		return []Entry{}, fmt.Errorf("日志范围错误（begin应该小于等于end）")
	}

	startIdx := begin - rl.offset
	endIdx := end - rl.offset

	if startIdx < 0 || endIdx >= len(rl.logData) || startIdx > endIdx {
		return []Entry{}, nil
	}

	// 返回副本以避免竞态条件
	result := make([]Entry, endIdx-startIdx+1)
	copy(result, rl.logData[startIdx:endIdx+1])
	return result, nil
}

func (rl *raftLog) newLog(command *any, term int) Entry {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	e := Entry{
		Command: command,
		Term:    term,
		Index:   rl.endIndex() + 1,
	}
	rl.logData = append(rl.logData, e)
	return e
}

func (rl *raftLog) append(ents ...Entry) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	fromIndex := ents[0].Index
	switch {
	case fromIndex == rl.logData[len(rl.logData)-1].Index+1:
		rl.logData = append(rl.logData, ents...)
	case fromIndex <= rl.offset:
		rl.logger.Info("所有日志均将被替换", zap.Int("fromIndex", fromIndex), zap.Int("endIndex", ents[len(ents)-1].Index))
		rl.logData = ents
		rl.offset = fromIndex
	default:
		rl.logger.Info("部分日志将被替换", zap.Int("fromIndex", fromIndex), zap.Int("endIndex", ents[len(ents)-1].Index))
		rl.logData = append(rl.logData[:fromIndex-rl.offset], ents...)
	}
}

// 需要保证传入的 resp 是本任期的响应，以实现间接提交。函数内部不再检查term
func (rl *raftLog) logAccept(resp SendLogReply) {
	// TODO 也许可以实现一个logRespCount缩容操作
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if resp.Index > int(rl.committed) {
		rl.logRespCount[resp.Index]++
		if rl.logRespCount[resp.Index] > rl.peerLen/2 {
			rl.committed = resp.Index
			rl.logger.Info("提交log数据", zap.Int("commitIndex", resp.Index))
			select {
			case rl.selfCh <- struct{}{}:
			default:
			}
		}
	}
}

func (rl *raftLog) setCommit(commit int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.committed < commit {
		rl.committed = commit
		rl.logger.Info("提交log数据", zap.Int("commitIndex", commit))
		select {
		case rl.selfCh <- struct{}{}:
		default:
		}
	}
}

// 在后台持续推进apply和commit
func (rl *raftLog) apply() {
	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-rl.selfCh:
			rl.mu.RLock()
			if rl.applied >= rl.committed {
				rl.mu.RUnlock()
				continue
			}
			e, err := rl.getSlice(rl.applied+1, rl.committed)
			commited := rl.committed
			rl.mu.RUnlock()

			if err != nil {
				rl.logger.Panic("获取log失败", zap.Int("beginIndex", rl.applied+1), zap.Int("endIndex", rl.committed), zap.Error(err))
			}

			for i := range e {
				select {
				case <-rl.ctx.Done():
					return
				case rl.applyCh <- raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       *e[i].Command,
					CommandIndex:  e[i].Index,
					SnapshotValid: false,
				}:
				}
			}

			if len(e) > 0 {
				rl.logger.Info("应用日志", zap.Int("beginIndex", e[0].Index), zap.Int("len", len(e)))
			}
			rl.mu.Lock()
			if commited > rl.applied {
				rl.applied = commited
			}
			rl.mu.Unlock()
		}
	}
}

func (rl *raftLog) getCommitIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return int(rl.committed)
}

func (rl *raftLog) endIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.logData[len(rl.logData)-1].Index
}

func (rl *raftLog) endTerm() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.logData[len(rl.logData)-1].Term
}
