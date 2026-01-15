package raft

import (
	"fmt"

	"go.uber.org/zap"
)

type Entry struct {
	Command *interface{}
	Term    int
	Index   int
}

type raftLog struct {
	logData   []Entry
	offset    int // 未安装快照时为1,安装完成后为快照最后一个index+1.
	committed uint64
	// applying 是应用程序被指示应用于其状态机的最高日志位置。其中一些条目可能正处于应用过程中，尚未达到已应用状态。
	// 使用方式：在接收 Ready 结构体时，该字段会递增。
	// 不变性：applied <= applying && applying <= committed
	applying uint64
	// applied 是应用程序成功应用于其状态机的最高日志位置。
	// 使用方式：在 Ready 结构体中已提交条目被应用（无论是同步还是异步）后推进时，该字段会递增。
	// 不变性：applied <= committed
	applied uint64

	logger *zap.Logger
}

func newRaftLog(logger *zap.Logger) raftLog {
	rf := raftLog{logData: []Entry{}, offset: 1, committed: 0, applying: 0, applied: 0, logger: logger}
	return rf
}

func (rl *raftLog) getIndex(i int) (int, error) {
	entry, err := rl.get(i)
	if err != nil {
		return -1, err
	}
	return entry.Index, nil
}

func (rl *raftLog) installSnapshot(index int, term int, snapshot []byte) bool {
	return true
}

func (rl *raftLog) getTerm(i int) (int, error) {
	entry, err := rl.get(i)
	if err != nil {
		return -1, err
	}
	return entry.Term, nil
}

func (rl *raftLog) get(i int) (Entry, error) {
	index := i - rl.offset
	if index < 0 || index >= len(rl.logData) {
		return Entry{}, fmt.Errorf("目标日志不存在: %d", i)
	}
	return rl.logData[index], nil
}

func (rl *raftLog) append(ents ...Entry) {
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

func (rl *raftLog) endIndex() int {
	return rl.logData[len(rl.logData)-1].Index
}

func (rl *raftLog) endTerm() int {
	return rl.logData[len(rl.logData)-1].Term
}
