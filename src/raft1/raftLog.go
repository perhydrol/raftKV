package raft

import "fmt"

type logList struct {
	Log               []Entry
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
	EndIndex          int
	Size              int
}

func (l *logList) GetBeginIndex() int {
	return l.Log[0].Index
}

func (l *logList) InstallSnapshot(index int, term int, snapshot []byte) bool {
	if index <= l.LastIncludedIndex {
		return false
	}
	l.LastIncludedIndex = index
	l.LastIncludedTerm = term

	keepOffset := index - l.Log[0].Index // 我需要快照包含的最后一条日志成为哨兵节点
	if keepOffset < len(l.Log) && keepOffset >= 0 {
		l.Log = l.Log[keepOffset:]
	} else {
		l.Log = l.Log[:0]
		// 新的哨兵节点
		l.Log = append(l.Log, Entry{Command: nil, Index: index, Term: term})
	}

	l.EndIndex = l.Log[len(l.Log)-1].Index
	l.Size = len(l.Log)
	l.Snapshot = snapshot
	return true
}

func (l *logList) GetIndexTerm(index int) (int, error) {
	if index < l.LastIncludedIndex || index > l.EndIndex {
		err := fmt.Errorf("logIndex %d is not within snapshotted area (< LastIncludedIndex %d)",
			index, l.LastIncludedIndex)
		return -1, err
	}
	offset := index - l.Log[0].Index
	if offset < 0 || offset >= len(l.Log) {
		err := fmt.Errorf("internal error: offset %d (index %d) out of array bounds [0:%d]",
			offset, index, len(l.Log))
		return -1, err
	}
	return l.Log[offset].Term, nil
}

func (l *logList) Get(logIndex int) Entry {
	if (logIndex - l.Log[0].Index) < 0 {
		msg := fmt.Sprintf("logIndex: %d, l.BeginIndex: %d\n", logIndex, l.Log[0].Index)
		fmt.Println(msg)
		panic(msg)
	}
	return l.Log[logIndex-l.Log[0].Index]
}

func (l *logList) Append(logs []Entry) {
	l.Log = append(l.Log, logs...)
	l.EndIndex = l.Log[len(l.Log)-1].Index
	l.Size = len(l.Log)
}

func (l *logList) AppendList(target int, logs []Entry) error {
	if target < l.LastIncludedIndex {
		return fmt.Errorf("truncate index %d is within snapshotted area (< %d)", target, l.LastIncludedIndex)
	}
	offset := target - l.Log[0].Index
	if offset < 0 || offset > l.Size {
		return fmt.Errorf("truncate offset %d (index %d) out of array bounds [0:%d]", offset, target, len(l.Log))
	}
	l.Log = l.Log[:offset]
	l.Append(logs)
	return nil
}

func (l *logList) GetLast() Entry {
	return l.Log[len(l.Log)-1]
}

func (l *logList) GetBegin() Entry {
	return l.Log[0]
}

func (l *logList) GetSlice(begin, end int) []Entry {
	beginOffset := begin - l.Log[0].Index
	if end == -1 {
		return l.Log[beginOffset:]
	}
	endOffset := end - l.Log[0].Index
	return l.Log[beginOffset:endOffset]
}
