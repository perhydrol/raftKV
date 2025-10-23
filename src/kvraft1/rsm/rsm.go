package rsm

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req any
	Me  int
	Id  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type applyS struct {
	reply any
	id    int
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	pendingOps   map[int]chan<- applyS
	pendingOpsId map[int]int
	maxIndex     int64
	close        chan struct{}
}

func (rsm *RSM) reader() {
	for iData := range rsm.applyCh {
		if iData.CommandValid {
			op := iData.Command.(Op)
			ret := rsm.sm.DoOp(op.Req)
			rsm.mu.Lock()
			ch, ok := rsm.pendingOps[iData.CommandIndex]
			rsm.mu.Unlock()
			if ok {
				ch <- applyS{reply: ret, id: op.Id}
			}
		}
	}
	defer func() {
		rsm.mu.Lock()
		close(rsm.close)
		rsm.mu.Unlock()
	}()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingOps:   map[int]chan<- applyS{},
		pendingOpsId: map[int]int{},
		close:        make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// 指南中提到一个棘手问题：Submit 调用 Start() 成功 (返回 isLeader=true)，但在这条日志被提交 (commit) 之前，它失去了 Leader 身份。
// 会发生什么:新的 Leader 可能会用另一条日志覆盖掉 Submit 提交的那个 index。当 reader 协程最终收到 applyCh 上 index 位置的消息时，它可能不是 Submit 当初提交的那个 op！
// 需要利用额外生成的ID
// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{Me: rsm.me, Id: int(atomic.LoadInt64(&rsm.maxIndex))*1000 + rand.Intn(1000), Req: req}
	atomic.AddInt64(&rsm.maxIndex, 1)

	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	awakeCh := make(chan applyS)

	rsm.mu.Lock()
	rsm.pendingOps[index] = awakeCh
	rsm.pendingOpsId[index] = op.Id
	defer func() {
		rsm.mu.Lock()
		delete(rsm.pendingOps, index)
		rsm.mu.Unlock()
		close(awakeCh)
	}()
	rsm.mu.Unlock()
	select {
	case ret := <-awakeCh:
		// ID发生了变化,leader不再有效
		if ret.id != op.Id {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, ret.reply
	case <-time.After(2000 * time.Millisecond):
		return rpc.ErrWrongLeader, nil
	case <-rsm.close:
		return rpc.ErrWrongLeader, nil
	}
}
