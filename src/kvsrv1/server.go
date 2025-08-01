package kvsrv

import (
	"errors"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

type baseError string

func (e baseError) Error() string {
	return string(e)
}

const (
	ErrNoKey   = baseError(rpc.ErrNoKey)
	ErrVersion = baseError(rpc.ErrVersion)
	ErrMaybe   = baseError(rpc.ErrMaybe)
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	data  sync.Map
	keyMu sync.Map
	// Your definitions here.
}

type valueWithVersion struct {
	value   string
	version uint64
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:    sync.Mutex{},
		data:  sync.Map{},
		keyMu: sync.Map{},
	}
	// Your code here.
	return kv
}

// 本函数需要保证调用前已获取 `KVServer.mu.Lock`
func (kv *KVServer) get(key string) (valueVersion valueWithVersion, err error) {
	if value, ok := kv.data.Load(key); ok {
		valueVersion, _ = value.(valueWithVersion)
		err = nil
		return
	} else {
		valueVersion.version = 0
		err = ErrNoKey
		return
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	mu, ok := kv.keyMu.Load(args.Key)
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	rwmu := mu.(*sync.RWMutex)
	rwmu.RLock()
	defer rwmu.RUnlock()
	if valueVersion, err := kv.get(args.Key); err != nil {
		reply.Err = rpc.Err(err.Error())
	} else {
		reply.Value = valueVersion.value
		reply.Version = rpc.Tversion(valueVersion.version)
		reply.Err = rpc.OK
		return
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.

// 我之前尝试使用另一个协程+阻塞channel实现异步写入kv.data，但测试有概率失败。
// 我突然意识到，进入协程后，从channel取出数据之后，实际写入之前Put就返回了
// 这时，其他client立刻进行 Get 将会出现数据竞争问题：Put返回后应该已经写入，但协程实现会受调度影响。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	mu, _ := kv.keyMu.LoadOrStore(args.Key, &sync.RWMutex{})
	rwmu := mu.(*sync.RWMutex)
	rwmu.Lock()
	defer rwmu.Unlock()
	valueVersion, err := kv.get(args.Key)
	if err != nil && (!errors.Is(err, ErrNoKey) || uint64(args.Version) != 0) {
		reply.Err = rpc.Err(err.Error())
		return
	}
	if valueVersion.version != uint64(args.Version) {
		reply.Err = rpc.Err(ErrVersion)
		return
	}
	value := valueWithVersion{value: args.Value, version: uint64(args.Version) + 1}
	kv.data.Store(args.Key, value)
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
