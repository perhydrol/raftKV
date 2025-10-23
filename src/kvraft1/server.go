package kvraft

import (
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type reqIdWithResult struct {
	reqId  int64
	result any
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	db           kvData
	lastRequests map[int64]lastRequestInfo
}

type kvData struct {
	KV      map[string]string
	Version map[string]rpc.Tversion
}

type lastRequestInfo struct {
	RequestId int64
	Reply     any // This will be *rpc.GetReply or *rpc.PutReply
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		if lastReq, ok := kv.lastRequests[args.ClientId]; ok && lastReq.RequestId >= args.RequestId {
			return lastReq.Reply
		}
		reply := rpc.GetReply{}
		value, ok := kv.db.KV[args.Key]
		if ok {
			reply.Err = rpc.OK
			reply.Value = value
			reply.Version = kv.db.Version[args.Key]
		} else {
			reply.Err = rpc.ErrNoKey
			reply.Value = ""
			reply.Version = 0
		}

		kv.lastRequests[args.ClientId] = lastRequestInfo{
			RequestId: args.RequestId,
			Reply:     reply,
		}
		return reply
	case rpc.PutArgs:
		if lastReq, ok := kv.lastRequests[args.ClientId]; ok && lastReq.RequestId >= args.RequestId {
			return lastReq.Reply
		}

		reply := rpc.PutReply{}
		currentVersion, exists := kv.db.Version[args.Key]
		if !exists {
			if args.Version == 0 {
				kv.db.KV[args.Key] = args.Value
				kv.db.Version[args.Key] = 1
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else {
			if args.Version == currentVersion {
				kv.db.KV[args.Key] = args.Value
				kv.db.Version[args.Key] = currentVersion + 1
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		}

		// Cache the new reply.
		kv.lastRequests[args.ClientId] = lastRequestInfo{
			RequestId: args.RequestId,
			Reply:     reply,
		}
		return reply
	default:
		panic("DoOp received an unknown request type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, opReply := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = opReply.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, opReply := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = opReply.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.db = kvData{
		KV:      make(map[string]string),
		Version: make(map[string]rpc.Tversion),
	}
	kv.lastRequests = make(map[int64]lastRequestInfo)
	return []tester.IService{kv, kv.rsm.Raft()}
}
