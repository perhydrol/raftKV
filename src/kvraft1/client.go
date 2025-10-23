package kvraft

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const timeout = 60 // second

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	requestId int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = rand.Int63()
	ck.requestId = 0
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	serverIdx := ck.leaderId
	for {
		var reply rpc.GetReply
		serverAddress := ck.servers[serverIdx]
		for !ck.clnt.Call(serverAddress, "KVServer.Get", &args, &reply) {
			time.Sleep(50 * time.Millisecond)
		}
		if reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrTimeout {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = serverIdx
		return reply.Value, reply.Version, reply.Err
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.PutArgs{
		Key:       key,
		Value:     value,
		Version:   version,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}
	serverIdx := ck.leaderId
	for {
		var reply rpc.PutReply
		serverAddress := ck.servers[serverIdx]
		for !ck.clnt.Call(serverAddress, "KVServer.Put", &args, &reply) {
			time.Sleep(50 * time.Millisecond)
		}
		if reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrTimeout {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = serverIdx
		return reply.Err
	}
}
