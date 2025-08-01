package kvsrv

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt          *tester.Clnt
	server        string
	putErrVersion sync.Map
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server, putErrVersion: sync.Map{}}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{
		Key: key,
	}
	var reply rpc.GetReply
	for {
		reply = rpc.GetReply{}
		if !ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case rpc.ErrNoKey:
			fmt.Printf("[Warning] client: Get: key '%s' has no vaule.\n", key)
			return "", 0, rpc.ErrNoKey
		case rpc.OK:
			return reply.Value, reply.Version, rpc.OK
		}
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
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.

// 笔记：首次Put，返回ErrVersion意味着确实是版本号不正确。
// 但如果该Put因为其他错误再次尝试，那么我们知道：
// 1. 之前的错误不是ErrVersion，也就是server并未明确拒绝写入
// 2. 我们不确定之前是否成功写入，而此时却返回ErrVersion，此时有两种可能，因此是Maybe：
//   - 之前的写入成功了，但因为其他原因没有返回OK，这次的版本号重复自然会写入失败。我们已经成功写入了值。
//   - 之前写入就失败了，但ErrVersion在返回时丢失了，这一次再次写入失败。我们最终也没有成功写入。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}
	for !ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) {
		ck.putErrVersion.Store(key, 1) // 不确定本次是否成功
		time.Sleep(100 * time.Millisecond)
	}
	// fmt.Printf("[Info] client: Put: key %s , reply status %s.\n", key, reply.Err)
	defer func() {
		ck.putErrVersion.Delete(key)
	}()
	switch reply.Err {
	case rpc.OK:
		return rpc.OK
	case rpc.ErrVersion:
		if _, ok := ck.putErrVersion.Load(key); !ok {
			// fmt.Printf("[Info] client: Put: key %s rpc first return ErrVersion.\n", key)
			return rpc.ErrVersion
		} else {
			return rpc.ErrMaybe
		}
	case rpc.ErrNoKey:
		return rpc.ErrNoKey
	}
	fmt.Printf("[Warning] client: Put: An unknown error occurred '%s'.\n", reply.Err)
	return reply.Err
}
