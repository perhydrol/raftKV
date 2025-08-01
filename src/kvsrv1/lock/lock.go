package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id      string
	l       string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, id: kvtest.RandValue(8), l: l, version: 0}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		lockId, version, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey || lockId == "" {
			putErr := lk.ck.Put(lk.l, lk.id, lk.version)
			if putErr == rpc.OK {
				lk.version++
				return
			} else {
				lk.version = version
				time.Sleep(time.Second)
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		lockId, verson, err := lk.ck.Get(lk.l)
		if err != rpc.OK {
			continue
		}
		if lockId == lk.id && verson == lk.version {
			for {
				putErr := lk.ck.Put(lk.l, "", lk.version)
				if putErr == rpc.OK {
					lk.version++
					return
				} else {
					fmt.Printf("[Warning] lock: Release: An error occurred while releasing key:%s value:%s version:%d, try again.\n", lk.l, lk.id, lk.version)
				}
			}
		}
	}
}
