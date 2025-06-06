package lock

import (
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
	lock string

	// Lock uuid
	lockId string

	// Lock version
	lockVersion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:          ck,
		lock:        l,
		lockVersion: 0,
	}
	// You may add code here
	lockId := kvtest.RandValue(8)
	lk.lockId = lockId

	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lock)
		// Get err = rpc.OK
		if err == rpc.OK {
			if val != "" && val != lk.lockId {
				// Someone else is holding lock
				continue
			}
			lk.lockVersion = ver
		}

		if lk.ck.Put(lk.lock, lk.lockId /*version*/, lk.lockVersion) == rpc.OK {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	val, ver, err := lk.ck.Get(lk.lock)
	if err == rpc.OK && val == lk.lockId {
		lk.ck.Put(lk.lock, "", ver)
	}
}
