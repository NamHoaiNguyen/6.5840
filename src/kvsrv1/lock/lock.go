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
			if val != "" {
				// Someone else is holding lock
				time.Sleep(10 * time.Millisecond)
			}
			lk.lockVersion = ver
		}

		if err == rpc.ErrNoKey {
			lk.lockVersion = 0
		}

		// fmt.Printf("Status of err: %s, Status of val: %s\n", err, val)

		if lk.ck.Put(lk.lock, lk.lockId /*version*/, lk.lockVersion) == rpc.OK {
			fmt.Println("Namnh check lock should put")
			// lk.lockVersion++
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// time.Sleep(2000 * time.Millisecond)

	val, ver, err := lk.ck.Get(lk.lock)
	if err == rpc.OK && val == lk.lockId {
		// fmt.Println("Namnh check lock should be released")

		lk.ck.Put(lk.lock, "", ver)
	}
}
