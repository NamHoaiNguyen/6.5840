package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type dataStore struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]dataStore
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]dataStore),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value.Value
		reply.Version = rpc.Tversion(value.Version)
		reply.Err = rpc.OK

		return
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, ok := kv.data[args.Key]
	if !ok {
		if args.Version == 0 {
			// If key doesn't exist and version = 0.
			// Save it
			new_version := args.Version + 1
			kv.data[args.Key] = dataStore{Value: args.Value, Version: new_version}

			reply.Err = rpc.OK
		} else {
			// key doesn't exist but version != 0. Return.
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	// If found value and version match
	if args.Version == data.Version {
		// Incr current version by 1
		data.Version++
		kv.data[args.Key] = dataStore{Value: args.Value, Version: data.Version}

		reply.Err = rpc.OK
		return
	}

	// If found value and versions don't match
	if args.Version != data.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Return error in other cases
	reply.Err = rpc.ErrNoKey
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
