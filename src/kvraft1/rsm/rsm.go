package rsm

import (
	"fmt"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

// In Lab 4A, it is ok to use a monotonic increasing number
// as an unique identifier.
// TODO(namnh) : Update in Lab 4B
var OpId int = 0

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int // id of raft node
	Id  int // id of each operation to ensure linerizability
	Req any // request sent by client
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

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	// TODO(namnh) : check type
	msgCh chan any // channel between "execute" goroutine and submit
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
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// Goroutine that handle message received from leader
	go rsm.Execute()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Goroutine that handle message received from leader
func (rsm *RSM) Execute() {
	// Read from applyCh and pass to DoOp()
	for {
		select {
		case msg, ok := <-rsm.applyCh:
			if !ok {
				// channel is closed, shutdown goroutine
				return
			}
			println("Value of closed", ok)

			if !msg.CommandValid {
				// Just ignore invalid message
				continue
			}

			res := rsm.sm.DoOp(msg.Command)

			rsm.msgCh <- res
			// case
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	// defer rsm.mu.Unlock()

	// Increase monotonic increasing ?Id
	OpId++

	// Prepare data for request
	operation := Op{
		Me:  rsm.me,
		Id:  OpId,
		Req: req,
	}

	// Call Raft service
	_, termBeforeRequest, isLeader := rsm.rf.Start(operation)
	if !isLeader {
		// If node is NOT leader, return
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	fmt.Printf("I am leader with term: %d", termBeforeRequest)

	// In case that the node is not leader when processing request,
	// this request should be routed to the new leader.
	termAfterRequest, isLeader := rsm.rf.GetState()
	if termAfterRequest > termBeforeRequest || !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// select {
	// // TODO(namnh) : recheck this number
	// case <-time.After(500 * time.Millisecond):
	// }

	// In case that leader is partitioned, message won't be committed.
	// Because a message is committed if and only if majority of node agree to commit.
	// So, we need a timeout in this case. If client doesn't receive request in an interval,
	// this request should be treated as timeout and should resent.
	// Other approach is that in this case, anny client in the same partition won't be able to talk to a new leader either.
	// So it's OK in this case for the server to wait indefinitely until the partition heals.

	rsm.mu.Unlock()
	// wait until "Execute" goroutine return value
	msg := <-rsm.msgCh

	return rpc.OK, msg
}

// Introduction
// In this lab you will build a fault-tolerant key/value storage service using your Raft library from Lab 3. To clients, the service looks similar to the server of Lab 2. However, instead of a single server, the service consists of a set of servers that use Raft to help them maintain identical databases. Your key/value service should continue to process client requests as long as a majority of the servers are alive and can communicate, in spite of other failures or network partitions. After Lab 4, you will have implemented all parts (Clerk, Service, and Raft) shown in the diagram of Raft interactions.

// Clients will interact with your key/value service through a Clerk, as in Lab 2. A Clerk implements the Put and Get methods with the same semantics as Lab 2: Puts are at-most-once and the Puts/Gets must form a linearizable history.

// Providing linearizability is relatively easy for a single server. It is harder if the service is replicated, since all servers must choose the same execution order for concurrent requests, must avoid replying to clients using state that isn't up to date, and must recover their state after a failure in a way that preserves all acknowledged client updates.

// This lab has three parts. In part A, you will implement a replicated-state machine package, rsm, using your raft implementation; rsm is agnostic of the requests that it replicates. In part B, you will implement a replicated key/value service using rsm, but without using snapshots. In part C, you will use your snapshot implementation from Lab 3D, which will allow Raft to discard old log entries. Please submit each part by the respective deadline.

// You should review the extended Raft paper, in particular Section 7 (but not 8). For a wider perspective, have a look at Chubby, Paxos Made Live, Spanner, Zookeeper, Harp, Viewstamped Replication, and Bolosky et al.

// Start early.

// Getting Started
// We supply you with skeleton code and tests in src/kvraft1. The skeleton code uses the skeleton package src/kvraft1/rsm to replicate a server. A server must implement the StateMachine interface defined in rsm to replicate itself using rsm. Most of your work will be implementing rsm to provide server-agnostic replication. You will also need to modify kvraft1/client.go and kvraft1/server.go to implement the server-specific parts. This split allows you to re-use rsm in the next lab. You may be able to re-use some of your Lab 2 code (e.g., re-using the server code by copying or importing the "src/kvsrv1" package) but it is not a requirement.

// To get up and running, execute the following commands. Don't forget the git pull to get the latest software.

// $ cd ~/6.5840
// $ git pull
// ..
// Part A: replicated state machine (RSM) (moderate/hard)
// $ cd src/kvraft1/rsm
// $ go test -v
// === RUN   TestBasic
// Test RSM basic (reliable network)...
// ..
//     config.go:147: one: took too long
// In the common situation of a client/server service using Raft for replication, the service interacts with Raft in two ways: the service leader submits client operations by calling raft.Start(), and all service replicas receive committed operations via Raft's applyCh, which they execute. On the leader, these two activities interact. At any given time, some server goroutines are handling client requests, have called raft.Start(), and each is waiting for its operation to commit and to find out what the result of executing the operation is. And as committed operations appear on the applyCh, each needs to be executed by the service, and the results need to be handed to the goroutine that called raft.Start() so that it can return the result to the client.

// The rsm package encapsulates the above interaction.
// It sits as a layer between the service (e.g. a key/value database) and Raft.
// In rsm/rsm.go you will need to implement a "reader" goroutine that reads the applyCh, and a rsm.Submit() function that calls raft.Start() for a client operation and then waits for the reader goroutine to hand it the result of executing that operation.

// The service that is using rsm appears to the rsm reader goroutine as a StateMachine object providing a DoOp() method. The reader goroutine should hand each committed operation to DoOp(); DoOp()'s return value should be given to the corresponding rsm.Submit() call for it to return. DoOp()'s argument and return value have type any; the actual values should have the same types as the argument and return values that the service passes to rsm.Submit(), respectively.

// The service should pass each client operation to rsm.Submit().
// To help the reader goroutine match applyCh messages with waiting calls to rsm.Submit(),
// Submit() should wrap each client operation in an Op structure along with a unique identifier.
// Submit() should then wait until the operation has committed and been executed, and return the result of execution (the value returned by DoOp()).
// If raft.Start() indicates that the current peer is not the Raft leader, Submit() should return an rpc.ErrWrongLeader error.
// Submit() should detect and handle the situation in which leadership changed just after it called raft.Start(), causing the operation to be lost (never committed).

// For Part A, the rsm tester acts as the service, submitting operations that it interprets as increments on a state consisting of a single integer. In Part B you'll use rsm as part of a key/value service that implements StateMachine (and DoOp()), and calls rsm.Submit().

// If all goes well, the sequence of events for a client request is:

// The client sends a request to the service leader.
// The service leader calls rsm.Submit() with the request.
// rsm.Submit() calls raft.Start() with the request, and then waits.
// Raft commits the request and sends it on all peers' applyChs.
// The rsm reader goroutine on each peer reads the request from the applyCh and passes it to the service's DoOp().
// On the leader, the rsm reader goroutine hands the DoOp() return value to the Submit() goroutine that originally submitted the request, and Submit() returns that value.
// Your servers should not directly communicate; they should only interact with each other through Raft.

// Implement rsm.go: the Submit() method and a reader goroutine. You have completed this task if you pass the rsm 4A tests:
