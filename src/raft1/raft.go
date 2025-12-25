package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Current node's term

	votedFor int // CandidateId that received vote in current term(or null if none)

	lastHeartbeatTimeRecv int64 // Time of latest heartbeat that node receives from leader(
	// reupdated each time node receive heartbeat message) (unit: millisecond)

	state NodeState // current state of node

	log []LogEntry // log entries

	heartbeatInterval int64 // Heartbeat timeout interval of each node

	electInterval int64 // Random election timeout of node(each node has it owns elect interval)

	// index of highest log entry known to be
	// committed (initialized to 0, increases
	// monotonically)
	commitIndex int

	// index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)
	lastApplied int

	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	// (Volatile state on leaders)
	// (Reinitialized after election)
	nextIndex []int

	// 	for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	// (Volatile state on leaders)
	// (Reinitialized after election)
	matchIndex []int

	// Specific for Lab
	applyCh chan raftapi.ApplyMsg

	snapshotMsg *raftapi.ApplyMsg

	cond *sync.Cond

	peerCond []*sync.Cond
}

type NodeState int

const (
	Candidate NodeState = iota
	Follower
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	return int(rf.currentTerm), rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	if rf.persister.ReadSnapshot() != nil {
		rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
		return
	}

	rf.persister.Save(rf.encodeState(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		DPrintf("[%d]: read persist, currentTerm: %d, votedFor: %d, logs: %v\n", rf.me, currentTerm, votedFor, logs)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastApplied = rf.log[0].Index
		rf.commitIndex = rf.log[0].Index
		rf.state = Follower
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	return w.Bytes()
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	// Truncate the log(keep index 0-th)
	// MUST remove dump entry at 0-th index and replace it
	// with entry at index-th
	rf.log = append([]LogEntry{}, rf.log[index-rf.log[0].Index:]...)
	rf.log[0].Command = nil

	rf.persister.Save(rf.encodeState(), snapshot)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	// Appending new log entry into leader's log
	newLogEntry := LogEntry{
		Index:   rf.log[len(rf.log)-1].Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.peerCond[server].Signal()
	}

	return rf.log[len(rf.log)-1].Index, int(rf.currentTerm), (rf.state == Leader)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// TODO(namnh) : recheck
	// close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	// func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// starting term of node = 0
	rf.currentTerm = 0

	// Init candidate id that received vote in current term
	rf.votedFor = -1
	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
	// Node's beginning state = Follower
	rf.state = Follower
	rf.ResetElectionTimeout()
	rf.heartbeatInterval = 110

	// Dummy entry to let log start from 1th-index
	rf.log = []LogEntry{
		{Index: 0, Term: 0, Command: nil},
	}

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Assing applyMsg chan
	rf.applyCh = applyCh

	rf.cond = sync.NewCond(&rf.mu)
	rf.peerCond = make([]*sync.Cond, len(rf.peers))
	for server := range rf.peerCond {
		// Each peer uses it owns mutex
		rf.peerCond[server] = sync.NewCond(&sync.Mutex{})
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Snapshot
	rf.snapshotMsg = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start elect each time election interval is timeout
	go rf.StartElect()
	// Leader periodically send heartbeat to other nodes
	go rf.SendHeartbeats()
	// Background task which update log to state machine
	go rf.ApplyLog()
	// Init retry sending message
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.ReplicateLog(server)
	}

	return rf
}
