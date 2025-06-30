package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// Start of data not used in raft algorithm, but for easier to implement
	// Use to fast find a majority of matchIndex log to update commitIndex
	// Key is index of log, value is number of nodes that commited that index
	// During elecction, if candidate's receive appendentry message from other node
	// if this entry is valid, candidate stops election and becomes follower
	// appendEntryResponses chan bool

	// Channel to notify that a new election round should be started if no leader
	// is elected in previous term
	electionTimeout chan bool

	// Retry channel. Use to resend ONLY append entry message to follower nodes
	retryCh chan int

	// Done channekl. To stop goroutine
	done chan struct{}

	collectLog map[int]int

	cond *sync.Cond

	peerCond []*sync.Cond
	// End of data not used in raft algorithm, but for easier to implement
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	// fmt.Printf("Node: %d receive command with state: %d\n", rf.me, rf.state)
	// fmt.Println("Value of command that node receive", command)

	if rf.state != Leader {
		return -1, -1, false
	}

	// Appending new log entry into leader's log
	newLogEntry := LogEntry{
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)

	// rf.matchIndex[rf.me] = len(rf.log) - 1
	// rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	// fmt.Printf("Leader is :%d and currentTerm: %d\n", rf.me, rf.currentTerm)
	// fmt.Println("Namnh check rf.log at leader node each PUT command: ", rf.log)

	// Replicate leader's log to other nodes
	// go rf.SendAppendEntries()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.peerCond[server].L.Lock()
		rf.peerCond[server].Signal()
		rf.peerCond[server].L.Unlock()
	}

	// go rf.ReplicateLog()
	// rf.ReplicateLog()

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
}

func (rf *Raft) killed() bool {
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

	// AppendEntries response channel
	rf.electionTimeout = make(chan bool)

	// TODO(namnh, 3B) : Start init data
	rf.log = []LogEntry{
		{Index: 0, Term: 0, Command: nil},
	}
	rf.collectLog = make(map[int]int)

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Create retry channel
	rf.retryCh = make(chan int)

	// Assing applyMsg chan
	rf.applyCh = applyCh

	rf.cond = sync.NewCond(&rf.mu)
	rf.peerCond = make([]*sync.Cond, len(rf.peers))
	for server := range rf.peerCond {
		// Each peer uses it owns mutex
		rf.peerCond[server] = sync.NewCond(&sync.Mutex{})
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start elect each time election interval is timeout
	go rf.StartElect()
	// Leader periodically send heartbeat to other nodes
	go rf.SendHeartbeats()
	// Background task which update log to state machine
	go rf.UpdateStateMachineLogV2()
	// Init retry sending message
	// go rf.ReplicateLog()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.SendAppendEntriesV2(server)
	}

	return rf
}
