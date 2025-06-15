package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
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
	currentTerm int64 // Current node's term

	votedFor int // CandidateId that received vote in current term(or null if none)

	lastHeartbeatTimeRecv int64 // Time of latest heartbeat that node receives from leader(
	// reupdated each time node receive heartbeat message) (unit: millisecond)

	state NodeState // current state of node

	log []LogEntry // log entries

	commitIndex int64 // index of highest log entry known to be commited(initialized to 0, increase monotonically)

	heartbeatInterval int64 // Heartbeat timeout interval of each node

	heartBeatTimer *time.Ticker // Used to send heartbeat each heartbeatInterval

	electInterval int64 // Random election timeout of node(each node has it owns elect interval)

	// If no leader is voted in each election timeout -> start new round election
	electionTimer *time.Ticker

	// During elecction, if candidate's receive appendentry message from other node
	// if this entry is valid, candidate stops election and becomes follower
	appendEntryResponses chan bool

	// Channel to notify that a new election round should be started if no leader
	// is elected in previous term
	electionTimeout chan bool

	mutex sync.Mutex
}

type NodeState int

const (
	Candidate NodeState = iota
	Follower
	Leader
)

type LogEntry struct {
	Term    int64 // term when entry was received by leader
	Command any   // command for state machine
}

type RequestAppendEntriesArgs struct {
	Term         int64      // Leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int64      // index of log entry immediately preceding new ones
	PrevLogTerm  int64      // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat)
	LeaderCommit int64      // leader commit index
}

type RequestAppendEntriesReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool  //  true of follower container entry matching prevLogIndex and prevLogTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64 // candidate's term
	CandidateId  int   //candidate requesting vote id
	LastLogIndex int64 // index of candidate's last log entry
	LastLogTerm  int64 // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64 //  currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate recived vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	// TODO(namnh) : Lab3A - Only care about term
	// TODO(namnh) : Modify this method for Lab3B
	if args.Term < rf.currentTerm ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		// If candidate's term < node' s currentTerm or voted for someone else
		// return false(node won't vote for candidate) and
		// return currentTerm of node to let candidate update its term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// Now args.Term == rf.currentTerm
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		// If voted for the same candidate in this term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.ResetElectionTimeout()

		return
	}

	if args.Term > rf.currentTerm {
		// Update currentTerm, votedFor and steps down to follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// still trying to elect leader
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.ResetElectionTimeout()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteResult chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}

	voteResult <- reply
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

// Goroutine
func (rf *Raft) StartElect() {
	for !rf.killed() {
		rf.mutex.Lock()

		if time.Now().UnixMilli()-rf.lastHeartbeatTimeRecv < rf.electInterval ||
			rf.state == Leader {
			rf.mutex.Unlock() // Unlock required at line 315

			time.Sleep(time.Duration(rf.electInterval) * time.Millisecond)
			continue
		}

		// To begin an election, follower must transit candidate
		rf.state = Candidate
		// Candidate vote for itself
		rf.votedFor = rf.me
		// Increment its current term
		rf.currentTerm++

		// Prepate request vote request
		voteReq := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me, // Node vote for itself to become leader
			// lastLogIndex: (int64)(len(rf.log) - 1),     // index of candidate's last log entry
			// lastLogTerm:  (rf.log[len(rf.log)-1].Term), // term of candidate's last log entry
		}

		sleepInterval := rf.electInterval

		rf.mutex.Unlock() // Unlock required at line 315

		go rf.CollectVote(voteReq)

		// NOTE: We MUST pause. Otherwise, multi collect vote will be sent at the same term
		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
	}
}

// ONLY 1 GOROUTINE CALL THIS FUNCTION
func (rf *Raft) CollectVote(voteReq *RequestVoteArgs) {
	// We want to use buffered channel instead of unbuffered
	// Channel size of peers -1, because a candidate doesn't request itself
	voteResult := make(chan *RequestVoteReply, len(rf.peers)-1)

	// Vote reply
	voteReply := &RequestVoteReply{}

	voteCounts := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// Node shouldn't send request vote to itself
			continue
		}

		go rf.sendRequestVote(i, voteReq, voteReply, voteResult)
	}

	for !rf.killed() {
		// Need to handle 3 cases
		// 1 : Candidate receive majority vote -> become leader
		// 2 : Candidate doesn't receive majority
		//     vote in election timeout -> elect at next term
		// 3 : Candidate receives append entries (heartbeat message) from other node(for example A)
		// if A'term >= candidate term -> candidate becomes follower and accepts A as its leader
		// else -> candidate continues its vote process
		select {
		case voteReply := <-voteResult:
			rf.mu.Lock()

			if voteReply.Term > rf.currentTerm {
				// If someone else replies a term > candidate's current term,
				// candidate steps back to follower and update it current term
				// with reply's term
				rf.currentTerm = voteReply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.ResetElectionTimeout()
				rf.mu.Unlock() // Unlock acquired at line 364

				return
			}
			rf.mu.Unlock() // Unlock acquired at line 364

			// node reply's term < candidate's term
			if voteReply.VoteGranted {
				rf.mu.Lock()

				voteCounts++
				if voteCounts > len(rf.peers)/2 {
					// If acandidate win majority,  becomes leader
					rf.state = Leader
					rf.ResetElectionTimeout()
					rf.mu.Unlock() // Unlock acquired at line 382

					// Send heartbeat to other nodes to confirm its leadership
					go rf.SendHeartbeats()

					return
				}

				rf.mu.Unlock() // Unlock acquired at line 382

			}
		case <-rf.appendEntryResponses:
			// Candidate receive append entries node from leader
			rf.mu.Lock()
			// Transit from canditate -> follower
			rf.state = Follower
			// Reupdate latest time that a node receives a heartbeat message
			rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
			rf.votedFor = -1
			rf.ResetElectionTimeout()

			rf.mu.Unlock() // Unlock acquired at line 402

			return
		case <-rf.electionTimer.C:
			// Out of time election. Reelect with next term
			rf.mu.Lock()
			// Reset election timeout for next round
			rf.ResetElectionTimeout()
			rf.mu.Unlock() // Unlock acquired at line 415

			return
		}
	}
}

// Goroutine
func (rf *Raft) SendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			// Only leader sends heartbeat
			rf.mu.Unlock() // Unlock acquired at line 428

			// Sleep for heartbeatInterval(ms)
			time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)

			return
		}

		// Prepare params to send
		heartBeatReq := &RequestAppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// TODO(namnh) : Add commented params to support remaining parts of Lab3
			// PrevLogIndex: ,
			// PrevLogTerm: ,
			// Empty for heartbeat message
			Entries: nil,
			// LeaderCommit: ,
		}
		rf.mu.Unlock() // Unlock acquired at line 428

		heartbeatRes := &RequestAppendEntriesReply{}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			// Yeah, leader doesn't be blocking to receive response from followers
			go rf.LeaderHandleAppendEntriesResponse(i, heartBeatReq, heartbeatRes)
		}

		// Sleep for heartbeatInterval(ms)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

// SENDER
func (rf *Raft) LeaderHandleAppendEntriesResponse(
	server int,
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {

	ok := rf.peers[server].Call("Raft.HandleRequestAppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.state != Leader {
		// If leader isn't leader anymore
		// or leader's term isn't the same as before
		return
	}

	if reply.Term > rf.currentTerm {
		// fmt.Printf("Leader: %d should step down!!!\n", rf.me)
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = reply.Term
		rf.votedFor = -1

		return
	}
}

// RECEIVER
// If node receives append entries(including hearbeat) message
func (rf *Raft) HandleRequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// If is heartbeat message
	rf.mu.Lock()

	if args.Entries == nil {
		// Heartbeat message
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock() // Unlock acquired at line 505
			return
		}

		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term

		reply.Success = true

		var isCandidate bool
		if rf.state == Candidate {
			// If nodes's current state is candidate, send message to stop electing process
			// NOTE: In case that an outdated leader rejoin cluster, no need to send anything.
			// Because in this case, when outdated leader tries to send  heartbeat/append entry
			// messages, other followers detects this leader's term < their terms then reply
			// their terms. Outdated leader, based on follower's response will step down to follower.
			isCandidate = true
		}

		rf.state = Follower
		rf.votedFor = -1
		// Reupdate last time receive heartbeat message
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.mu.Unlock() // Unlock acquired at line 505

		// Send message to collectVote flow that node accepts another node as its leader
		// rf.appendEntryResponses <- reply.Success
		if isCandidate {
			go func() { rf.appendEntryResponses <- isCandidate }()
		}

		return
	}
}

// NEED to acquire lock before calling
func (rf *Raft) ResetHeartbeatTimeout() {
	newHeartbeatTimeout := (100 + (rand.Int63() % 150))
	rf.heartbeatInterval = newHeartbeatTimeout
	if rf.heartBeatTimer != nil {
		rf.heartBeatTimer.Stop()
	}
	rf.heartBeatTimer = time.NewTicker((time.Duration)(newHeartbeatTimeout) * time.Millisecond)
}

// NEED to acquire lock before calling
func (rf *Raft) ResetElectionTimeout() {
	newElectionTimeout := (400 + (rand.Int63n(201)))
	rf.electInterval = newElectionTimeout
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.electionTimer = time.NewTicker(
		(time.Duration)(newElectionTimeout) * time.Millisecond)
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// starting term of node = 0
	rf.currentTerm = 0

	// Init candidate id that received vote in current term
	rf.votedFor = -1
	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
	// Starting state of node is always = Follower
	rf.state = Follower
	rf.ResetElectionTimeout()
	rf.ResetHeartbeatTimeout()

	// AppendEntries response channel
	rf.appendEntryResponses = make(chan bool)
	rf.electionTimeout = make(chan bool)

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start elect goroutine to start elections
	go rf.StartElect()
	go rf.SendHeartbeats()

	return rf
}
