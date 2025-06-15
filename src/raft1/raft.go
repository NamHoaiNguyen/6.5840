package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
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

	// electInterval int64 // Random election timeout of node(each node has it owns elect interval)

	// heartbeatInterval int64 // Heartbeat timeout of each node

	lastHeartbeatTimeRecv int64 // Time of latest heartbeat that node receives from leader(
	// reupdated each time node receive heartbeat message) (unit: millisecond)

	state NodeState // current state of node

	log []LogEntry // log entries

	commitIndex int64 // index of highest log entry known to be commited(initialized to 0, increase monotonically)

	//==========New design for timer=================
	heartbeatInterval int64 // Heartbeat timeout of each node

	heartBeatTimer *time.Ticker

	electInterval int64 // Random election timeout of node(each node has it owns elect interval)

	// If no leader is voted in each election timeout -> start new round election
	electionTimer *time.Ticker

	// During elecction, if candidate's receive appendentry message from other node
	// if this entry is valid, candidate stops election and becomes follower
	appendEntryResponses chan bool

	// Channel to notify that a new election round should be started if no leader
	// is elected in previous term
	electionTimeout chan bool

	// Election first time
	firstElect bool

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
	// TODO(namnh) : Just define attributed needed for each class(3A)
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
	// TODO(namnh) : no need to check log in this lab3A
	reply.VoteGranted = true
	// TOOD(namnh) : Update election timeout when receving heartbeat(IMPORTANT)
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

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// func (rf *Raft) sendRequestAppendEntries(server int, args *)

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
			rf.mutex.Unlock()

			time.Sleep(time.Duration(rf.electInterval) * time.Millisecond)
			continue
		}

		// fmt.Printf("Node: %d wants to start electting!!!\n", rf.me)

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

		// rf.mutex.Unlock()

		// rf.CollectVote(voteReq)

		// TODO(namnh): recheck. Should request just after interval time
		// NOTE: We MUST pause. Otherwise, multi collect vote will be sent at the same term
		// <-rf.electionTimer.C
		sleepInterval := rf.electInterval
		rf.mutex.Unlock()

		go rf.CollectVote(voteReq)

		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
	}
	// voteCount := 1

	// for i := 0; i < len(rf.peers); i++ {
	// 	if i == rf.me {
	// 		// Of course, node shouldn't send request vote for itself
	// 		continue
	// 	}

	// 	voteReply := &RequestVoteReply{}

	// 	go rf.HandleVoteResponse(i, voteReq, voteReply, &voteCount)
	// }
}

// func (rf *Raft) HandleVoteResponse(server int, voteReq *RequestVoteArgs, voteReply *RequestVoteReply, voteCount *int) {
// 	ok := rf.sendRequestVote(server, voteReq, voteReply)
// 	if !ok {
// 		return
// 	}

// 	rf.mu.Lock()
// 	if voteReply.Term > rf.currentTerm {
// 		rf.currentTerm = voteReply.Term
// 		rf.state = Follower
// 		rf.votedFor = -1

// 		rf.mu.Unlock()

// 		return
// 	}

// 	if voteReply.VoteGranted {
// 		*voteCount++
// 		if *voteCount > len(rf.peers)/2 {
// 			// If candidate win majority, it becomes leader
// 			rf.state = Leader
// 			rf.mu.Unlock()

// 			// Send heartbeat to other nodes to confirm its leadership
// 			go rf.SendHeartbeats()

// 			return // Should stop election
// 		}
// 	}

// 	rf.mu.Unlock()

// }

// ONLY 1 GOROUTINE CALL THIS FUNCTION(so only locks at critical sections instead of from the begining to end of function)
func (rf *Raft) CollectVote(voteReq *RequestVoteArgs) {
	// rf.mu.Lock()
	// defer rf.mutex.Unlock()
	// We want to use buffered channel instead of unbuffered
	// Channel size of peers -1, because a candidate doesn't request itself
	rf.mu.Lock()
	peerServers := len(rf.peers)
	// rf.mu.Unlock()

	voteResult := make(chan *RequestVoteReply, peerServers)

	// Vote reply
	voteReply := &RequestVoteReply{}

	voteCounts := 1

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// Of course, node shouldn't send request vote for itself
			continue
		}

		go rf.sendRequestVote(i, voteReq, voteReply, voteResult)
	}

	for !rf.killed() {
		// TODO(namnh) : need to handle 3 cases
		// 1 : Candidate receive majority vote -> become leader
		// 2 : Candidate doesn't receive majority
		//     vote in election timeout -> elect at next term
		// 3 : Candidate receives append entries (heartbeat message) from other node(for example A)
		// if A'term >= candidate term -> candidate becomes follower and accepts A as its leader
		// else -> candidate continues its vote process
		select {
		case voteReply := <-voteResult:
			rf.mu.Lock()
			fmt.Printf("Namnh check voteReply.term: %d and voteGranted: %t and candidate's currentTem: %d\n", voteReply.Term, voteReply.VoteGranted, rf.currentTerm)

			if voteReply.Term > rf.currentTerm {
				fmt.Printf("Node %d candidate become follower because of invalid term!!!\n", rf.me)

				rf.currentTerm = voteReply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.ResetElectionTimeout()
				// rf.ResetHeartbeatTimeout()
				rf.mu.Unlock()

				return
			}
			rf.mu.Unlock()

			// node reply's term < candidate's term
			if voteReply.VoteGranted {
				rf.mu.Lock()

				voteCounts++
				if voteCounts > len(rf.peers)/2 {
					// If candidate win majority
					// rf.mu.Lock()
					// Candidate become leader
					rf.state = Leader
					fmt.Printf("Node: %d becomes leader at term: %d!!!\n", rf.me, rf.currentTerm)
					rf.ResetElectionTimeout()
					// rf.ResetHeartbeatTimeout()

					rf.mu.Unlock()

					// Send heartbeat to other nodes to confirm its leadership
					go rf.SendHeartbeats()

					return // Should stop election
				}

				rf.mu.Unlock()

			}
		case shouldStopVote := <-rf.appendEntryResponses:
			// Candidate receive append entries node from leader
			fmt.Printf("Node : %d Receive message from another node and it wants to be leader", rf.me)

			if shouldStopVote {
				rf.mu.Lock()
				rf.state = Follower
				rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
				rf.votedFor = -1
				rf.ResetElectionTimeout()
				// rf.ResetHeartbeatTimeout()

				rf.mu.Unlock()

				return
			}

		case <-rf.electionTimer.C:
			// case <-rf.electionTimeout:
			// Election time of this term is out of.
			fmt.Println("Election interval is timeout. Vote again!!!")
			rf.mu.Lock()
			// if rf.state == Candidate {
			// go rf.StartElect()
			// }

			// Reset election timeout for next round
			rf.ResetElectionTimeout()
			// rf.ResetHeartbeatTimeout()

			// rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()

			rf.mu.Unlock()

			return
		}
	}
}

// Goroutine
func (rf *Raft) SendHeartbeats() {
	// rf.mu.Lock()
	// if rf.state != Leader {
	// 	// Only leader sends heartbeat
	// 	rf.mu.Unlock()
	// 	return
	// }
	// rf.mu.Unlock()

	for !rf.killed() {
		// fmt.Println("BEFORE TAKING LOCK TO SEND HEARTBEAT")

		// rf.mu.Lock()
		_, state := rf.GetState()
		rf.mu.Lock()
		if rf.state != Leader {
			// fmt.Println("Only leader can send log heartbeat!!!")
			// Only leader sends heartbeat
			heartbeatInterval := rf.heartbeatInterval
			rf.mu.Unlock()
			// <-rf.heartBeatTimer.C
			time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)

			return
		}

		fmt.Printf("Leader : %d send heartbeat at: %d term and state is leader: %t!!!\n", rf.me, rf.currentTerm, state)

		// Prepare params to send
		heartBeatReq := &RequestAppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// TODO(namnh) : What params should we do for these 3 ?
			// Do we need them for Lab3A?
			// PrevLogIndex: ,
			// PrevLogTerm: ,
			// Empty for heartbeat message
			Entries: nil,
			// LeaderCommit: ,
		}
		rf.mu.Unlock()

		heartbeatRes := &RequestAppendEntriesReply{}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			// Yeah, leader doesn't be blocking to receive response from followers
			go rf.LeaderHandleAppendEntriesResponse(i, heartBeatReq, heartbeatRes)
		}

		// <-rf.heartBeatTimer.C
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
		fmt.Printf("Leader: %d should step down!!!", rf.me)
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = reply.Term
		rf.votedFor = -1

		return
	}

	// TODO(namnh) : Maybe it is enought for lab3A ?
}

// RECEIVER
// If node receives append entries(including hearbeat) message
// TODO(namnh) : Note this function should be called anytime
func (rf *Raft) HandleRequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// If is heartbeat message
	rf.mu.Lock()

	if args.Entries == nil {
		// Heartbeat message
		if args.Term < rf.currentTerm {
			// reply.Term = rf.currentTerm
			// fmt.Printf("node state: %d is fales to add entry into node: %d!!!\n", args.LeaderId, rf.me)
			// fmt.Printf("Value of args.Term: %d and rf.currentTerm: %d\n", args.Term, rf.currentTerm)

			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		// Now append entries request's term >= node's term
		// First, update node's current term
		// rf.mu.Lock()

		// fmt.Printf("node id: %d should becomes followers at this step!!!\n", rf.me)
		// TODO(namnh) : Order of these 2 lines are important or not ?
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
		rf.ResetElectionTimeout()
		rf.mu.Unlock()

		// Send message to collectVote flow that node accepts another node as its leader
		// rf.appendEntryResponses <- reply.Success
		if isCandidate {
			go func() { rf.appendEntryResponses <- isCandidate }()
		}
		// rf.appendEntryResponses <- reply.Success
		return

		// TODO(namnh )Check log consistency(not for lab3A) ?
		// if

		// // TODO(namnh : 3A) : Should it happen when election happens flow ?
		// if rf.state == Candidate {
		// 	// If node's role = candidate, immediately become follower
		// 	rf.state = Follower
		// 	return
		// }
	}
}

// ONLY belongs to heartbeat timeout
func (rf *Raft) ResetHeartbeatTimeout() {
	//TODO(namnh) : Do we need lock?
	newHeartbeatTimeout := (100 + (rand.Int63() % 150))
	rf.heartbeatInterval = newHeartbeatTimeout
	if rf.heartBeatTimer != nil {
		rf.heartBeatTimer.Stop()
	}
	rf.heartBeatTimer = time.NewTicker((time.Duration)(newHeartbeatTimeout) * time.Millisecond)
}

// ONLY belongs to vote election goroutine
func (rf *Raft) ResetElectionTimeout() {
	//TODO(namnh) : Do we need lock?

	newElectionTimeout := (400 + (rand.Int63n(201)))
	rf.electInterval = newElectionTimeout
	fmt.Printf("Node : %d generate new Value of new newElectionTimeout for next election: %d!!!\n", rf.me, newElectionTimeout)
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.electionTimer = time.NewTicker(
		(time.Duration)(newElectionTimeout) * time.Millisecond)
}

// There are 2 events that execute after each interval.
// 1 Send Heartbeat to leader IF node = FOLLOWER(each HeartbeatInterval)
// 2 Elect new leader in case there is no leader voted at previous term(random voteInterval)
func (rf *Raft) tickerV3() {
	for !rf.killed() {
		// Start vote election each interval
		<-rf.electionTimer.C
		rf.mu.Lock()
		// Check that we should start election or not
		if time.Now().UnixMilli()-rf.lastHeartbeatTimeRecv >= rf.electInterval &&
			rf.state != Leader {
			fmt.Printf("Server %d start election in tickerV3! with server's state: %d!!!\n", rf.me, rf.state)
			// TODO(namnh) : how should we stop previous CollectVote ?
			if rf.firstElect {
				rf.firstElect = false
			} else {
				rf.electionTimeout <- true
				fmt.Println("Stop previous election to start a new one!!!")
			}
			go rf.StartElect()
			rf.ResetElectionTimeout()
		}

		rf.mu.Unlock()
	}
}

// func (rf *Raft) ticker() {
// 	for rf.killed() == false {

// 		// Your code here (3A)
// 		// Check if a leader election should be started.

// 		// If should start leader election
// 		// now - lastHeartbeatTaken >= HeartbeatInterval

// 		// pause for a random amount of time between 50 and 350
// 		// milliseconds.
// 		ms := 50 + (rand.Int63() % 300)
// 		time.Sleep(time.Duration(ms) * time.Millisecond)
// 	}
// }

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

	// heartbeat interval of node
	// rf.heartbeatInterval = rf.ResetHeartbeatTimeout()
	// get election timeout at initializaiton
	// rf.lastElectTimeout = rf.ResetVoteTimeout()
	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
	// Starting state of node is always = Follower
	rf.state = Follower
	// New timer
	// rf.electInterval = 0
	// rf.electionTimer = time.NewTicker(0)
	rf.ResetElectionTimeout()
	rf.ResetHeartbeatTimeout()
	// rf.heartBeatTimer = time.NewTimer(0)

	// AppendEntries response channel
	rf.appendEntryResponses = make(chan bool)

	rf.electionTimeout = make(chan bool)
	rf.firstElect = true

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()
	// go rf.tickerV3()
	go rf.StartElect()
	// TODO(namnh) : Should we send heartbeat at here ?
	go rf.SendHeartbeats()

	return rf
}
