package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int //candidate requesting vote id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //  currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate recived vote
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote

	// TODO(namnh, 3B) : Modify this method for Lab3B
	// If hadn't voted for anyone else in this term, or voted for candidate sent request this term
	// or candidate'sterm > node's current term && candidate's log is up-to-date
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		// if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm {
		// Update currentTerm, votedFor and steps down to follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = Follower

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.ResetElectionTimeout()

		return
	}

	// All other cases, no vote
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
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

// Goroutine
func (rf *Raft) StartElect() {
	for !rf.killed() {
		rf.cond.L.Lock()

		if time.Now().UnixMilli()-rf.lastHeartbeatTimeRecv < rf.electInterval ||
			rf.state == Leader {
			rf.cond.L.Unlock()

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
		// TODO(namnh, 3B) : Modify request
		voteReq := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,                        // Node vote for itself to become leader
			LastLogIndex: (len(rf.log) - 1),            // index of candidate's last log entry
			LastLogTerm:  (rf.log[len(rf.log)-1].Term), // term of candidate's last log entry
		}

		sleepInterval := rf.electInterval
		// rf.ResetElectionTimeout()

		rf.cond.L.Unlock()

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
			rf.cond.L.Lock()

			if voteReply.Term > rf.currentTerm {
				// If someone else replies a term > candidate's current term,
				// candidate steps back to follower and update it current term
				// with reply's term
				rf.currentTerm = voteReply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.ResetElectionTimeout()
				rf.cond.L.Unlock() // Unlock acquired at line 364

				return
			}
			rf.cond.L.Unlock() // Unlock acquired at line 364

			// node reply's term < candidate's term
			if voteReply.VoteGranted {
				rf.cond.L.Lock()

				voteCounts++
				if voteCounts > len(rf.peers)/2 {
					// If acandidate win majority,  becomes leader
					rf.state = Leader
					rf.ResetElectionTimeout()
					// TODO(namnh, 3B) : Reinitialized nextIndex and matchIndex
					// Should we split it into another goroutine ?
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))

					for index, _ := range rf.nextIndex {
						// Initialized to leader last log index + 1
						rf.nextIndex[index] = len(rf.log)
						rf.matchIndex[index] = 0
					}

					// Notify to sendheartbeats goroutine
					// rf.cond.Signal()
					rf.cond.Broadcast()
					rf.cond.L.Unlock()
					return
				}

				rf.cond.L.Unlock()
			}
		case <-rf.appendEntryResponses:
			// Candidate receive append entries node from leader
			rf.cond.L.Lock()
			// Transit from canditate -> follower
			rf.state = Follower
			// Reupdate latest time that a node receives a heartbeat message
			rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
			rf.votedFor = -1
			rf.ResetElectionTimeout()

			rf.cond.L.Unlock() // Unlock acquired at line 402

			return
		case <-rf.electionTimer.C:
			// Out of time election. Reelect with next term
			rf.cond.L.Lock()
			// Reset election timeout for next round
			rf.ResetElectionTimeout()
			rf.cond.L.Unlock()

			return
		}
	}
}
