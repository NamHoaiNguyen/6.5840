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
	newElectionTimeout := 350 + (rand.Int63() % 200)
	rf.electInterval = newElectionTimeout
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	// If someone else comes with a HIGHER term, update
	// node 's currentTerm to vote.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		((args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
				args.LastLogIndex >= rf.log[len(rf.log)-1].Index)) {
		// If candidates's log is up-to-date, vote for it.
		rf.state = Follower
		// Node MUST steps down if vote
		rf.votedFor = args.CandidateId
		// To avoid a node after voting quickly start election
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()

		reply.VoteGranted = true
		return
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()

	if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()

		return
	}

	if !reply.VoteGranted {
		return
	}

	*voteCount++
	if *voteCount > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		rf.state = Leader
		rf.votedFor = -1
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for server := range rf.peers {
			rf.nextIndex[server] = rf.log[len(rf.log)-1].Index + 1
			if server == rf.me {
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				continue
			}
			rf.matchIndex[server] = 0
		}

		// Notify to sendHeartbeat goroutine to send heartbeat message
		rf.cond.Broadcast()
	}
}

// Goroutine
func (rf *Raft) StartElect() {
	// var electInterval int64
	for !rf.killed() {
		rf.cond.L.Lock()
		if time.Now().UnixMilli()-rf.lastHeartbeatTimeRecv >= rf.electInterval && rf.state != Leader {
			// To begin an election, follower must becomes candidate
			rf.state = Candidate
			// Vote for itself
			rf.votedFor = rf.me
			// Increment its current term
			rf.currentTerm++

			// electInterval = rf.electInterval
			rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
			rf.ResetElectionTimeout()

			rf.persist()

			// Prepate request vote request
			voteReq := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me, // Node vote for itself to become leader
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term, // term of candidate's last log entry
			}
			voteCount := 1

			for server := range rf.peers {
				if server == rf.me {
					continue
				}

				go rf.sendRequestVote(server, voteReq, &voteCount)
			}
		}
		rf.cond.L.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}
