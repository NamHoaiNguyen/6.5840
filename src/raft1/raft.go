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
	currentTerm int // Current node's term

	votedFor int // CandidateId that received vote in current term(or null if none)

	lastHeartbeatTimeRecv int64 // Time of latest heartbeat that node receives from leader(
	// reupdated each time node receive heartbeat message) (unit: millisecond)

	state NodeState // current state of node

	log []LogEntry // log entries

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

	// Start of data not used in raft algorithm, but for easier to implement
	// Used for stepping back to find log leader until meeting a suitable one.
	virtualLog []LogEntry

	mutex sync.Mutex

	cond *sync.Cond
	// End of data not used in raft algorithm, but for easier to implement
}

type NodeState int

const (
	Candidate NodeState = iota
	Follower
	Leader
)

type LogEntry struct {
	// TODO(namnh, 3B) : Recheck the precision of index field ?
	Index   int // index of log
	Term    int // term when entry was received by leader
	Command any // command for state machine
}

type RequestAppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat)
	LeaderCommit int        // leader commit index
}

type RequestAppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool //  true of follower container entry matching prevLogIndex and prevLogTerm
}

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
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log) - 1)) {
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
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)

	fmt.Println("Namnh check rf.log: ", rf.log)

	// Replicate leader's log to other nodes
	go rf.SendAppendEntries(false /*isRetry*/)

	return len(rf.log) - 1, int(rf.currentTerm), (rf.state == Leader)
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

// Goroutine
func (rf *Raft) SendHeartbeats() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.state != Leader {
			// Only leader sends heartbeat
			rf.cond.Wait() // Unlock acquired at line 452
		}
		rf.cond.L.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			// Leader doesn't be blocking to receive response from followers
			go rf.LeaderHandleAppendEntriesResponse(i, true /*isHearbeat*/, false /*isRetry*/) // heartbeat message doesn't need to retry
		}

		// Sleep for heartbeatInterval(ms)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

// TODO(namnh, 3B): Implement
// 1 . Reinitialized nextIndex[] and matchIndex[] after relection

// Goroutine
// Should split this method with heartbeat
// Because heartbeat should be sent periodically
// TODO(namnh, 3B) : SendAppendEntries method needs to handle 2 cases.
// 1 : In normal situation, leader sends log entry to server.
// 2 : Log between leader and follower don't match, leader must resend
// log until the point that both node agree with each other about this log.
func (rf *Raft) SendAppendEntries(isRetry bool) {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.state != Leader {
			// Only leader can send append entries message
			rf.cond.Wait()
			// TODO(namnh, 3B) : Recheck using condition variable at here?
		}
		rf.cond.L.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go rf.LeaderHandleAppendEntriesResponse(i, false /*isHeartbeat*/, isRetry)
		}
	}
}

// Leader execute append entry requests and handle response
func (rf *Raft) LeaderHandleAppendEntriesResponse(server int, isHeartbeat bool, isRetry bool) {
	rf.cond.L.Lock()
	// fmt.Printf("Before Value of isRetry: %t and rf.nextIndex[server]: %d\n", isRetry, rf.nextIndex[server])

	if isRetry {
		// If leader and node's log mitmatch, decrease nextIndex
		rf.nextIndex[server]--
		// fmt.Printf("After Value of isRetry: %t and rf.nextIndex[server]: %d\n", isRetry, rf.nextIndex[server])
	}

	prevLogIndex := 0
	if (rf.nextIndex[server] >= 1) {
		prevLogIndex = rf.nextIndex[server] - 1
	}

	if prevLogIndex < 0 {
		panic("There is no way that a follower can deny a log with index = 0!!!")
	}

	logIndex := prevLogIndex + 1
	var entriesList []LogEntry
	if !isHeartbeat {
		// Only append entry message has entries log
		for logIndex < len(rf.log) {
			// Send all message from prevLogIndex + 1 to the end of log
			entriesList = append(entriesList, rf.log[logIndex])
			// fmt.Println("Value of entrieesList: ", entriesList)
			logIndex++
		}
	}

	appendEntryReq := &RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entriesList,
		LeaderCommit: rf.commitIndex,
	}
	rf.cond.L.Unlock()

	appendEntryRes := &RequestAppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", appendEntryReq, appendEntryRes)

	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if appendEntryRes.Term != rf.currentTerm || rf.state != Leader {
	// if rf.state != Leader {
		// If leader isn't leader anymore
		// or leader's term isn't the same as before
		return
	}

	if appendEntryRes.Term > rf.currentTerm {
		// fmt.Printf("Leader: %d should step down!!!\n", rf.me)
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = appendEntryRes.Term
		rf.votedFor = -1

		return
	}

	// Now leader 's term >= node's term
	if !appendEntryRes.Success {
		// There is inconsistency between leader's log and follower'log
		// Leader decrease its nextIndex corressponding with follower.
		rf.nextIndex[server]--
		// Resend log until log between server and follower match.
		go rf.SendAppendEntries(true /*isRetry*/)
	}
}

// RECEIVER
// If node receives append entries(including hearbeat) message
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// If is heartbeat message
	rf.cond.L.Lock()

	// if args.Entries == nil {
	// 	// Heartbeat message
	// 	if args.Term < rf.currentTerm {
	// 		reply.Term = rf.currentTerm
	// 		reply.Success = false
	// 		rf.cond.L.Unlock() // Unlock acquired at line 505
	// 		return
	// 	}

	// 	reply.Term = rf.currentTerm
	// 	rf.currentTerm = args.Term

	// 	reply.Success = true

	// 	var isCandidate bool
	// 	if rf.state == Candidate {
	// 		// If nodes's current state is candidate, send message to stop electing process
	// 		// NOTE: In case that an outdated leader rejoin cluster, no need to send anything.
	// 		// Because in this case, when outdated leader tries to send  heartbeat/append entry
	// 		// messages, other followers detects this leader's term < their terms then reply
	// 		// their terms. Outdated leader, based on follower's response will step down to follower.
	// 		isCandidate = true
	// 	}

	// 	rf.state = Follower
	// 	rf.votedFor = -1
	// 	// Reupdate last time receive heartbeat message
	// 	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
	// 	rf.cond.L.Unlock() // Unlock acquired at line 505

	// 	// Send message to collectVote flow that node accepts another node as its leader
	// 	// rf.appendEntryResponses <- reply.Success
	// 	if isCandidate {
	// 		go func() { rf.appendEntryResponses <- isCandidate }()
	// 	}

	// 	return
	// }

	// ============================Start 3B=======================================
	// Always return node's currentTerm
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// if request's term < node's current term. Return false
		// (and node who sent request must be steps down to follower)
		// fmt.Println("Term condition less than is hit!!!")
		reply.Success = false
		rf.cond.L.Unlock()
		return
	}

	// TODO(namnh, 3B) : Check this one
	// if args.PrevLogIndex == 0 {
	// 	rf.HandleAppendEntryLog(args, reply)
	// 	rf.cond.L.Unlock()
	// 	return
	// }
	if len(rf.log) <= args.PrevLogIndex && args.PrevLogIndex == 0 {
		panic("There is something wrong!!!")
	}

	if len(rf.log) <= args.PrevLogIndex ||
		(len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// If log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm, return success = false
		// fmt.Println("PrevLogIndex condition less than is hit!!!")
		reply.Success = false
		rf.cond.L.Unlock()
		return
	}

	// Now args's term >= node's currentTerm
	rf.currentTerm = args.Term

	// TODO(namnh, 3B) : Recheck this condition/
	// Except 2 above conditons, success = true
	// reply.Success = true

	if args.Entries == nil {
		// Heartbeat message
		// fmt.Println("Is heartbeat message!!!")
		var isCandidate bool
		if rf.state == Candidate {
			// If nodes's current state is candidate, send message to stop electing process
			// NOTE: In case that an outdated leader rejoin cluster, no need to send anything.
			// Because in this case, when outdated leader tries to send heartbeat/append entry
			// messages, other followers detects this leader's term < their terms then reply
			// their terms. Outdated leader, based on follower's response will step down to follower.
			isCandidate = true
		}

		rf.state = Follower
		rf.votedFor = -1
		// Reupdate last time receive heartbeat message
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()

		reply.Success = true
		rf.cond.L.Unlock()

		// Send message to collectVote flow that node accepts another node as its leader
		// rf.appendEntryResponses <- reply.Success
		if isCandidate {
			go func() { rf.appendEntryResponses <- isCandidate }()
		}

		return
	}

	rf.cond.L.Unlock()

	// Split handling append entry message to a seperate goroutine
	// TODO(namnh, 3B, IMPORTANT) : CHECK THIS CAREFULLY!!!
	// Use goroutine can cause logic error.
	rf.HandleAppendEntryLog(args, reply)
}

// RECEIVER. Only respond to leader after apply entry to state machine
func (rf *Raft) HandleAppendEntryLog(args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	// fmt.Println("HandleAppendEntryLog is called !!!")
	numEntries := 0

	// TODO(namnh, 3B) : Check this logic
	for _, log := range args.Entries {
		// If an existing entry conflicts with a new one(same index but different terms)
		// delete the existing entry and all that follow it
		logEntryIndex := log.Index
		logEntryTerm := log.Term

		if logEntryIndex < len(rf.log) && logEntryTerm == rf.log[logEntryIndex].Term {
			numEntries++
			continue
		}

		if logEntryIndex < len(rf.log) && logEntryTerm != rf.log[logEntryIndex].Term {
			// delete the existing entry and all that follow it
			rf.log = rf.log[:logEntryIndex]
			// In case condition is hit, all following entry after logEntryIndex was deleted.
			// Then new log entries form leader following logEntryIndex couldn't be found
			// So, we can skip the loop and just append remaining entries to node's log
			break
		}
	}

	// if (numEntries < len(args.Entries)) {
		rf.log = append(rf.log, args.Entries[numEntries:]...)
	// }

	// rf.log = append(rf.log, args.Entries...)

	// fmt.Printf("New log of node: %d after be appended!!!\n", rf.me)
	fmt.Println("Log value!!!", rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// TODO(namnh, 3B, IMPORTANT) : CHECK THIS CAREFULLY!!!
	// Maybe the reason is above logic wrong
	reply.Success = true
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
	rf.ResetHeartbeatTimeout()

	// AppendEntries response channel
	rf.appendEntryResponses = make(chan bool)
	rf.electionTimeout = make(chan bool)

	// TODO(namnh, 3B) : Start init data
	rf.log = []LogEntry{
		{Index: 0, Term: 0, Command: 0},
	}
	rf.virtualLog = []LogEntry{
		{Index: 0, Term: 0, Command: 0},
	}

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start elect goroutine to start elections
	go rf.StartElect()
	go rf.SendHeartbeats()

	return rf
}
