package raft

import (
	"time"

	"6.5840/raftapi"
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

// Goroutine
func (rf *Raft) UpdateStateMachineLogV2() {
	for !rf.killed() {
		rf.cond.L.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			logEntry := rf.log[rf.lastApplied]
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.cond.L.Unlock()
			rf.applyCh <- applyMsg
			rf.cond.L.Lock()
		}

		rf.cond.L.Unlock()
	}
}

// Goroutine
func (rf *Raft) SendHeartbeats() {
	for !rf.killed() {
		rf.cond.L.Lock()

		for rf.state != Leader {
			// Only leader sends heartbeat
			rf.cond.Wait()
		}
		rf.cond.L.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			// Leader doesn't be blocking to receive response from followers
			go rf.SendAppendEntries(server, true /*isHearbeat*/)
		}

		// Sleep for heartbeatInterval(ms)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) isReplicationNeeded(server int) bool {
	return (rf.state == Leader && rf.log[len(rf.log)-1].Index >= rf.nextIndex[server])
}

func (rf *Raft) ReplicateLog(server int) {
	for !rf.killed() {
		rf.peerCond[server].L.Lock()
		rf.cond.L.Lock()
		for !rf.isReplicationNeeded(server) {
			rf.cond.L.Unlock()
			rf.peerCond[server].Wait()
			rf.cond.L.Lock()
		}
		rf.cond.L.Unlock()
		rf.peerCond[server].L.Unlock()

		rf.SendAppendEntries(server, false /*isHeartbeat*/)
	}
}

// Leader execute append entry requests and handle response
func (rf *Raft) SendAppendEntries(server int, isHeartbeat bool) {
	rf.cond.L.Lock()
	if rf.state != Leader {
		// There is a case that before a leader sending append entry request to other node,
		// something happens that cause this node isn't leader anymore
		rf.cond.L.Unlock()
		return
	}

	// if len(rf.log)-1 < rf.nextIndex[server] && !isHeartbeat {
	// 	fmt.Println("Shouldn't send append entry message anymore!!!")
	// 	rf.cond.L.Unlock()
	// 	return
	// }

	if rf.nextIndex[server] < 1 {
		panic("nextIndex of a server MUST >= 1")
	}

	prevLogIndex := 0
	if rf.nextIndex[server] >= 1 {
		// To avoid out of index
		prevLogIndex = rf.nextIndex[server] - 1
	}

	var entriesList []LogEntry
	if !isHeartbeat {
		// Only append entry message has entries log
		entriesList = append(entriesList, rf.log[(prevLogIndex+1):]...)
	}

	appendEntryReq := &RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entriesList,
		LeaderCommit: rf.commitIndex,
	}
	appendEntryRes := &RequestAppendEntriesReply{}
	rf.cond.L.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", appendEntryReq, appendEntryRes)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if appendEntryRes.Term > rf.currentTerm {
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = appendEntryRes.Term
		rf.votedFor = -1
		return
	}

	if appendEntryRes.Term != rf.currentTerm || rf.state != Leader {
		// If leader isn't leader anymore
		// or leader's term isn't the same as before
		return
	}

	// Now leader 's term >= node's term
	if !appendEntryRes.Success {
		// There is inconsistency between leader's log and follower'log
		// Leader decrease its nextIndex corressponding with follower.
		rf.nextIndex[server]--
		rf.peerCond[server].Signal()
		return
	}

	// Incase appendEntryRes.Success == true
	// Update nextIndex(because we always send get to the end of leader's log
	// to send. In other words, rf.nextIndex[server] == logIndex = len(leader's log) - 1)
	rf.matchIndex[server] = prevLogIndex + len(entriesList)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	if rf.matchIndex[server] >= len(rf.log) {
		panic("MATCH INDEX CAN'T BE LARGER OR EQUAL LENGTH OF LOG")
	}

	N := rf.log[len(rf.log)-1].Index
	for N > rf.commitIndex {
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				count += 1
			}
		}

		if count > len(rf.peers)/2 {
			break
		}

		N -= 1
	}
	// Reupdate leader's commitIndex if majority of node commit up to N-th index
	rf.commitIndex = N

	// Update state machine log
	rf.cond.Broadcast()

	if rf.isReplicationNeeded(server) {
		rf.peerCond[server].Signal()
		return
	}
}

// RECEIVER
// If node receives append entries(including hearbeat) message
func (rf *Raft) AppendEntries(
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {
	rf.cond.L.Lock()

	if args.Term < rf.currentTerm {
		// if request's term < node's current term. Return false
		// (and node who sent request must be steps down to follower)
		reply.Success = false
		reply.Term = rf.currentTerm

		rf.cond.L.Unlock()
		return
	}

	reply.Term = args.Term

	if (len(rf.log) <= args.PrevLogIndex) ||
		(len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// If log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm, return success = false
		reply.Success = false
		rf.cond.L.Unlock()
		return
	}

	// Now args's term >= node's currentTerm
	rf.currentTerm = args.Term

	rf.state = Follower
	rf.votedFor = -1

	if args.Entries == nil {
		// Reupdate last time receive heartbeat message
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
			rf.cond.Broadcast()
		}

		reply.Success = true
		rf.cond.L.Unlock()
		return
	}

	// MUST unlock first before calling HandleAppendEntryLog.
	// Otherwise deadlock happens.
	rf.cond.L.Unlock()
	// Use goroutine can cause logic error.
	rf.HandleAppendEntryLog(args, reply)
}

// RECEIVER. Only respond to leader after apply entry to state machine
func (rf *Raft) HandleAppendEntryLog(
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	numEntries := 0
	for _, entry := range args.Entries {
		// If an existing entry conflicts with a new one(same index but different terms)
		// delete the existing entry and all that follow it
		logEntryIndex := entry.Index
		logEntryTerm := entry.Term

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

	// listEntries := args.Entries
	rf.log = append(rf.log, args.Entries[numEntries:]...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.cond.Broadcast()
	}

	reply.Success = true
}
