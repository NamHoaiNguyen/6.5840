package raft

import (
	"time"

	"6.5840/raftapi"
)

type LogEntry struct {
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
	XTerm   int  // term in the conflicting entry(if any)
	XIndex  int  // index of first entry with that term
	XLen    int  // log length
}

// Goroutine
func (rf *Raft) ApplyLog() {
	for !rf.killed() {
		rf.cond.L.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}

		if rf.snapshotMsg != nil {
			msg := rf.snapshotMsg
			rf.snapshotMsg = nil
			rf.cond.L.Unlock()
			rf.applyCh <- *msg
		} else {
			rf.cond.L.Unlock()
		}

		rf.cond.L.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			startIndexInMem := rf.lastApplied - rf.log[0].Index

			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[startIndexInMem].Command,
				CommandIndex: rf.log[startIndexInMem].Index,
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
			// go rf.SendAppendEntries(server)
			go rf.SendRPC(server)
		}

		// Sleep for heartbeatInterval(ms)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

// NOT THREAD-SAFE
func (rf *Raft) isReplicationNeeded(server int) bool {
	return rf.state == Leader && rf.log[len(rf.log)-1].Index >= rf.nextIndex[server]
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

		rf.SendRPC(server)
	}
}

func (rf *Raft) SendRPC(server int) {
	rf.cond.L.Lock()

	if rf.nextIndex[server] > rf.log[0].Index {
		appendEntryReq, appendEntryRes := rf.SetupAppendEntryParams(server)
		// MUST Unlock before calling SendAppendEntries
		rf.cond.L.Unlock()

		rf.SendAppendEntries(server, appendEntryReq, appendEntryRes)
		return
	}

	installSnapshotReq, installSnapshotRes := rf.SetupInstallSnapshotParams(server)
	// MUST Unlock before calling SendInstallSnapshot
	rf.cond.L.Unlock()

	rf.SendInstallSnapshot(server, installSnapshotReq, installSnapshotRes)
}

// NOT THREAD-SAFE
func (rf *Raft) SetupAppendEntryParams(server int) (RequestAppendEntriesArgs, RequestAppendEntriesReply) {
	// Need a mapping between real index of log and log's index in slice because of snapshot
	prevLogIndex := rf.nextIndex[server] - 1
	firstIndex := rf.log[0].Index

	// Get Index of prevLogIndex in slice index
	// (Start index can be 0 or rf.log[0].Index in case snapshot)
	prevLogIndexInMem := prevLogIndex - firstIndex
	if prevLogIndexInMem < 0 {
		panic("prevLogIndexInMem CAN'T LESS THAN 0 IN SetupAppendEntryParams")
	}

	// Length of log that leader will send to follower
	logLengthSend := len(rf.log) - prevLogIndexInMem - 1
	entriesList := make([]LogEntry, logLengthSend)
	copy(entriesList, rf.log[prevLogIndexInMem+1:])

	return RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndexInMem].Term,
		Entries:      entriesList,
		LeaderCommit: rf.commitIndex,
	}, RequestAppendEntriesReply{}
}

// Leader execute append entry requests and handle response
func (rf *Raft) SendAppendEntries(
	server int,
	req RequestAppendEntriesArgs,
	res RequestAppendEntriesReply) {
	rf.cond.L.Lock()
	if rf.state != Leader {
		// There is a case that before a leader sending append entry request to other node,
		// something happens that cause this node isn't leader anymore
		rf.cond.L.Unlock()
		return
	}
	rf.cond.L.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &req, &res)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()

	if req.Term > rf.currentTerm {
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = res.Term
		rf.votedFor = -1
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}

	if res.Term != rf.currentTerm || rf.state != Leader {
		// If leader isn't leader anymore
		// or leader's term isn't the same as before
		return
	}

	// Now leader 's term >= node's term
	if !res.Success {
		// If there is inconsistency between leader's log and follower'log
		// Leader will change follower's nextIndex
		// Optimization
		if res.XLen != -1 {
			// follower's log is too short
			rf.nextIndex[server] = res.XLen
		} else {
			prevLogIndexInMem := req.PrevLogIndex - rf.log[0].Index
			for i := prevLogIndexInMem; i >= 0; i-- {
				// if leader HAS XTerm
				if rf.log[i].Term == res.XTerm {
					rf.nextIndex[server] = i + 1 + rf.log[0].Index
					rf.peerCond[server].Signal()
					return
				}
			}

			// Leader doesn't have XTerm
			rf.nextIndex[server] = res.XIndex
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}

		rf.peerCond[server].Signal()
		return
	}

	// Incase appendEntryRes.Success == true
	// Update nextIndex(because we always send get to the end of leader's log
	// to send.
	// So, rf.nextIndex[server] == logIndex = len(leader's log) - 1)
	if len(req.Entries) > 0 {
		rf.nextIndex[server] =
			req.Entries[len(req.Entries)-1].Index + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// Recalculate commitIndex in Leader
	N := rf.log[len(rf.log)-1].Index
	for N > rf.commitIndex {
		count := 0
		for server := range rf.peers {
			if rf.matchIndex[server] >= N && rf.log[N-rf.log[0].Index].Term == rf.currentTerm {
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
	// TODO(namnh) : Should care thundering-herd
	rf.cond.Broadcast()

	// Leader MUST check to send log to follower's node or not. In case a node
	// joins into cluster and just receives heartbeat message, forget this can
	// make this nodes' log is inconsistency with leader's log.
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
	defer rf.cond.L.Unlock()
	defer rf.persist()

	reply.Success = false
	// Init default value for fields used for optimization
	reply.XIndex = -1
	reply.XTerm = -1
	reply.XLen = -1

	if args.Term < rf.currentTerm {
		// if request's term < node's current term. Return false
		// (and node who sent request must be steps down to follower)
		reply.Term = rf.currentTerm
		return
	}

	// reply's term = args's term because args.Term >= rf.currentTerm
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	// Now args's term >= node's currentTerm
	rf.currentTerm = args.Term
	rf.state = Follower
	// Reupdate last time receive heartbeat message
	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()

	// If log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm, return success = false
	prevLogIndexInMem := args.PrevLogIndex - rf.log[0].Index
	if prevLogIndexInMem < 0 {
		return
	}

	if prevLogIndexInMem >= len(rf.log) {
		reply.XLen = rf.log[len(rf.log)-1].Index
		return
	}

	if rf.log[prevLogIndexInMem].Term != args.PrevLogTerm {
		// optimization
		conflictTerm := rf.log[prevLogIndexInMem].Term
		reply.XTerm = conflictTerm
		// Find the first index that stores term of conflicting entry
		for i := 0; i <= prevLogIndexInMem; i++ {
			if rf.log[i].Term == conflictTerm {
				reply.XIndex = i + rf.log[0].Index
				return
			}
		}
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// Append any new entries not already in the log
	numEntries := 0
	for _, entry := range args.Entries {
		logEntryIndex := entry.Index
		logEntryIndexInMem := logEntryIndex - rf.log[0].Index
		logEntryTerm := entry.Term

		if logEntryIndexInMem < len(rf.log) &&
			logEntryTerm == rf.log[logEntryIndexInMem].Term {
			numEntries++
			continue
		}

		if logEntryIndexInMem < len(rf.log) &&
			(logEntryTerm != rf.log[logEntryIndexInMem].Term ||
				logEntryIndexInMem >= len(rf.log)) {
			// delete the existing entry and all that follow it
			rf.log = rf.log[:logEntryIndexInMem]
			// In case condition is hit, all following entry after logEntryIndex was deleted.
			// Then new log entries form leader following logEntryIndex couldn't be found
			// So, we can skip the loop and just append remaining entries to node's log
			break
		}
	}
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[numEntries:]...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}

	// TODO(namnh) : Should care about thundering herd
	rf.cond.Broadcast()

	reply.Success = true
}
