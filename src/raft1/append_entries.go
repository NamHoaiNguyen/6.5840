package raft

import (
	"fmt"
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
			fmt.Printf("Namnh UpdateStateMachineLog Node: %d is :%d state\n", rf.me, rf.state)
			fmt.Printf("namnh check rf.commitIndex: %d and rf.lastApplied before applied: %d!!!\n", rf.commitIndex, rf.lastApplied)

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

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			// Leader doesn't be blocking to receive response from followers
			go rf.LeaderHandleAppendEntriesResponse(i, true /*isHearbeat*/) // heartbeat message doesn't need to retry
		}

		// Sleep for heartbeatInterval(ms)
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) isReplicationNeeded(server int) bool {
	// rf.cond.L.Lock()
	// defer rf.cond.L.Unlock()
	isReplicationNeeded := (rf.state == Leader && rf.log[len(rf.log)-1].Index >= rf.nextIndex[server])
	// fmt.Printf("Value of isReplicationNeeded: %t\n", isReplicationNeeded)
	return isReplicationNeeded
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

		rf.LeaderHandleAppendEntriesResponse(server, false /*isHeartbeat*/)
	}
}

// Leader execute append entry requests and handle response
func (rf *Raft) LeaderHandleAppendEntriesResponse(server int, isHeartbeat bool) {
	rf.cond.L.Lock()
	// fmt.Printf("LeaderHandleAppendEntriesResponse NODE: %d TRY TO SEND APPEND ENTRY MESSAGE: %t\n!!!", rf.me, isHeartbeat)

	if server == rf.me {
		panic("Debug: Leader doesn't send append entry to itself!!!")
	}

	if rf.state != Leader {
		// There is a case that before a leader sending append entry request to other node,
		// something happens that cause this node isn't leader anymore
		rf.cond.L.Unlock()
		return
	}

	// fmt.Printf("Before Value of isRetry: %t and rf.nextIndex[server]: %d\n", isRetry, rf.nextIndex[server])

	// if isRetry {
	// 	// If leader and node's log mitmatch, decrease nextIndex
	// 	rf.nextIndex[server]--
	// 	// fmt.Printf("After Value of isRetry: %t and rf.nextIndex[server]: %d\n", isRetry, rf.nextIndex[server])
	// }

	if len(rf.log)-1 < rf.nextIndex[server] && !isHeartbeat {
		fmt.Println("Shouldn't send append entry message anymore!!!")
		rf.cond.L.Unlock()
		return
	}

	if rf.nextIndex[server] < 1 {
		fmt.Printf("THERE IS BAD. rf.nextIndex[server]: %d and server: %d\n", rf.nextIndex[server], server)
		panic("nextIndex of a server MUST >= 1")
	}

	prevLogIndex := 0
	if rf.nextIndex[server] >= 1 {
		// To avoid out of index
		prevLogIndex = rf.nextIndex[server] - 1
	}

	if prevLogIndex < 0 || prevLogIndex >= len(rf.log) {
		panic("There is no way that a follower can deny a log with index = 0 or > len of log!!!")
	}

	var entriesList []LogEntry
	// TODO(namnh, 3B, VERY VERY IMPORANT): tHIS CAN BE THE KEY
	// if !isHeartbeat || prevLogIndex == 0 {
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

	if isHeartbeat {
		fmt.Println("Value of matchIndex and nextIndex at LEADER NODE", rf.matchIndex, rf.nextIndex)
	}

	appendEntryRes := &RequestAppendEntriesReply{}

	rf.cond.L.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", appendEntryReq, appendEntryRes)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	// defer rf.cond.L.Unlock()

	if appendEntryRes.Term > rf.currentTerm {
		fmt.Printf("Leader: %d should step down. appendEntryRes.Term: %d, current term of leader: %d. Node: %d who sent that message make me step down!!!\n", rf.me, appendEntryRes.Term, rf.currentTerm, server)
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = appendEntryRes.Term
		rf.votedFor = -1
		rf.cond.L.Unlock()
		return
	}

	if appendEntryRes.Term != rf.currentTerm || rf.state != Leader {
		fmt.Printf("I AM NOT MYSELF ANYMORE: %d. MY STATE IS : %d. My term after: %d and before: %d!!!", rf.me, rf.state, appendEntryRes.Term, rf.currentTerm)
		// If leader isn't leader anymore
		// or leader's term isn't the same as before
		rf.cond.L.Unlock()
		return
	}

	// Now leader 's term >= node's term
	if !appendEntryRes.Success {
		// There is inconsistency between leader's log and follower'log
		// Leader decrease its nextIndex corressponding with follower.
		rf.nextIndex[server]--
		fmt.Printf("LEADER:%d MUST RESEND PREV LOG MESSAGE: %d to server: %d\n", rf.me, rf.nextIndex[server], server)
		// TODO(namnh, 3B) : This logic is wrong. Only send to specific node.
		// Resend log until log between server and follower match.
		// go rf.LeaderHandleAppendEntriesResponse(server, false /*isHearbeat*/)
		// rf.cond.L.Unlock()
		// rf.LeaderHandleAppendEntriesResponse(server, false /*isHearbeat*/)
		rf.peerCond[server].Signal()
		rf.cond.L.Unlock()
		return
	}

	// // appendEntryRes.Success == true and args
	// if (rf.matchIndex[server] == 0) {
	// 	rf.cond.L.Unlock()
	// 	rf.LeaderHandleAppendEntriesResponse(server, false /*isHearbeat*/)
	// 	return
	// }

	if rf.nextIndex[server] > len(rf.log) {
		panic("This is an implementation error!!!")
	}

	// Incase appendEntryRes.Success == true
	// Update nextIndex(because we always send get to the end of leader's log
	// to send. In other words, rf.nextIndex[server] == logIndex = len(leader's log) - 1)
	rf.matchIndex[server] = prevLogIndex + len(entriesList)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	if rf.matchIndex[server] >= len(rf.log) {
		panic("MATCH INDEX CAN'T BE LARGER OR EQUAL LENGTH OF LOG")
	}

	// // TODO(namnh, 3B) : Recheck this logic?
	// // If a log entry is replicate to majority of nodes, update commitIndex
	// for index, value := range rf.matchIndex {
	// 	if value > len(rf.log)-1 {
	// 		fmt.Printf("Node: %d has problem with matchIndex at: %d index.\n", index, value)
	// 		panic("Error!!!!")
	// 	}

	// 	// if index == rf.me {
	// 	// 	fmt.Printf("Current node: %d is leader: %t!!!\n", rf.me, rf.state == Leader)
	// 	// 	continue
	// 	// }

	// 	if _, exists := rf.collectLog[rf.matchIndex[index]]; !exists {
	// 		rf.collectLog[rf.matchIndex[index]] = 1
	// 	} else {
	// 		rf.collectLog[rf.matchIndex[index]]++
	// 	}
	// }

	// // fmt.Println("Namnh check value of map", rf.collectLog)
	// // fmt.Println("Namnh check log of leader", rf.log)

	// // Check that if there is any value is replicate to majority of nodes
	// for key, value := range rf.collectLog {
	// 	if rf.log[key].Term == rf.currentTerm && value > len(rf.peers)/2 {
	// 		// Update commitIndex
	// 		rf.commitIndex = key
	// 		break
	// 	}
	// }

	// // Need to clear.
	// clear(rf.collectLog)

	fmt.Println("mathindex and nextinder at SERVER", rf.matchIndex, rf.nextIndex)
	fmt.Printf("CURRENT TERM OF LEADER AFTER RECEIVING APPEND ENTRY RESPONSE: %d\n", rf.currentTerm)

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

	// fmt.Printf("Value of rf.commitIndex BEFORE updated: %d at leader: %d\n", rf.commitIndex, rf.me)

	rf.commitIndex = N
	rf.cond.Broadcast()

	// for index, log := range rf.log {
	// 	count := 0
	// 	for peer := range rf.peers {
	// 		if rf.matchIndex[peer] >= index {
	// 			count++
	// 		}
	// 	}
	// 	// If there exists an N such that N > commitIndex, a majority
	// 	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// 	// set commitIndex = N (§5.3, §5.4).
	// 	if count > len(rf.peers)/2 && index > rf.commitIndex && log.Term == rf.currentTerm {
	// 		rf.commitIndex = index
	// 	}
	// }

	fmt.Printf("Value of rf.commitIndex AFTER updated: %d at leader: %d\n", rf.commitIndex, rf.me)
	// rf.cond.L.Unlock()
	// TODO(namnh, 3B) : Signal to update state machine to flush

	// TODO(namnh, 3B, VERY VERY IMPORTANT)
	// if prevLogIndex == 0 {
	if rf.isReplicationNeeded(server) {
		rf.peerCond[server].Signal()
		rf.cond.L.Unlock()
		// rf.LeaderHandleAppendEntriesResponse(server, false /*isHearbeat*/)

		return
	}

	rf.cond.L.Unlock()
}

// RECEIVER
// If node receives append entries(including hearbeat) message
func (rf *Raft) AppendEntries(
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {
	// If is heartbeat message
	rf.cond.L.Lock()

	// if len(args.Entries) > 0 {
	// 	fmt.Printf("HandleAppendEntryLog WILL called in node: %d and log length: %d\n", rf.me, len(args.Entries))
	// 	fmt.Printf("Value of prevLogIndex: %d and leaderCommit: %d\n", args.PrevLogIndex, args.LeaderCommit)

	// 	for _, val := range args.Entries {
	// 		fmt.Printf("Value of Term: %d and index: %d\n", val.Term, val.Index)
	// 		fmt.Println("Value of command", val.Command)
	// 	}
	// }

	if args.Term < rf.currentTerm {
		// if request's term < node's current term. Return false
		// (and node who sent request must be steps down to follower)
		fmt.Printf("Term condition less than is hit!!! args.Term: %d and rf.currentTerm: %d\n", args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm

		rf.cond.L.Unlock()
		return
	}

	// TODO(namnh, 3B) : Check this one
	// if args.PrevLogIndex == 0 {
	// 	rf.HandleAppendEntryLog(args, reply)
	// 	rf.cond.L.Unlock()
	// 	return
	// }

	reply.Term = args.Term

	// if (len(rf.log) <= args.PrevLogIndex) ||
	// 	 (len(rf.log) > args.PrevLogIndex &&
	// 	      rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)  ||
	// 	 // TODO(namnh, 3B, VERY IMPORTANT) : This condition can be the key to fix error.
	// 	 (args.PrevLogIndex == 0 && args.PrevLogTerm == 0) {
	if (len(rf.log) <= args.PrevLogIndex) ||
		(len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// If log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm, return success = false
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			fmt.Printf("PrevLogIndex: %d condition less than is hit. rf.log[args.PrevLogIndex].Term: %d and args.PrevLogTerm: %d!!!\n", args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			fmt.Printf("I AM : %d node and i reject you: %d!!!\n", rf.me, args.LeaderId)
		}
		reply.Success = false
		rf.cond.L.Unlock()
		return
	}

	// Now args's term >= node's currentTerm
	rf.currentTerm = args.Term

	// TODO(namnh, 3B) : Recheck this condition/
	// Except 2 above conditons, success = true
	// reply.Success = true

	// TODO(namnh, 3B) : Recheck this one
	rf.state = Follower
	rf.votedFor = -1

	if args.Entries == nil {
		// Heartbeat message
		// Reupdate last time receive heartbeat message
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		if rf.state == Candidate {
			// If nodes's current state is candidate, send message to stop electing process
			// NOTE: In case that an outdated leader rejoin cluster, no need to send anything.
			// Because in this case, when outdated leader tries to send heartbeat/append entry
			// messages, other followers detects this leader's term < their terms then reply
			// their terms. Outdated leader, based on follower's response will step down to follower.
			fmt.Printf("namnh check node: %d receive heartbeat when trying to elect at :%d state\n", rf.me, rf.state)

		}

		// TODO(namnh, 3B, IMPORTANT) : THIS CORRECT OF LOGIC IS VERY IMPORTANT.
		if args.LeaderCommit > rf.commitIndex {
			fmt.Printf("Follower : %d receive leaderCommit: %d > its commitIndex: %d. Its lastApplied: %d\n", rf.me, args.LeaderCommit, rf.commitIndex, rf.lastApplied)
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
			// rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			rf.cond.Broadcast()
		}
		fmt.Printf("Follower : %d receive leaderCommit: %d > its commitIndex: %d. Its lastApplied: %d OUTSIDE IF CONDITION\n", rf.me, args.LeaderCommit, rf.commitIndex, rf.lastApplied)

		fmt.Printf("Value of rf.commitIndex AFTER updated: %d at follower: %d when RECEIVING HEARTBEAT!!!\n", rf.commitIndex, rf.me)

		reply.Success = true
		rf.cond.L.Unlock()
		return
	}

	rf.cond.L.Unlock()
	// Split handling append entry message to a seperate goroutine
	// TODO(namnh, 3B, IMPORTANT) : CHECK THIS CAREFULLY!!!
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

	// TODO(namnh, 3B) : Check this logic
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
			fmt.Printf("Namnh check value of logEntryIndex: %d and logEntryTerm: %d\n", logEntryIndex, logEntryTerm)
			// delete the existing entry and all that follow it
			rf.log = rf.log[:logEntryIndex]
			// In case condition is hit, all following entry after logEntryIndex was deleted.
			// Then new log entries form leader following logEntryIndex couldn't be found
			// So, we can skip the loop and just append remaining entries to node's log
			break
		}
	}

	listEntries := args.Entries
	rf.log = append(rf.log, listEntries[numEntries:]...)

	// if (numEntries < len(args.Entries)) {
	// rf.log = append(rf.log, args.Entries[numEntries:]...)
	fmt.Printf("Last log index :%d of Log of node: %d after be appended\n", len(rf.log)-1, rf.me)
	// fmt.Println("LOG!!!", rf.log)
	// }

	// rf.log = append(rf.log, args.Entries...)

	// if len(args.Entries) > 0 {
	// 	// fmt.Println("This is append entry message from leader!!!Log valueof FOLLOWER after be added!!!", rf.log)
	fmt.Printf("namnh check log follower: %d after be appended!!!\n", rf.me)
	fmt.Println("namnh log after be appended!!!\n", rf.log)

	fmt.Printf("Follower : %d receive leaderCommit: %d and its commitIndex: %d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	// }

	if args.LeaderCommit > rf.commitIndex {
		fmt.Printf("WHEN APPLY LOG ENTRY Follower : %d receive leaderCommit: %d > its commitIndex: %d and its last index of log length: %d WHEN APPLY LOG ENTRY\n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.log)-1)
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.cond.Broadcast()
		// Because follower only need to signal instead of broadcast
		// rf.cond.Signal()
	}

	fmt.Printf("Value of rf.commitIndex AFTER updated: %d at follower: %d when RECEIVING APPEND LOG MESSAGE!!!\n", rf.commitIndex, rf.me)

	// TODO(namnh, 3B, IMPORTANT) : CHECK THIS CAREFULLY!!!
	// Maybe the reason is above logic wrong
	reply.Success = true
}
