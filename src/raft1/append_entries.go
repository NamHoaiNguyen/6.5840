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
func (rf *Raft) UpdateStateMachineLog() {
	for !rf.killed() {
		rf.cond.L.Lock()

		// fmt.Printf("Namnh UpdateStateMachineLog Node: %d is :%d state and commitIndex: %d and lastApplied: %d\n", rf.me, rf.state, rf.commitIndex, rf.lastApplied)

		for rf.commitIndex > rf.lastApplied {
			// fmt.Printf("Namnh UpdateStateMachineLog Node: %d is :%d state\n", rf.me, rf.state)

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

		time.Sleep(100 * time.Millisecond)
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
func (rf *Raft) SendAppendEntries() {
	// for !rf.killed() {
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

		go rf.LeaderHandleAppendEntriesResponse(i, false /*isHeartbeat*/, false /*isRetry*/)
	}
	// }
}

// Goroutine
func (rf *Raft) RetrySendAppendEntries() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.state != Leader {
			rf.cond.Wait()
		}
		rf.cond.L.Unlock()

		select {
		case server := <-rf.retryCh:
			go rf.LeaderHandleAppendEntriesResponse(server, false /*isHeartbeat*/, false /*isRetry*/)
		}
	}
}

// Leader execute append entry requests and handle response
func (rf *Raft) LeaderHandleAppendEntriesResponse(server int, isHeartbeat bool, isRetry bool) {
	rf.cond.L.Lock()

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

	if isRetry {
		// If leader and node's log mitmatch, decrease nextIndex
		rf.nextIndex[server]--
		// fmt.Printf("After Value of isRetry: %t and rf.nextIndex[server]: %d\n", isRetry, rf.nextIndex[server])
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
	rf.cond.L.Unlock()

	appendEntryRes := &RequestAppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", appendEntryReq, appendEntryRes)
	if !ok {
		// TODO(namnh, 3B) : This logic is wrong? How can i know which one should i reply sending ?
		// Resend append entry request if false
		// In this case, isRetry shouldbe = false
		// because nextIndex shouldn't be updated
		if !isHeartbeat {
			// fmt.Printf("There is problem when sending append entry message to node: %d\n", server)
			// go rf.SendAppendEntries(false /*isRetry*/)
			rf.retryCh <- server
		}
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if appendEntryRes.Term != rf.currentTerm || rf.state != Leader {
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
		// TODO(namnh, 3B) : This logic is wrong. Only send to specific node.
		// Resend log until log between server and follower match.
		// go rf.SendAppendEntries(false /*isRetry*/)
		go rf.LeaderHandleAppendEntriesResponse(server, false /*isHearbeat*/, true /*isRetry*/)

		return
	}

	// Incase appendEntryRes.Success == true
	// Update nextIndex(because we always send get to the end of leader's log
	// to send. In other words, rf.nextIndex[server] == logIndex = len(leader's log) - 1)
	if rf.nextIndex[server] > len(rf.log) {
		panic("This is an implementation error!!!")
	}

	rf.matchIndex[server] = prevLogIndex + len(entriesList)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

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

	N := len(rf.log) - 1
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

	rf.commitIndex = N

	// TODO(namnh, 3B) : Signal to update state machine to flush
}

// RECEIVER
// If node receives append entries(including hearbeat) message
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// If is heartbeat message
	rf.cond.L.Lock()

	if len(args.Entries) > 0 {
		fmt.Printf("HandleAppendEntryLog WILL called in node: %d and log length: %d 6 TIMES\n", rf.me, len(args.Entries))
		fmt.Printf("Value of prevLogIndex: %d and leaderCommit: %d\n", args.PrevLogIndex, args.LeaderCommit)

		for _, val := range args.Entries {
			fmt.Printf("Value of Term: %d and index: %d\n", val.Term, val.Index)
			fmt.Println("Value of command", val.Command)
		}
	}

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

	if args.LeaderCommit > rf.commitIndex {
		// fmt.Printf("Follower : %d receive leaderCommit: %d > its commitIndex: %d and index of log \n", rf.me, args.LeaderCommit, rf.commitIndex)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

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

	if len(args.Entries) > 0 {
		// fmt.Println("This is append entry message from leader!!!Log valueof FOLLOWER after be added!!!", rf.log)
		// fmt.Printf("namnh check log follower: %d after be appended!!!\n", rf.me)
		// fmt.Println("namnh log after be appended!!!\n", rf.log)

		// fmt.Printf("Follower : %d receive leaderCommit: %d and its commitIndex: %d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	}

	// if args.LeaderCommit > rf.commitIndex {
	// 	fmt.Printf("Follower : %d receive leaderCommit: %d > its commitIndex: %d and index of log \n", rf.me, args.LeaderCommit, rf.commitIndex)
	// 	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	// }

	rf.cond.L.Unlock()

	// if len(args.Entries) > 0 {
	// 	fmt.Printf("HandleAppendEntryLog WILL called in node: %d and log length: %d\n", rf.me, len(args.Entries))
	// }

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

	// if len(args.Entries) > 0 {
	// 	// fmt.Println("This is append entry message from leader!!!Log valueof FOLLOWER after be added!!!", rf.log)
	// 	fmt.Printf("namnh check log follower: %d after be appended!!!\n", rf.me)
	// 	fmt.Println("namnh log after be appended!!!\n", rf.log)

	// 	fmt.Printf("Follower : %d receive leaderCommit: %d and its commitIndex: %d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	// }

	// if args.LeaderCommit > rf.commitIndex {
	// 	// fmt.Printf("Follower : %d receive leaderCommit: %d > its commitIndex: %d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	// 	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	// }

	// TODO(namnh, 3B, IMPORTANT) : CHECK THIS CAREFULLY!!!
	// Maybe the reason is above logic wrong
	reply.Success = true
}
