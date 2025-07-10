package raft

import (
	"fmt"
	"time"

	"6.5840/raftapi"
)

type InstallSnapshotArgs struct {
	Term int // leader's term

	LeaderId int

	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int

	// term of lastIncludedIndex
	LastIncludedTerm int

	// byte offset where chunk is positioned in the
	// snapshot file.
	// As required by lab, Snapshot should be sent entirely
	// in a single InstallSnapshotRPC.
	// In other words, offset should be = 0
	Offset int

	// raw bytes of the snapshot chunk, starting at
	// offset
	Data []byte

	// As required by lab, this should be true
	Done bool // true if this is last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// Leader
func (rf *Raft) SendInstallSnapshot(server int) {
	rf.cond.L.Lock()
	if rf.state != Leader {
		rf.cond.L.Unlock()
		return
	}

	snapshotInstallReq := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		// Offset  always start at zero
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		// as required by lab, snapshot should be sent entirely
		// in a single installsnapshot rpc.
		// In other words, done MUST always be true
		Done:              true,
	}
	rf.cond.L.Unlock()
	snapshotInstallRes := &InstallSnapshotReply{}

	ok := rf.peers[server].Call("Raft.InstallSnapshot", snapshotInstallReq, snapshotInstallRes)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if rf.state != Leader {
		return
	}

	if snapshotInstallRes.Term > rf.currentTerm {
		// Common rule
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = snapshotInstallRes.Term
		rf.votedFor = -1
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}

	// TODO(namnh, 3D) : Recheck
	// Does leader need to recheck args'Lastincluded index with index
	// of 0-th log entry
	// if snapshotInstallReq.LastIncludedIndex != rf.log[0].Index {
	// 	return
	// }

	// Update nextIndex and matchIndex
	// So leader won't send entries before the snapshot
	rf.matchIndex[server] = snapshotInstallReq.LastIncludedIndex
	rf.nextIndex[server] = snapshotInstallReq.LastIncludedIndex + 1

	fmt.Printf("Value of rf.matchIndex[server]: %d and rf.nextIndex[server]: %d at server after install snapshot", rf.matchIndex[server], rf.nextIndex[server])

	rf.persister.Save(rf.encodeState(), snapshotInstallReq.Data)
}

// Follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Immediately return if leader's term < node's currentTerm
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if rf.commitIndex >= args.LastIncludedIndex {
		// Snapshot is stale
		reply.Term = rf.currentTerm
		return
	}

	// SnapshotInstall request should also be treated as a kind of heartbeat at the follower
	// So, follower should update its last time receiving heartbeat
	rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	if !args.Done {
		panic("We don't use offset mechanism")
	}

	// 5. Save snapshot file, discard any existing or partial snapshot
	// with a smaller index
	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)

	// Now args.LastIncludedIndex > rf.commitIndex
	if len(rf.log) > args.LastIncludedIndex &&
		rf.log[args.LastIncludedIndex].Term == args.LastIncludedTerm {
		// 6. If existing log entry has same index and term as snapshot’s
		// last included entry, retain log entries following it and reply
		rf.log = append([]LogEntry{}, rf.log[args.LastIncludedIndex:]...)
		rf.log[0].Command = nil
	} else {
		// 7. Discard the entire log
		rf.log = rf.log[:0]
		rf.log[0] = LogEntry{
			Index: args.LastIncludedIndex, 
			Term: args.LastIncludedTerm,
			Command: nil,
		}
	}

	shouldDiscardEntireLog := true
	logEntryIndexInMem := 0
	for index, entry := range rf.log {
		if entry.Index == args.LastIncludedIndex &&
		   entry.Term == args.LastIncludedTerm {
			shouldDiscardEntireLog = false
			logEntryIndexInMem = index
		}
	}

	if shouldDiscardEntireLog {
		// 7. Discard the entire log
		rf.log = rf.log[:0]
		// Use snapshot as dummy entry
		rf.log[0] = LogEntry{
			Index: args.LastIncludedIndex, 
			Term: args.LastIncludedTerm,
			Command: nil,
		}
	} else {
		// 6. If existing log entry has same index and term as snapshot’s
		// last included entry, retain log entries following it and reply
		rf.log = append([]LogEntry{}, rf.log[logEntryIndexInMem:]...)
		rf.log[0].Command = nil
	}

	// Update commitIndex and lastApplied
	// TODO(namnh, 3D) : Recheck this one
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// Persist the snapshot
	rf.persister.Save(rf.encodeState(), args.Data)

	// Reset state machine using snapshot contents
	rf.snapshotMsg = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot     : args.Data,
		SnapshotTerm : args.LastIncludedTerm,
		SnapshotIndex : args.LastIncludedIndex,
	}
}
