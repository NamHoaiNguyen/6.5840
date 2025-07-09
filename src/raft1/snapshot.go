package raft

import (
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
func (rf *Raft) SendSnapshotInstall(server int) {
	snapshotInstallReq := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	snapshotInstallRes := &InstallSnapshotReply{}

	ok := rf.peers[server].Call("Raft.SnapshotInstall", snapshotInstallReq, snapshotInstallRes)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()

	if snapshotInstallRes.Term > rf.currentTerm {
		// Leader MUST step down if follower's term > leader's term
		rf.state = Follower
		// Reupdate leader's term
		rf.currentTerm = snapshotInstallRes.Term
		rf.votedFor = -1
		rf.lastHeartbeatTimeRecv = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}

	// Update nextIndex and matchIndex
	rf.matchIndex[server] = snapshotInstallReq.LastIncludedIndex
	rf.nextIndex[server] = snapshotInstallReq.LastIncludedIndex + 1

	rf.persister.Save(rf.encodeState(), snapshotInstallReq.Data)
}

// Follower
func (rf *Raft) SnapshotInstall(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Immediately return if leader's term < node's currentTerm
	if args.Term > rf.currentTerm {
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

	// Now args.LastIncludedIndex > rf.commitIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// Create new snapshot file if first chunk (offset is 0)
	// Write data into snapshot file at given offset
	// Reply and wait for more data chunks if done is false
	if !args.Done {
		return
	}

	firstLogIndex := rf.log[0].Index
	if firstLogIndex <= args.LastIncludedIndex {
		rf.log = append([]LogEntry{}, LogEntry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
	} else if firstLogIndex < args.LastIncludedIndex {
		trimLen := args.LastIncludedIndex - firstLogIndex
		rf.log = append([]LogEntry{}, rf.log[trimLen:]...)
		rf.log[0].Command = nil
	}
	rf.persister.Save(rf.encodeState(), args.Data)
	rf.smsg = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}
