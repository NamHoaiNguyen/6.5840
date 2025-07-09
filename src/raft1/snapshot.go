package raft

type SnapshotInstallArgs struct {
	Term int // leader's term

	LeaderId int

	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int

	// term of lastIncludedIndex
	LastIncludedTerm int

	// byte offset where chunk is positioned in the
	// snapshot file
	Offset int

	// raw bytes of the snapshot chunk, starting at
	// offset
	Data []byte

	Done bool // true if this is last chunk
}

type SnapshotInstallReply struct {
	Term int // currentTerm, for leader to update itself
}

// Leader
func (rf *Raft) SendSnapshotInstall(server int) {
	var SnapshotInstallReq SnapshotInstallArgs
	var SnapshotInstallRes SnapshotInstallReply

	ok := rf.peers[server].Call("Raft.SnapShotInstall", SnapshotInstallReq, SnapshotInstallRes)
	if !ok {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
}

// Follower
func (rf *Raft) SnapShotInstall() bool {
	return false
}