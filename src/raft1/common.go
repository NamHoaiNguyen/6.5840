package raft

import (
	"math/rand"
	"time"
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
