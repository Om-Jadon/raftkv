package raft

// RequestVoteArgs is Figure 2 RequestVote RPC arguments.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is Figure 2 RequestVote RPC results.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is Figure 2 AppendEntries RPC arguments (heartbeats use empty Entries).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is Figure 2 AppendEntries RPC results, plus fast-backup hints.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs is Figure 13 InstallSnapshot RPC arguments.
// Phase 1 defines the type; the handler is a stub until Phase 8.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	Done              bool
}

// InstallSnapshotReply is Figure 13 InstallSnapshot RPC results.
type InstallSnapshotReply struct {
	Term uint64
}
