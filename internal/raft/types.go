package raft

// NodeID identifies a Raft peer. IDs are stable for the life of a cluster.
type NodeID uint64

// Role is the Raft state machine role.
type Role int

// Raft peer roles.
const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single Raft log entry. Index 0 is reserved for a sentinel.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// ApplyMsg is delivered to the state machine once an entry is committed.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
	Result  any
}

// ApplyResult is returned to a Propose waiter after the entry is applied.
type ApplyResult struct {
	Index  uint64
	Term   uint64
	Result any
	Err    error
}

// Status is a snapshot of volatile node state for /status.
type Status struct {
	ID          NodeID
	Role        Role
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LogSize     uint64
	Leader      NodeID
}

// HardState is the persistent Raft metadata (term and vote).
type HardState struct {
	CurrentTerm uint64
	VotedFor    *NodeID
}

// Snapshot is a compacted prefix of the log plus state-machine bytes.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}
