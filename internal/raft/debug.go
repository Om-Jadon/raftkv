package raft

// DebugState is a consistent copy of volatile + log state for invariant checkers.
type DebugState struct {
	ID          NodeID
	Role        Role
	Term        uint64
	Log         []LogEntry
	CommitIndex uint64
	LastApplied uint64
	VotedFor    *NodeID
	Leader      NodeID
}

// DebugState copies node state under the mutex.
func (n *Node) DebugState() DebugState {
	n.mu.Lock()
	defer n.mu.Unlock()
	log := make([]LogEntry, len(n.log))
	for i, e := range n.log {
		log[i] = LogEntry{Term: e.Term, Index: e.Index, Command: append([]byte(nil), e.Command...)}
	}
	var vf *NodeID
	if n.votedFor != nil {
		id := *n.votedFor
		vf = &id
	}
	leader := n.leaderID
	if n.role == Leader {
		leader = n.id
	}
	return DebugState{
		ID:          n.id,
		Role:        n.role,
		Term:        n.currentTerm,
		Log:         log,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		VotedFor:    vf,
		Leader:      leader,
	}
}
