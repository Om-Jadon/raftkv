package raft

// NoopStateMachine discards applied entries. Replaced by the KV store in Phase 5.
type NoopStateMachine struct{}

// Apply implements StateMachine.
func (NoopStateMachine) Apply(LogEntry) (any, error) { return nil, nil }

// Snapshot implements StateMachine.
func (NoopStateMachine) Snapshot() ([]byte, error) { return nil, nil }

// Restore implements StateMachine.
func (NoopStateMachine) Restore([]byte) error { return nil }
