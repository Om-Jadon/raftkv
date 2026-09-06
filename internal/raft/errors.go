package raft

import "errors"

var (
	// ErrNotLeader is returned when Propose is called on a non-leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrLostLeadership is returned when a proposed index is overwritten or the leader steps down.
	ErrLostLeadership = errors.New("raft: lost leadership")
	// ErrTimeout is returned when waiting for an apply exceeds the deadline.
	ErrTimeout = errors.New("raft: apply timeout")
	// ErrStopped is returned when the node has been stopped.
	ErrStopped = errors.New("raft: stopped")
)
