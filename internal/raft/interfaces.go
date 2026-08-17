package raft

import (
	"context"
	"time"
)

// Transport sends Raft RPCs to a peer. Implementations must not be called
// while holding Node.mu — copy the args, release the lock, then send.
type Transport interface {
	SendRequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler is the receiving side of Transport. Node implements it.
type RPCHandler interface {
	RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Clock abstracts time so tests can later swap in a fake without rewriting Raft.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker is a cancellable periodic timer.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// Storage persists hard state, log entries, and snapshots.
type Storage interface {
	SaveHardState(term uint64, votedFor *NodeID) error
	AppendEntries(entries []LogEntry) error
	TruncateFrom(index uint64) error
	Load() (HardState, []LogEntry, error)
	SaveSnapshot(snap Snapshot) error
	LoadSnapshot() (Snapshot, bool, error)
}

// StateMachine applies committed log entries. Snapshot/Restore are stubs until
// the KV layer and log compaction land.
type StateMachine interface {
	Apply(entry LogEntry) (result any, err error)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
