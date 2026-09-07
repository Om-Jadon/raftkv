package raft

import "time"

// Config is per-node Raft configuration. Every node must get its own RandSeed
// (or a distinct ID-derived seed) so election timeouts do not correlate.
type Config struct {
	ID                 NodeID
	Peers              []NodeID
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
	RandSeed           int64
	// DataDir is the durable storage directory for this node (Phase 4+).
	// Empty means the caller supplies an in-memory or other Storage.
	DataDir string
	// SnapshotThresholdBytes triggers log compaction after apply when the
	// in-memory log exceeds this size. Zero disables snapshots.
	SnapshotThresholdBytes uint64
}

// DefaultConfig returns lab-style timeouts: election 150–300ms, heartbeat 50ms.
func DefaultConfig(id NodeID, peers []NodeID) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		TickInterval:       10 * time.Millisecond,
		RandSeed:           int64(id) * 1_000_003,
	}
}

// WANConfig returns timeouts suited to a real TCP/gRPC network.
func WANConfig(id NodeID, peers []NodeID) Config {
	c := DefaultConfig(id, peers)
	c.ElectionTimeoutMin = 300 * time.Millisecond
	c.ElectionTimeoutMax = 600 * time.Millisecond
	c.HeartbeatInterval = 100 * time.Millisecond
	c.TickInterval = 20 * time.Millisecond
	return c
}
