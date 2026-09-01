package raft

import (
	"context"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type stubTransport struct{}

func (stubTransport) SendRequestVote(context.Context, NodeID, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, nil
}
func (stubTransport) SendAppendEntries(context.Context, NodeID, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, nil
}
func (stubTransport) SendInstallSnapshot(context.Context, NodeID, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, nil
}

func TestStopIdempotentNoLeak(t *testing.T) {
	n := New(DefaultConfig(1, nil), stubTransport{}, RealClock{}, NewMemoryStorage(), NoopStateMachine{})
	n.Run()
	n.Stop()
	n.Stop()
	if n.Role() != Follower {
		t.Fatalf("role = %v, want Follower", n.Role())
	}
}

func TestSentinelLogIndex(t *testing.T) {
	n := New(DefaultConfig(1, nil), stubTransport{}, RealClock{}, NewMemoryStorage(), NoopStateMachine{})
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIndex() != 0 {
		t.Fatalf("lastIndex = %d, want 0", n.lastIndex())
	}
	if n.firstIndex() != 0 {
		t.Fatalf("firstIndex = %d, want 0", n.firstIndex())
	}
	term, ok := n.termAt(0)
	if !ok || term != 0 {
		t.Fatalf("termAt(0) = %d, %v; want 0, true", term, ok)
	}
}
