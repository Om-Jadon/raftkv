package harness

import (
	"errors"
	"testing"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/transport/memory"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestClusterStartsAndStops(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	if got := c.Len(); got != 3 {
		t.Fatalf("cluster size = %d, want 3", got)
	}
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		n := c.Node(id)
		if n.Role() != raft.Follower {
			t.Fatalf("node %d role = %v, want Follower", id, n.Role())
		}
	}
}

func TestTransportDeliversRPC(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	args := &raft.RequestVoteArgs{Term: 1, CandidateID: 1}
	reply, err := c.SendRequestVote(1, 2, args)
	if err != nil {
		t.Fatalf("SendRequestVote: %v", err)
	}
	if reply == nil {
		t.Fatal("expected reply")
	}
	if got := c.Node(2).RequestVoteCount(); got != 1 {
		t.Fatalf("node 2 RequestVote count = %d, want 1", got)
	}

	c.SetDropRate(1.0)
	_, err = c.SendRequestVote(1, 3, args)
	if !errors.Is(err, memory.ErrDropped) {
		t.Fatalf("dropRate=1.0 error = %v, want ErrDropped", err)
	}
	if got := c.Node(3).RequestVoteCount(); got != 0 {
		t.Fatalf("node 3 RequestVote count = %d, want 0 (dropped)", got)
	}
}

func TestPartitionIsolatesNode(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	c.Disconnect(3)

	args := &raft.RequestVoteArgs{Term: 1, CandidateID: 1}
	_, err := c.SendRequestVote(1, 3, args)
	if !errors.Is(err, memory.ErrPartitioned) {
		t.Fatalf("isolated dest error = %v, want ErrPartitioned", err)
	}
	if got := c.Node(3).RequestVoteCount(); got != 0 {
		t.Fatalf("isolated node received RPC: count = %d", got)
	}

	_, err = c.SendRequestVote(3, 1, args)
	if !errors.Is(err, memory.ErrPartitioned) {
		t.Fatalf("isolated src error = %v, want ErrPartitioned", err)
	}
	if got := c.Node(1).RequestVoteCount(); got != 0 {
		t.Fatalf("node 1 received RPC from isolated peer: count = %d", got)
	}

	_, err = c.SendRequestVote(1, 2, args)
	if err != nil {
		t.Fatalf("unrelated pair should still connect: %v", err)
	}
	if got := c.Node(2).RequestVoteCount(); got != 1 {
		t.Fatalf("node 2 RequestVote count = %d, want 1", got)
	}

	c.Reconnect(3)
	_, err = c.SendRequestVote(1, 3, args)
	if err != nil {
		t.Fatalf("after reconnect: %v", err)
	}
	if got := c.Node(3).RequestVoteCount(); got != 1 {
		t.Fatalf("node 3 RequestVote count = %d after reconnect, want 1", got)
	}
}
