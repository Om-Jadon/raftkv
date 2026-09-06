package memory

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

type countHandler struct {
	rv atomic.Int32
}

func (h *countHandler) RequestVote(context.Context, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h.rv.Add(1)
	return &raft.RequestVoteReply{}, nil
}

func (h *countHandler) AppendEntries(context.Context, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{}, nil
}

func (h *countHandler) InstallSnapshot(context.Context, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

func TestDupRateDeliversTwice(t *testing.T) {
	n := New(1)
	h := &countHandler{}
	n.Register(2, h)
	n.SetDupRate(1.0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := n.Transport(1).SendRequestVote(ctx, 2, &raft.RequestVoteArgs{Term: 1}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if h.rv.Load() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("dup deliveries=%d, want >= 2", h.rv.Load())
}

func TestAsymmetricBlock(t *testing.T) {
	n := New(1)
	h := &countHandler{}
	n.Register(2, h)
	n.Register(1, h)
	n.Block(1, 2)
	ctx := context.Background()
	if _, err := n.Transport(1).SendRequestVote(ctx, 2, &raft.RequestVoteArgs{}); err != ErrPartitioned {
		t.Fatalf("1->2 err=%v want ErrPartitioned", err)
	}
	if _, err := n.Transport(2).SendRequestVote(ctx, 1, &raft.RequestVoteArgs{}); err != nil {
		t.Fatalf("2->1 err=%v", err)
	}
}
