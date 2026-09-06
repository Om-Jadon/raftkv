package chaos

import (
	"context"
	"strings"
	"testing"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func TestSkipVotePersistAllowsDoubleVote(t *testing.T) {
	raft.SkipVotePersist.Store(true)
	defer raft.SkipVotePersist.Store(false)

	st := raft.NewMemoryStorage()
	n1 := raft.New(raft.DefaultConfig(1, nil), nil, raft.RealClock{}, st, raft.NoopStateMachine{})
	first, err := n1.RequestVote(context.Background(), &raft.RequestVoteArgs{
		Term: 5, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.VoteGranted {
		t.Fatal("first vote should be granted")
	}

	n2 := raft.New(raft.DefaultConfig(1, nil), nil, raft.RealClock{}, st, raft.NoopStateMachine{})
	second, err := n2.RequestVote(context.Background(), &raft.RequestVoteArgs{
		Term: 5, CandidateID: 3, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second.VoteGranted {
		t.Fatal("SkipVotePersist should allow a second grant in the same term after restart")
	}
}

func TestVotePersistDeniesDoubleVote(t *testing.T) {
	st := raft.NewMemoryStorage()
	n1 := raft.New(raft.DefaultConfig(1, nil), nil, raft.RealClock{}, st, raft.NoopStateMachine{})
	first, err := n1.RequestVote(context.Background(), &raft.RequestVoteArgs{
		Term: 5, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.VoteGranted {
		t.Fatal("first vote should be granted")
	}

	n2 := raft.New(raft.DefaultConfig(1, nil), nil, raft.RealClock{}, st, raft.NoopStateMachine{})
	second, err := n2.RequestVote(context.Background(), &raft.RequestVoteArgs{
		Term: 5, CandidateID: 3, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.VoteGranted {
		t.Fatal("persisted votedFor must reject a second candidate in the same term")
	}
}

func TestBrokenFigure8CommitRule(t *testing.T) {
	termAt := func(index uint64) (uint64, bool) {
		if index == 1 {
			return 1, true
		}
		return 0, false
	}
	replicas := func(uint64) int { return 3 }

	got := raft.HighestCommittable(1, 0, 2, termAt, replicas, 3, true)
	if got != 0 {
		t.Fatalf("correct Raft must not commit old-term index: got %d", got)
	}
	got = raft.HighestCommittable(1, 0, 2, termAt, replicas, 3, false)
	if got != 1 {
		t.Fatalf("SkipFigure8 must commit old-term index by replica count: got %d", got)
	}
}

func TestCheckerCatchesFigure8Symptom(t *testing.T) {
	chk := newChecker()
	old := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("A")}}
	if err := chk.observeCommits([]raft.DebugState{{ID: 1, CommitIndex: 1, Log: old}}); err != nil {
		t.Fatal(err)
	}
	later := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 2, Command: []byte("B")}}
	err := chk.check([]raft.DebugState{{ID: 2, Role: raft.Leader, Term: 3, Log: later, CommitIndex: 0}})
	if err == nil {
		t.Fatal("expected completeness/durability to fire when a later leader is missing a committed entry")
	}
	if !strings.Contains(err.Error(), "completeness") && !strings.Contains(err.Error(), "durability") {
		t.Fatalf("got %v, want completeness or durability", err)
	}
}
