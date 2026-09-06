package chaos

import (
	"testing"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func TestCheckerElectionSafety(t *testing.T) {
	chk := newChecker()
	err := chk.check([]raft.DebugState{
		{ID: 1, Role: raft.Leader, Term: 3, Log: sent()},
		{ID: 2, Role: raft.Leader, Term: 3, Log: sent()},
	})
	if err == nil {
		t.Fatal("expected election safety violation")
	}
}

func TestCheckerLogMatching(t *testing.T) {
	chk := newChecker()
	a := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("x")}, {Index: 2, Term: 1, Command: []byte("y")}}
	b := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("x")}, {Index: 2, Term: 2, Command: []byte("z")}}
	if err := chk.logMatching([]raft.DebugState{{ID: 1, Log: a}, {ID: 2, Log: b}}); err != nil {
		t.Fatalf("logs that only diverge at a newer term should still match the shared prefix: %v", err)
	}
	b[1].Command = []byte("nope")
	if err := chk.logMatching([]raft.DebugState{{ID: 1, Log: a}, {ID: 2, Log: b}}); err == nil {
		t.Fatal("expected log matching violation")
	}
}

func TestCheckerLeaderCompleteness(t *testing.T) {
	chk := newChecker()
	committed := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("A")}}
	if err := chk.observeCommits([]raft.DebugState{{ID: 1, CommitIndex: 1, Log: committed}}); err != nil {
		t.Fatal(err)
	}
	leader := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 2, Command: []byte("B")}}
	if err := chk.leaderCompleteness([]raft.DebugState{{ID: 2, Role: raft.Leader, Log: leader}}); err == nil {
		t.Fatal("expected leader completeness violation")
	}
}

func TestCheckerDurability(t *testing.T) {
	chk := newChecker()
	a := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("A")}}
	if err := chk.observeCommits([]raft.DebugState{{ID: 1, CommitIndex: 1, Log: a}}); err != nil {
		t.Fatal(err)
	}
	b := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("A")}}
	b[1].Command = []byte("mutated")
	if err := chk.observeCommits([]raft.DebugState{{ID: 2, CommitIndex: 1, Log: b}}); err == nil {
		t.Fatal("expected durability violation")
	}
}

func TestCheckerStateMachineSafety(t *testing.T) {
	chk := newChecker()
	a := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("A")}}
	b := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("B")}}
	if err := chk.stateMachineSafety([]raft.DebugState{
		{ID: 1, LastApplied: 1, Log: a},
		{ID: 2, LastApplied: 1, Log: b},
	}); err == nil {
		t.Fatal("expected state machine safety violation")
	}
}

func TestCheckerLeaderAppendOnly(t *testing.T) {
	chk := newChecker()
	first := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 4, Command: []byte("a")}}
	if err := chk.leaderAppendOnly([]raft.DebugState{{ID: 1, Role: raft.Leader, Term: 4, Log: first}}); err != nil {
		t.Fatal(err)
	}
	shrunk := []raft.LogEntry{{Index: 0}}
	if err := chk.leaderAppendOnly([]raft.DebugState{{ID: 1, Role: raft.Leader, Term: 4, Log: shrunk}}); err == nil {
		t.Fatal("expected append-only violation")
	}
}

func sent() []raft.LogEntry {
	return []raft.LogEntry{{Index: 0, Term: 0}}
}

func TestLogsMatchSelf(t *testing.T) {
	a := []raft.LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("x")}}
	if !raft.LogsMatch(a, a) {
		t.Fatal("log should match itself")
	}
}
