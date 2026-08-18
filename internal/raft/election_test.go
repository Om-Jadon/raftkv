package raft

import (
	"context"
	"testing"
)

func TestVoteRestrictionLogUpToDate(t *testing.T) {
	n := New(DefaultConfig(1, nil), stubTransport{}, RealClock{}, NewMemoryStorage(), NoopStateMachine{})
	n.mu.Lock()
	n.log = []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
	}
	n.currentTerm = 2
	n.mu.Unlock()

	deny, err := n.RequestVote(context.Background(), &RequestVoteArgs{
		Term: 2, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deny.VoteGranted {
		t.Fatal("granted vote to candidate with older log term")
	}

	n.mu.Lock()
	n.votedFor = nil
	n.mu.Unlock()

	short, err := n.RequestVote(context.Background(), &RequestVoteArgs{
		Term: 2, CandidateID: 2, LastLogIndex: 1, LastLogTerm: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if short.VoteGranted {
		t.Fatal("granted vote to candidate with shorter log")
	}

	n.mu.Lock()
	n.votedFor = nil
	n.mu.Unlock()

	ok, err := n.RequestVote(context.Background(), &RequestVoteArgs{
		Term: 2, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok.VoteGranted {
		t.Fatal("denied vote to equally up-to-date candidate")
	}
}
