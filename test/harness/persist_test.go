package harness

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/storage"
)

func TestPersistBasic(t *testing.T) {
	c := NewPersistentCluster(t, 3)
	c.CheckOneLeader()

	var committed [][]byte
	for i := 1; i <= 5; i++ {
		cmd := []byte(fmt.Sprintf("p-%d", i))
		_, index, _ := c.Start(cmd)
		c.WaitCommit(index)
		c.WaitApplied(index)
		committed = append(committed, cmd)
	}

	for id := raft.NodeID(1); id <= 3; id++ {
		c.Crash(id)
	}
	for id := raft.NodeID(1); id <= 3; id++ {
		c.Restart(id)
	}
	c.CheckOneLeader()

	_, index, _ := c.Start([]byte("after-restart"))
	c.WaitCommit(index)
	c.WaitApplied(index)

	for id := raft.NodeID(1); id <= 3; id++ {
		applied := c.Applied(id)
		if len(applied) < len(committed)+1 {
			t.Fatalf("node %d applied %d, want >= %d", id, len(applied), len(committed)+1)
		}
		for i, want := range committed {
			if string(applied[i]) != string(want) {
				t.Fatalf("node %d applied[%d]=%q want %q", id, i, applied[i], want)
			}
		}
	}
	c.CheckAppliedSame()
}

func TestPersistPartition(t *testing.T) {
	c := NewPersistentCluster(t, 5)
	leader := c.CheckOneLeader()

	_, index, _ := c.Start([]byte("before"))
	c.WaitCommit(index)
	c.WaitApplied(index)

	var follower raft.NodeID
	for id := raft.NodeID(1); id <= 5; id++ {
		if id != leader {
			follower = id
			break
		}
	}
	c.Disconnect(follower)
	c.Crash(follower)
	c.Restart(follower)
	c.Reconnect(follower)

	c.CheckOneLeader()
	_, index2, _ := c.Start([]byte("after-partition-restart"))
	c.WaitCommit(index2)
	c.WaitApplied(index2)
	c.CheckAppliedSame()

	for id := raft.NodeID(1); id <= 5; id++ {
		applied := c.Applied(id)
		if len(applied) < 2 {
			t.Fatalf("node %d missing entries: %v", id, applied)
		}
		if string(applied[0]) != "before" {
			t.Fatalf("node %d lost committed entry: %v", id, applied)
		}
	}
}

func TestCrashDuringElection(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Simulate: node persisted a vote for candidate 2 in term 9, then crashed
	// before (or after) replying.
	cand := raft.NodeID(2)
	if err := st.SaveHardState(9, &cand); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	st2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	cfg := raft.DefaultConfig(1, []raft.NodeID{2, 3})
	cfg.DataDir = dir
	n := raft.New(cfg, stubTransport{}, raft.RealClock{}, st2, raft.NoopStateMachine{})
	n.Run()
	defer n.Stop()

	// Different candidate in the same term must not receive a vote.
	reply, err := n.RequestVote(t.Context(), &raft.RequestVoteArgs{
		Term:         9,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.VoteGranted {
		t.Fatal("voted twice in the same term")
	}

	// Original candidate may still receive the vote (idempotent).
	reply, err = n.RequestVote(t.Context(), &raft.RequestVoteArgs{
		Term:         9,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.VoteGranted {
		t.Fatal("should still grant vote to original candidate")
	}
}

type stubTransport struct{}

func (stubTransport) SendRequestVote(context.Context, raft.NodeID, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, nil
}
func (stubTransport) SendAppendEntries(context.Context, raft.NodeID, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, nil
}
func (stubTransport) SendInstallSnapshot(context.Context, raft.NodeID, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, nil
}

func TestUnreliableChurn(t *testing.T) {
	c := NewPersistentCluster(t, 5)
	c.CheckOneLeader()

	rng := rand.New(rand.NewSource(42))
	acked := make(map[uint64][]byte)
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		switch rng.Intn(5) {
		case 0, 1, 2:
			cmd := []byte(fmt.Sprintf("w-%d", len(acked)+1))
			leader := c.CheckOneLeader()
			index, _, ok := c.Node(leader).Start(cmd)
			if ok {
				// Wait briefly; if it commits, record it.
				wait := time.Now().Add(400 * time.Millisecond)
				for time.Now().Before(wait) {
					count := 0
					for id := raft.NodeID(1); id <= 5; id++ {
						if c.nodes[id] != nil && c.Node(id).CommitIndex() >= index {
							count++
						}
					}
					if count >= 3 {
						acked[index] = append([]byte(nil), cmd...)
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		case 3:
			id := raft.NodeID(rng.Intn(5) + 1)
			if c.nodes[id] != nil {
				c.Crash(id)
				time.Sleep(20 * time.Millisecond)
				c.Restart(id)
			}
		default:
			id := raft.NodeID(rng.Intn(5) + 1)
			if c.connected(id) {
				c.Disconnect(id)
				time.Sleep(50 * time.Millisecond)
				c.Reconnect(id)
			} else {
				c.Reconnect(id)
			}
		}
		c.CheckElectionSafety()
	}

	// Stabilize.
	for id := raft.NodeID(1); id <= 5; id++ {
		if c.nodes[id] == nil {
			c.Restart(id)
		}
		c.Reconnect(id)
	}
	c.CheckOneLeader()
	_, idx, _ := c.Start([]byte("churn-final"))
	c.WaitCommit(idx)
	c.WaitApplied(idx)

	// Zero-data-loss: every acked index must be present identically on all live nodes.
	for id := raft.NodeID(1); id <= 5; id++ {
		applied := c.Applied(id)
		for index, cmd := range acked {
			if int(index) > len(applied) {
				t.Fatalf("node %d missing committed index %d (%q); applied=%d", id, index, cmd, len(applied))
			}
			if string(applied[index-1]) != string(cmd) {
				t.Fatalf("node %d index %d = %q want %q", id, index, applied[index-1], cmd)
			}
		}
	}
	c.CheckAppliedSame()
}

func TestTornWriteToleratedCluster(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.AppendEntries([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("ok")},
		{Term: 1, Index: 2, Command: []byte("torn")},
	})
	_ = st.Close()

	path := filepath.Join(dir, "log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	_, entries, err := st2.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if string(e.Command) == "torn" {
			t.Fatalf("partial record loaded: %v", entries)
		}
	}
	if len(entries) < 2 || string(entries[1].Command) != "ok" {
		t.Fatalf("expected intact prefix, got %v", entries)
	}
}
