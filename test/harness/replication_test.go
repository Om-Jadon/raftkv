package harness

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func (c *Cluster) majority() int {
	return c.n/2 + 1
}

// Start submits cmd on the current leader and returns leader id, log index, and term.
func (c *Cluster) Start(cmd []byte) (raft.NodeID, uint64, uint64) {
	c.t.Helper()
	leader := c.CheckOneLeader()
	index, term, ok := c.Node(leader).Start(cmd)
	if !ok {
		c.t.Fatalf("Start on leader %d returned isLeader=false", leader)
	}
	return leader, index, term
}

// WaitCommit waits until a majority of live nodes have commitIndex >= index.
func (c *Cluster) WaitCommit(index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		c.CheckAppliedSame()
		count := 0
		for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
			if !c.connected(id) {
				continue
			}
			if c.Node(id).CommitIndex() >= index {
				count++
			}
		}
		if count >= c.majority() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("index %d not committed on majority", index)
}

// WaitApplied waits until every connected node has applied through index.
func (c *Cluster) WaitApplied(index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		c.CheckAppliedSame()
		ok := true
		for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
			if !c.connected(id) {
				continue
			}
			if c.Node(id).LastApplied() < index {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("index %d not applied on all connected nodes", index)
}

// Applied returns recorded commands applied on id.
func (c *Cluster) Applied(id raft.NodeID) [][]byte {
	c.t.Helper()
	sm, ok := c.sms[id]
	if !ok || sm == nil {
		c.t.Fatalf("no state machine for node %d", id)
	}
	return sm.Applied()
}

// CheckAppliedSame fails if connected nodes disagree on applied commands at any index.
func (c *Cluster) CheckAppliedSame() {
	c.t.Helper()
	var ref []raft.NodeID
	var refApplied [][]byte
	for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
		if !c.connected(id) {
			continue
		}
		applied := c.Applied(id)
		if ref == nil {
			ref = append(ref, id)
			refApplied = applied
			continue
		}
		if len(applied) != len(refApplied) {
			continue
		}
		for i := range applied {
			if !bytesEqual(applied[i], refApplied[i]) {
				c.t.Fatalf("nodes disagree at applied index %d: node %d vs ref %d", i+1, id, ref[0])
			}
		}
	}
}

// WaitNoCommit waits until deadline and fails if index becomes committed on a majority.
func (c *Cluster) WaitNoCommit(index uint64, window time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		count := 0
		for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
			if !c.connected(id) {
				continue
			}
			if c.Node(id).CommitIndex() >= index {
				count++
			}
		}
		if count >= c.majority() {
			c.t.Fatalf("index %d committed without majority quorum", index)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func cmd(n int) []byte {
	return []byte(fmt.Sprintf("cmd-%d", n))
}

func TestBasicAgreement(t *testing.T) {
	c := NewCluster(t, 3)
	c.CheckOneLeader()

	var want [][]byte
	for i := 1; i <= 3; i++ {
		_, index, _ := c.Start(cmd(i))
		c.WaitCommit(index)
		c.WaitApplied(index)
		want = append(want, cmd(i))
		c.CheckAppliedSame()
	}

	for id := raft.NodeID(1); id <= 3; id++ {
		applied := c.Applied(id)
		if len(applied) != len(want) {
			t.Fatalf("node %d applied %d entries, want %d", id, len(applied), len(want))
		}
		for i := range want {
			if !bytesEqual(applied[i], want[i]) {
				t.Fatalf("node %d applied[%d] mismatch", id, i)
			}
		}
	}
}

func TestNoAgreementWithoutMajority(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.CheckOneLeader()

	disconnectFollowers(c, leader, 3)

	index, _, ok := c.Node(leader).Start(cmd(99))
	if !ok {
		t.Fatalf("leader Start failed")
	}
	c.WaitNoCommit(index, 500*time.Millisecond)

	restartAll(c)
	_, index2, _ := c.Start(cmd(100))
	c.WaitCommit(index2)
	c.WaitApplied(index2)
}

func TestFollowerCatchUp(t *testing.T) {
	c := NewCluster(t, 5)
	c.CheckOneLeader()

	c.Disconnect(5)
	for i := 1; i <= 50; i++ {
		_, index, _ := c.Start(cmd(i))
		c.WaitCommit(index)
	}
	c.Reconnect(5)
	c.WaitApplied(50)
	c.CheckAppliedSame()
}

func TestConflictingEntriesOverwritten(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.CheckOneLeader()

	// Put the current leader in a 2-node minority partition.
	minority := []raft.NodeID{leader}
	for id := raft.NodeID(1); id <= 5; id++ {
		if id != leader {
			minority = append(minority, id)
			break
		}
	}
	for id := raft.NodeID(1); id <= 5; id++ {
		inMinority := false
		for _, mid := range minority {
			if id == mid {
				inMinority = true
				break
			}
		}
		if !inMinority {
			c.Disconnect(id)
		}
	}

	idx1, _, ok := c.Node(leader).Start(cmd(1))
	if !ok {
		t.Fatalf("minority leader Start failed")
	}
	c.WaitNoCommit(idx1, 300*time.Millisecond)

	for id := raft.NodeID(1); id <= 5; id++ {
		c.Reconnect(id)
	}
	for _, id := range minority {
		c.Disconnect(id)
	}

	newLeader := c.CheckOneLeader()
	idx2, _, ok := c.Node(newLeader).Start(cmd(2))
	if !ok {
		t.Fatalf("majority leader Start failed")
	}
	c.WaitCommit(idx2)

	for id := raft.NodeID(1); id <= 5; id++ {
		c.Reconnect(id)
	}
	c.WaitApplied(idx2)
	c.CheckAppliedSame()

	for id := raft.NodeID(1); id <= 5; id++ {
		applied := c.Applied(id)
		if len(applied) < 1 {
			t.Fatalf("node %d has no applied entries", id)
		}
		if !bytesEqual(applied[0], cmd(2)) {
			t.Fatalf("node %d first command should be cmd-2, got %q", id, applied[0])
		}
	}
}

func disconnectFollowers(c *Cluster, leader raft.NodeID, n int) {
	disconnected := 0
	for id := raft.NodeID(1); id <= raft.NodeID(c.n) && disconnected < n; id++ {
		if id != leader {
			c.Crash(id)
			disconnected++
		}
	}
}

func restartAll(c *Cluster) {
	for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
		if c.nodes[id] == nil {
			c.Restart(id)
		}
	}
}

func TestConcurrentStarts(t *testing.T) {
	c := NewCluster(t, 3)
	leader := c.CheckOneLeader()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, _, ok := c.Node(leader).Start(cmd(i))
			if !ok {
				t.Errorf("Start on leader %d failed", leader)
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.CheckAppliedSame()
		all := true
		for id := raft.NodeID(1); id <= 3; id++ {
			if len(c.Applied(id)) < n {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for id := raft.NodeID(1); id <= 3; id++ {
		if len(c.Applied(id)) != n {
			t.Fatalf("node %d applied %d, want %d", id, len(c.Applied(id)), n)
		}
	}
	c.CheckAppliedSame()
}

func TestFigure8(t *testing.T) {
	for i := 0; i < 100; i++ {
		figure8Round(t, i)
	}
}

func figure8Round(t *testing.T, seed int) {
	t.Helper()
	c := NewCluster(t, 5)
	leader := c.CheckOneLeader()

	_, index, _ := c.Start(cmd(seed))
	c.WaitCommit(index)
	c.WaitApplied(index)

	for round := 0; round < 5; round++ {
		leader = c.CheckOneLeader()
		disconnectFollowers(c, leader, 3)

		_, idx, ok := c.Node(leader).Start(cmd(seed*100 + round))
		if !ok {
			t.Fatalf("leader Start failed")
		}
		c.WaitNoCommit(idx, 300*time.Millisecond)

		restartAll(c)
		_, idx, _ = c.Start(cmd(seed*1000 + round))
		c.WaitCommit(idx)
		c.WaitApplied(idx)
		c.CheckAppliedSame()
	}
}
