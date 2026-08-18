package harness

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func TestInitialElection(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader := c.CheckOneLeader()
	term := c.CheckTerms()
	if term < 1 {
		t.Fatalf("term = %d, want >= 1 after election", term)
	}

	time.Sleep(500 * time.Millisecond)
	c.CheckElectionSafety()
	leader2 := c.CheckOneLeader()
	if leader2 != leader {
		t.Fatalf("leader changed from %d to %d with no failures", leader, leader2)
	}
	if got := c.CheckTerms(); got != term {
		t.Fatalf("term churned %d -> %d with no failures", term, got)
	}
}

func TestReElectionAfterLeaderDisconnect(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	old := c.CheckOneLeader()
	oldTerm := c.Node(old).Term()

	c.Disconnect(old)
	newLeader := c.CheckOneLeader()
	if newLeader == old {
		t.Fatalf("disconnected leader %d still the connected leader", old)
	}
	c.CheckElectionSafety()

	c.Reconnect(old)
	time.Sleep(300 * time.Millisecond)
	c.CheckElectionSafety()
	leader := c.CheckOneLeader()
	if c.Node(old).Role() == raft.Leader && leader != old {
		t.Fatalf("old leader %d did not step down; connected leader is %d", old, leader)
	}
	if c.Node(old).Term() < oldTerm {
		t.Fatalf("old leader term went backwards")
	}
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	leader := c.CheckOneLeader()
	others := make([]raft.NodeID, 0, 4)
	for id := raft.NodeID(1); id <= 5; id++ {
		if id != leader {
			others = append(others, id)
		}
	}
	c.Disconnect(leader)
	c.Disconnect(others[0])
	c.Disconnect(others[1])
	c.CheckNoLeader(600 * time.Millisecond)

	c.Reconnect(others[0])
	c.CheckOneLeader()
	c.CheckElectionSafety()
}

func TestManyElections(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	rng := rand.New(rand.NewSource(1))
	c.CheckOneLeader()
	for round := 0; round < 10; round++ {
		id := raft.NodeID(rng.Intn(5) + 1)
		c.Disconnect(id)
		c.CheckOneLeader()
		c.CheckElectionSafety()
		c.Reconnect(id)
		c.CheckOneLeader()
		c.CheckElectionSafety()
	}
}
