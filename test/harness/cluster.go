package harness

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/transport/memory"
)

// Cluster is an in-process group of Raft nodes sharing a memory network.
type Cluster struct {
	mu       sync.Mutex
	t        *testing.T
	n        int
	net      *memory.Network
	nodes    map[raft.NodeID]*raft.Node
	storage  map[raft.NodeID]raft.Storage
	sms      map[raft.NodeID]*RecordSM
	isolated map[raft.NodeID]bool
	seenLead map[uint64]raft.NodeID
	shutdown bool
}

// NewCluster starts n Followers with IDs 1..n, fully connected.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	if n <= 0 {
		t.Fatalf("NewCluster: n must be > 0")
	}
	c := &Cluster{
		t:        t,
		n:        n,
		net:      memory.New(1),
		nodes:    make(map[raft.NodeID]*raft.Node, n),
		storage:  make(map[raft.NodeID]raft.Storage, n),
		sms:      make(map[raft.NodeID]*RecordSM, n),
		isolated: make(map[raft.NodeID]bool, n),
		seenLead: make(map[uint64]raft.NodeID),
	}
	peers := make([]raft.NodeID, 0, n)
	for i := 1; i <= n; i++ {
		peers = append(peers, raft.NodeID(i))
	}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		cfg := raft.DefaultConfig(id, others(peers, id))
		st := raft.NewMemoryStorage()
		sm := &RecordSM{}
		node := raft.New(cfg, c.net.Transport(id), raft.RealClock{}, st, sm)
		c.net.Register(id, node)
		c.nodes[id] = node
		c.storage[id] = st
		c.sms[id] = sm
		node.Run()
	}
	t.Cleanup(c.Shutdown)
	return c
}

func others(all []raft.NodeID, self raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(all)-1)
	for _, id := range all {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// Len returns the configured cluster size.
func (c *Cluster) Len() int { return c.n }

// Node returns the live node with the given ID.
func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok || n == nil {
		c.t.Fatalf("no live node %d", id)
	}
	return n
}

// Transport returns the Raft transport that sends from the given node.
func (c *Cluster) Transport(from raft.NodeID) raft.Transport {
	return c.net.Transport(from)
}

// SetDropRate sets the probability that the fake network drops an RPC.
func (c *Cluster) SetDropRate(p float64) { c.net.SetDropRate(p) }

// Disconnect isolates id from every other node.
func (c *Cluster) Disconnect(id raft.NodeID) {
	c.isolated[id] = true
	c.net.Isolate(id)
}

// Reconnect restores connectivity for id.
func (c *Cluster) Reconnect(id raft.NodeID) {
	delete(c.isolated, id)
	c.net.Unisolate(id)
}

// Crash stops id and unregisters it from the network, keeping its storage.
func (c *Cluster) Crash(id raft.NodeID) {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok || n == nil {
		return
	}
	n.Stop()
	c.net.Unregister(id)
	c.isolated[id] = true
	c.nodes[id] = nil
}

// Restart replaces a crashed (or live) node with a fresh Node on the same storage.
func (c *Cluster) Restart(id raft.NodeID) {
	c.t.Helper()
	if live, ok := c.nodes[id]; ok && live != nil {
		c.Crash(id)
	}
	peers := make([]raft.NodeID, 0, c.n-1)
	for i := 1; i <= c.n; i++ {
		pid := raft.NodeID(i)
		if pid != id {
			peers = append(peers, pid)
		}
	}
	cfg := raft.DefaultConfig(id, peers)
	sm := c.sms[id]
	if sm == nil {
		sm = &RecordSM{}
		c.sms[id] = sm
	}
	sm.Reset()
	node := raft.New(cfg, c.net.Transport(id), raft.RealClock{}, c.storage[id], sm)
	c.net.Register(id, node)
	c.nodes[id] = node
	delete(c.isolated, id)
	node.Run()
}

// SendRequestVote sends a RequestVote RPC from one cluster node to another.
func (c *Cluster) SendRequestVote(from, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.net.Transport(from).SendRequestVote(ctx, to, args)
}

func (c *Cluster) connected(id raft.NodeID) bool {
	n, ok := c.nodes[id]
	return ok && n != nil && !c.isolated[id]
}

// CheckOneLeader fails unless exactly one connected node is Leader.
func (c *Cluster) CheckOneLeader() raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.CheckElectionSafety()
		var leaders []raft.NodeID
		for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
			if !c.connected(id) {
				continue
			}
			role, _ := c.nodes[id].RoleAndTerm()
			if role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("wanted exactly one connected leader")
	return 0
}

// CheckNoLeader waits window and fails if a connected node is Leader.
func (c *Cluster) CheckNoLeader(window time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		c.CheckElectionSafety()
		for id := raft.NodeID(1); id <= raft.NodeID(c.n); id++ {
			if !c.connected(id) {
				continue
			}
			role, _ := c.nodes[id].RoleAndTerm()
			if role == raft.Leader {
				c.t.Fatalf("node %d is leader without quorum", id)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// CheckElectionSafety fails if two live nodes are leaders in the same term.
func (c *Cluster) CheckElectionSafety() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	byTerm := make(map[uint64][]raft.NodeID)
	for id, n := range c.nodes {
		if n == nil {
			continue
		}
		role, term := n.RoleAndTerm()
		if role != raft.Leader {
			continue
		}
		byTerm[term] = append(byTerm[term], id)
	}
	for term, ids := range byTerm {
		if len(ids) > 1 {
			c.t.Fatalf("election safety: leaders %v share term %d", ids, term)
		}
		id := ids[0]
		if prev, ok := c.seenLead[term]; ok && prev != id {
			c.t.Fatalf("election safety: term %d had leader %d, now %d", term, prev, id)
		}
		c.seenLead[term] = id
	}
}

// CheckTerms fails if two live nodes disagree on currentTerm.
func (c *Cluster) CheckTerms() uint64 {
	c.t.Helper()
	var term uint64
	var seen bool
	for _, n := range c.nodes {
		if n == nil {
			continue
		}
		t := n.Term()
		if !seen {
			term = t
			seen = true
			continue
		}
		if t != term {
			c.t.Fatalf("terms disagree: %d vs %d", term, t)
		}
	}
	return term
}

// Shutdown stops every node and closes the fake network. It is idempotent.
func (c *Cluster) Shutdown() {
	if c.shutdown {
		return
	}
	c.shutdown = true
	for id, n := range c.nodes {
		if n != nil {
			n.Stop()
			c.nodes[id] = nil
		}
	}
	c.net.Close()
}
