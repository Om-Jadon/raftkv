package grpctransport_test

import (
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	grpctransport "github.com/Om-Jadon/raftkv/internal/transport/grpc"
)

func TestGRPCClusterAgrees(t *testing.T) {
	const n = 3
	type node struct {
		tr *grpctransport.Transport
		rn *raft.Node
	}
	nodes := make([]*node, n)
	ids := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = raft.NodeID(i + 1)
	}

	for i := 0; i < n; i++ {
		id := ids[i]
		var peers []raft.NodeID
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		tr := grpctransport.New(id, nil)
		addr, err := tr.Listen("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_ = addr
		cfg := raft.WANConfig(id, peers)
		rn := raft.New(cfg, tr, raft.RealClock{}, raft.NewMemoryStorage(), raft.NoopStateMachine{})
		tr.SetHandler(rn)
		nodes[i] = &node{tr: tr, rn: rn}
		t.Cleanup(func() {
			rn.Stop()
			_ = tr.Close()
		})
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			nodes[i].tr.SetPeer(ids[j], nodes[j].tr.Addr())
		}
		nodes[i].rn.Run()
	}

	deadline := time.Now().Add(5 * time.Second)
	var leader *raft.Node
	for time.Now().Before(deadline) {
		var leads []*raft.Node
		for _, n := range nodes {
			if role, _ := n.rn.RoleAndTerm(); role == raft.Leader {
				leads = append(leads, n.rn)
			}
		}
		if len(leads) == 1 {
			leader = leads[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no unique leader")
	}

	index, _, ok := leader.Start([]byte("hello"))
	if !ok {
		t.Fatal("Start failed")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if n.rn.CommitIndex() < index {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("index %d not committed on all nodes", index)
}
