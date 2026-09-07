package harness

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/kv"
	"github.com/Om-Jadon/raftkv/internal/raft"
)

const snapThreshold = 2048

func snapCluster(t *testing.T, n int, persist bool) *Cluster {
	t.Helper()
	return NewClusterWith(t, n, ClusterOptions{
		Persist:                persist,
		SnapshotThresholdBytes: snapThreshold,
		NewSM:                  func(raft.NodeID) raft.StateMachine { return kv.New() },
	})
}

func putCmd(key, val string, client, seq uint64) []byte {
	b, err := json.Marshal(kv.Command{Op: kv.OpPut, Key: key, Value: val, ClientID: client, SeqNum: seq})
	if err != nil {
		panic(err)
	}
	return b
}

func waitLeaderStart(t *testing.T, c *Cluster, cmd []byte) uint64 {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		leader := c.CheckOneLeader()
		index, _, ok := c.Node(leader).Start(cmd)
		if ok {
			return index
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader accepted Start")
	return 0
}

func TestSnapshotBasic(t *testing.T) {
	c := snapCluster(t, 3, false)
	defer c.Shutdown()

	const n = 10000
	var last uint64
	for i := 1; i <= n; i++ {
		last = waitLeaderStart(t, c, putCmd("k", "v", 1, uint64(i)))
		if i%100 == 0 {
			c.WaitCommit(last)
		}
	}
	c.WaitCommit(last)
	c.WaitApplied(last)

	deadline := time.Now().Add(2 * time.Second)
	var retained int
	for time.Now().Before(deadline) {
		retained = 0
		ok := true
		for id := raft.NodeID(1); id <= 3; id++ {
			nlen := c.Node(id).LogLen()
			if nlen > retained {
				retained = nlen
			}
			if nlen > 500 {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if retained > 500 {
		t.Fatalf("retained log entries = %d after %d writes (want <= 500)", retained, n)
	}

	idx := waitLeaderStart(t, c, putCmd("final", "yes", 2, 1))
	c.WaitCommit(idx)
	c.WaitApplied(idx)
	store, ok := c.Node(c.CheckOneLeader()).StateMachine().(*kv.KV)
	if !ok {
		t.Fatal("expected kv state machine")
	}
	if v, found := store.GetLocal("final"); !found || v != "yes" {
		t.Fatalf("read after compaction: found=%v v=%q", found, v)
	}
}

func TestInstallSnapshotLaggingFollower(t *testing.T) {
	c := snapCluster(t, 3, false)
	defer c.Shutdown()

	first := waitLeaderStart(t, c, putCmd("a", "1", 1, 1))
	c.WaitCommit(first)
	c.WaitApplied(first)

	lag := raft.NodeID(3)
	c.Crash(lag)
	before := 0
	if n := c.TryNode(lag); n != nil {
		before = n.InstallSnapshotCount()
	}

	var last uint64
	for i := 2; i <= 400; i++ {
		last = waitLeaderStart(t, c, putCmd("k", "x", 1, uint64(i)))
	}
	c.WaitCommit(last)
	c.WaitApplied(last)

	c.Restart(lag)
	c.WaitApplied(last)

	got := c.Node(lag).InstallSnapshotCount()
	if got <= before {
		t.Fatalf("lagging follower InstallSnapshot count = %d, want > %d", got, before)
	}

	stores := map[raft.NodeID]*kv.KV{}
	for id := raft.NodeID(1); id <= 3; id++ {
		k, ok := c.Node(id).StateMachine().(*kv.KV)
		if !ok {
			t.Fatal("expected kv")
		}
		stores[id] = k
	}
	ref := stores[1].Clone()
	for id := raft.NodeID(2); id <= 3; id++ {
		got := stores[id].Clone()
		if len(got) != len(ref) {
			t.Fatalf("node %d kv size %d want %d", id, len(got), len(ref))
		}
		for k, v := range ref {
			if got[k] != v {
				t.Fatalf("node %d key %s = %q want %q", id, k, got[k], v)
			}
		}
	}
}

func TestSnapshotCrashRestart(t *testing.T) {
	c := snapCluster(t, 3, true)
	defer c.Shutdown()

	var last uint64
	for i := 1; i <= 300; i++ {
		last = waitLeaderStart(t, c, putCmd("k", "v", 1, uint64(i)))
	}
	idx := waitLeaderStart(t, c, putCmd("keep", "yes", 2, 1))
	if idx > last {
		last = idx
	}
	c.WaitCommit(last)
	c.WaitApplied(last)

	for id := raft.NodeID(1); id <= 3; id++ {
		c.Restart(id)
	}
	c.CheckOneLeader()
	bump := waitLeaderStart(t, c, putCmd("after", "1", 3, 1))
	c.WaitCommit(bump)
	c.WaitApplied(bump)

	for id := raft.NodeID(1); id <= 3; id++ {
		k, ok := c.Node(id).StateMachine().(*kv.KV)
		if !ok {
			t.Fatal("expected kv")
		}
		if v, found := k.GetLocal("keep"); !found || v != "yes" {
			t.Fatalf("node %d lost keep after restart: found=%v v=%q", id, found, v)
		}
	}
}

func TestSessionTableSurvivesSnapshot(t *testing.T) {
	c := snapCluster(t, 3, true)
	defer c.Shutdown()

	first := waitLeaderStart(t, c, putCmd("k", "orig", 9, 1))
	c.WaitCommit(first)
	c.WaitApplied(first)

	var last uint64
	for i := 2; i <= 300; i++ {
		last = waitLeaderStart(t, c, putCmd("pad", "x", 1, uint64(i)))
	}
	c.WaitCommit(last)
	c.WaitApplied(last)

	for id := raft.NodeID(1); id <= 3; id++ {
		c.Restart(id)
	}
	c.CheckOneLeader()

	dup := waitLeaderStart(t, c, putCmd("k", "dup", 9, 1))
	c.WaitCommit(dup)
	c.WaitApplied(dup)

	for id := raft.NodeID(1); id <= 3; id++ {
		k, ok := c.Node(id).StateMachine().(*kv.KV)
		if !ok {
			t.Fatal("expected kv")
		}
		if v, _ := k.GetLocal("k"); v != "orig" {
			t.Fatalf("node %d re-applied duplicate: %q", id, v)
		}
	}
}
