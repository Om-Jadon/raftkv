package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/test/harness"
)

type kvCluster struct {
	t     *testing.T
	h     *harness.Cluster
	kvs   map[raft.NodeID]*KV
	svcs  map[raft.NodeID]*Server
	https map[raft.NodeID]*httptest.Server
	addrs []string
}

func startKVCluster(t *testing.T, n int) *kvCluster {
	t.Helper()
	kc := &kvCluster{
		t:     t,
		kvs:   make(map[raft.NodeID]*KV, n),
		svcs:  make(map[raft.NodeID]*Server, n),
		https: make(map[raft.NodeID]*httptest.Server, n),
	}
	kc.h = harness.NewClusterWith(t, n, harness.ClusterOptions{
		NewSM: func(id raft.NodeID) raft.StateMachine {
			store := New()
			kc.kvs[id] = store
			return store
		},
	})
	addrs := make(map[raft.NodeID]string, n)
	for id := raft.NodeID(1); id <= raft.NodeID(n); id++ {
		svc := NewServer(kc.h.Node(id))
		kc.svcs[id] = svc
		hs := httptest.NewServer(svc.Handler())
		kc.https[id] = hs
		addrs[id] = hs.URL
		kc.addrs = append(kc.addrs, hs.URL)
		t.Cleanup(hs.Close)
	}
	for _, svc := range kc.svcs {
		svc.SetAddrs(addrs)
	}
	kc.h.CheckOneLeader()
	return kc
}

func (kc *kvCluster) client() *Client {
	return NewClient(kc.addrs)
}

func (kc *kvCluster) clientAt(id raft.NodeID) *Client {
	return NewClient([]string{kc.https[id].URL})
}

func (kc *kvCluster) crash(id raft.NodeID) {
	kc.h.Crash(id)
}

func (kc *kvCluster) restart(id raft.NodeID) {
	kc.h.Restart(id)
	kc.svcs[id].SetNode(kc.h.Node(id))
}

func (kc *kvCluster) maybeLeader() raft.NodeID {
	for id := raft.NodeID(1); id <= raft.NodeID(kc.h.Len()); id++ {
		n := kc.h.TryNode(id)
		if n == nil {
			continue
		}
		if role, _ := n.RoleAndTerm(); role == raft.Leader {
			return id
		}
	}
	return 0
}

func TestKVBasic(t *testing.T) {
	kc := startKVCluster(t, 3)
	c := kc.client()
	ctx := context.Background()

	if err := c.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	v, err := c.Get(ctx, "a")
	if err != nil || v != "1" {
		t.Fatalf("get a = %q, %v", v, err)
	}
	if err := c.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Get(ctx, "a")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestKVLeaderRedirect(t *testing.T) {
	kc := startKVCluster(t, 3)
	leader := kc.h.CheckOneLeader()
	var follower raft.NodeID
	for id := raft.NodeID(1); id <= 3; id++ {
		if id != leader {
			follower = id
			break
		}
	}
	c := kc.clientAt(follower)
	ctx := context.Background()
	if err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	v, err := c.Get(ctx, "k")
	if err != nil || v != "v" {
		t.Fatalf("get = %q, %v want v", v, err)
	}
}

func TestKVExactlyOnce(t *testing.T) {
	kc := startKVCluster(t, 3)
	leader := kc.h.CheckOneLeader()
	kc.svcs[leader].DropNextReply.Store(true)

	c := kc.client()
	ctx := context.Background()
	got, err := c.Append(ctx, "x", "a")
	if err != nil {
		t.Fatal(err)
	}
	if got != "a" {
		t.Fatalf("append = %q want a", got)
	}
	// A second append with a new seq should yield "aa" if the first applied once.
	got, err = c.Append(ctx, "x", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got != "ab" {
		t.Fatalf("append = %q want ab (duplicate apply?)", got)
	}
}

func TestKVDuringLeaderChange(t *testing.T) {
	kc := startKVCluster(t, 5)
	c := kc.client()
	ctx := context.Background()

	acked := make(map[string]string)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("v-%d", i)
		if err := c.Put(ctx, key, val); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		acked[key] = val
		if i%5 == 4 {
			if lead := kc.maybeLeader(); lead != 0 {
				kc.crash(lead)
				time.Sleep(80 * time.Millisecond)
				kc.restart(lead)
			}
		}
	}

	for k, want := range acked {
		got, err := c.Get(ctx, k)
		if err != nil || got != want {
			t.Fatalf("after churn get %s = %q, %v want %q", k, got, err, want)
		}
	}
}

func TestKVConcurrentClients(t *testing.T) {
	kc := startKVCluster(t, 3)
	ctx := context.Background()
	const clients, ops = 20, 50

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		i := i
		go func() {
			defer wg.Done()
			c := kc.client()
			key := fmt.Sprintf("c-%d", i)
			model := ""
			for j := 0; j < ops; j++ {
				switch j % 3 {
				case 0:
					val := fmt.Sprintf("%d-%d", i, j)
					if err := c.Put(ctx, key, val); err != nil {
						errs <- err
						return
					}
					model = val
				case 1:
					got, err := c.Get(ctx, key)
					if err != nil {
						errs <- err
						return
					}
					if got != model {
						errs <- fmt.Errorf("client %d get %q want %q", i, got, model)
						return
					}
				default:
					if err := c.Delete(ctx, key); err != nil {
						errs <- err
						return
					}
					model = ""
					_, err := c.Get(ctx, key)
					if model == "" && !errors.Is(err, ErrNotFound) {
						errs <- fmt.Errorf("client %d expected missing, got %v", i, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestNoStaleRead(t *testing.T) {
	kc := startKVCluster(t, 5)
	c := kc.client()
	ctx := context.Background()
	if err := c.Put(ctx, "k", "old"); err != nil {
		t.Fatal(err)
	}

	oldLeader := kc.h.CheckOneLeader()
	kc.h.Disconnect(oldLeader)

	// Majority elects and writes a new value.
	deadline := time.Now().Add(3 * time.Second)
	var newC *Client
	for time.Now().Before(deadline) {
		var addrs []string
		for id := raft.NodeID(1); id <= 5; id++ {
			if id != oldLeader {
				addrs = append(addrs, kc.https[id].URL)
			}
		}
		newC = NewClient(addrs)
		if err := newC.Put(ctx, "k", "new"); err == nil {
			break
		}
		newC = nil
		time.Sleep(50 * time.Millisecond)
	}
	if newC == nil {
		t.Fatal("majority failed to put after partition")
	}

	// Direct GET on the partitioned old leader must not return the stale value.
	httpClient := &http.Client{Timeout: 400 * time.Millisecond}
	resp, err := httpClient.Get(kc.https[oldLeader].URL + "/kv/" + url.PathEscape("k"))
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == 200 && string(body) == "old" {
			t.Fatalf("partitioned leader served stale read %q", body)
		}
		if resp.StatusCode == 200 && string(body) == "new" {
			t.Fatalf("partitioned leader served %q without quorum", body)
		}
	}
}
