package memory

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

var (
	// ErrDropped is returned when the network drops an RPC.
	ErrDropped = errors.New("memory transport: rpc dropped")
	// ErrPartitioned is returned when the sender or receiver is isolated.
	ErrPartitioned = errors.New("memory transport: partitioned")
	// ErrUnknownPeer is returned when the destination is not registered.
	ErrUnknownPeer = errors.New("memory transport: unknown peer")
	// ErrClosed is returned after Network.Close.
	ErrClosed = errors.New("memory transport: closed")
)

// Network is a controllable in-process Raft network.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	servers  map[raft.NodeID]raft.RPCHandler
	dropRate float64
	delay    time.Duration
	isolated map[raft.NodeID]struct{}
	closed   bool
	wg       sync.WaitGroup
}

// New returns an empty network. seed drives drop/delay randomness.
func New(seed int64) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		servers:  make(map[raft.NodeID]raft.RPCHandler),
		isolated: make(map[raft.NodeID]struct{}),
	}
}

// Register attaches a handler for id. Replaces any previous handler.
func (n *Network) Register(id raft.NodeID, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[id] = h
}

// Unregister removes the handler for id.
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, id)
}

// Transport returns a Transport that sends from the given node.
func (n *Network) Transport(from raft.NodeID) raft.Transport {
	return &client{net: n, from: from}
}

// SetDropRate sets the probability that any RPC is dropped. 1.0 drops everything.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetDelay sets a fixed delivery delay applied after drop/partition checks.
func (n *Network) SetDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// Isolate disconnects id from every other node (symmetric).
func (n *Network) Isolate(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated[id] = struct{}{}
}

// Unisolate restores connectivity for id.
func (n *Network) Unisolate(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolated, id)
}

// Close waits for in-flight RPCs. Further sends fail with ErrClosed.
func (n *Network) Close() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.wg.Wait()
}

type client struct {
	net  *Network
	from raft.NodeID
}

func (c *client) SendRequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, err := c.net.prepare(c.from, to)
	if err != nil {
		return nil, err
	}
	return await(ctx, &c.net.wg, c.net.delaySnapshot(), func() (*raft.RequestVoteReply, error) {
		return h.RequestVote(ctx, args)
	})
}

func (c *client) SendAppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, err := c.net.prepare(c.from, to)
	if err != nil {
		return nil, err
	}
	return await(ctx, &c.net.wg, c.net.delaySnapshot(), func() (*raft.AppendEntriesReply, error) {
		return h.AppendEntries(ctx, args)
	})
}

func (c *client) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	h, err := c.net.prepare(c.from, to)
	if err != nil {
		return nil, err
	}
	return await(ctx, &c.net.wg, c.net.delaySnapshot(), func() (*raft.InstallSnapshotReply, error) {
		return h.InstallSnapshot(ctx, args)
	})
}

func (n *Network) delaySnapshot() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.delay
}

func (n *Network) prepare(from, to raft.NodeID) (raft.RPCHandler, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClosed
	}
	if _, ok := n.isolated[from]; ok {
		return nil, ErrPartitioned
	}
	if _, ok := n.isolated[to]; ok {
		return nil, ErrPartitioned
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return nil, ErrDropped
	}
	h, ok := n.servers[to]
	if !ok {
		return nil, ErrUnknownPeer
	}
	return h, nil
}

func await[T any](ctx context.Context, wg *sync.WaitGroup, delay time.Duration, fn func() (T, error)) (T, error) {
	var zero T
	wg.Add(1)
	ch := make(chan struct {
		v   T
		err error
	}, 1)
	go func() {
		defer wg.Done()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				ch <- struct {
					v   T
					err error
				}{zero, ctx.Err()}
				return
			case <-timer.C:
			}
		}
		v, err := fn()
		ch <- struct {
			v   T
			err error
		}{v, err}
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-ch:
		return r.v, r.err
	}
}
