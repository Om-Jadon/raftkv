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
	delayMax time.Duration
	dupRate  float64
	isolated map[raft.NodeID]struct{}
	blocked  map[raft.NodeID]map[raft.NodeID]struct{}
	hook     func(kind string, from, to raft.NodeID, err error)
	closed   bool
	wg       sync.WaitGroup
}

// New returns an empty network. seed drives drop/delay randomness.
func New(seed int64) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		servers:  make(map[raft.NodeID]raft.RPCHandler),
		isolated: make(map[raft.NodeID]struct{}),
		blocked:  make(map[raft.NodeID]map[raft.NodeID]struct{}),
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
	n.delayMax = 0
}

// SetDelayRange sets a random per-RPC delay in [0, max]. Reorders naturally.
func (n *Network) SetDelayRange(max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = 0
	n.delayMax = max
}

// SetDupRate is the probability that a delivered RPC is sent twice.
func (n *Network) SetDupRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dupRate = p
}

// SetHook is called for every send attempt (err is nil, ErrDropped, or ErrPartitioned).
func (n *Network) SetHook(h func(kind string, from, to raft.NodeID, err error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hook = h
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

// Block drops RPCs from → to (asymmetric).
func (n *Network) Block(from, to raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[from] == nil {
		n.blocked[from] = make(map[raft.NodeID]struct{})
	}
	n.blocked[from][to] = struct{}{}
}

// Unblock restores from → to.
func (n *Network) Unblock(from, to raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked[from], to)
}

// ClearFaults restores reliable, fully connected delivery (does not re-register servers).
func (n *Network) ClearFaults() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = 0
	n.delay = 0
	n.delayMax = 0
	n.dupRate = 0
	n.isolated = make(map[raft.NodeID]struct{})
	n.blocked = make(map[raft.NodeID]map[raft.NodeID]struct{})
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
	h, delay, dup, err := c.net.prepare("RequestVote", c.from, to)
	if err != nil {
		return nil, err
	}
	send := func() (*raft.RequestVoteReply, error) { return h.RequestVote(ctx, args) }
	if dup {
		go func() { _, _ = await(ctx, &c.net.wg, delay, send) }()
	}
	return await(ctx, &c.net.wg, delay, send)
}

func (c *client) SendAppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, delay, dup, err := c.net.prepare("AppendEntries", c.from, to)
	if err != nil {
		return nil, err
	}
	send := func() (*raft.AppendEntriesReply, error) { return h.AppendEntries(ctx, args) }
	if dup {
		go func() { _, _ = await(ctx, &c.net.wg, delay, send) }()
	}
	return await(ctx, &c.net.wg, delay, send)
}

func (c *client) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	h, delay, dup, err := c.net.prepare("InstallSnapshot", c.from, to)
	if err != nil {
		return nil, err
	}
	send := func() (*raft.InstallSnapshotReply, error) { return h.InstallSnapshot(ctx, args) }
	if dup {
		go func() { _, _ = await(ctx, &c.net.wg, delay, send) }()
	}
	return await(ctx, &c.net.wg, delay, send)
}

func (n *Network) prepare(kind string, from, to raft.NodeID) (raft.RPCHandler, time.Duration, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	hook := n.hook
	fail := func(err error) (raft.RPCHandler, time.Duration, bool, error) {
		if hook != nil {
			hook(kind, from, to, err)
		}
		return nil, 0, false, err
	}
	if n.closed {
		return fail(ErrClosed)
	}
	if _, ok := n.isolated[from]; ok {
		return fail(ErrPartitioned)
	}
	if _, ok := n.isolated[to]; ok {
		return fail(ErrPartitioned)
	}
	if m := n.blocked[from]; m != nil {
		if _, ok := m[to]; ok {
			return fail(ErrPartitioned)
		}
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return fail(ErrDropped)
	}
	h, ok := n.servers[to]
	if !ok {
		return fail(ErrUnknownPeer)
	}
	delay := n.delay
	if n.delayMax > 0 {
		delay = time.Duration(n.rng.Int63n(int64(n.delayMax) + 1))
	}
	dup := n.dupRate > 0 && n.rng.Float64() < n.dupRate
	if hook != nil {
		hook(kind, from, to, nil)
	}
	return h, delay, dup, nil
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
				select {
				case ch <- struct {
					v   T
					err error
				}{zero, ctx.Err()}:
				default:
				}
				return
			case <-timer.C:
			}
		}
		v, err := fn()
		select {
		case ch <- struct {
			v   T
			err error
		}{v, err}:
		default:
		}
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-ch:
		return r.v, r.err
	}
}
