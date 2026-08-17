package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Node is a single Raft peer. mu guards all mutable fields below it.
type Node struct {
	mu sync.Mutex

	id    NodeID
	peers []NodeID
	cfg   Config

	currentTerm uint64
	votedFor    *NodeID
	log         []LogEntry // log[0] is the sentinel {Term:0, Index:0}

	role        Role
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	transport Transport
	clock     Clock
	storage   Storage
	sm        StateMachine
	applyCh   chan ApplyMsg

	rng              *rand.Rand
	electionDeadline time.Time

	rpcMu         sync.Mutex
	requestVotes  int
	appendEntries int
	installSnaps  int

	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

// New constructs a Follower with a sentinel log entry at index 0.
func New(cfg Config, transport Transport, clock Clock, storage Storage, sm StateMachine) *Node {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 10 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 150*time.Millisecond
	}
	if clock == nil {
		clock = RealClock{}
	}
	if storage == nil {
		storage = NewMemoryStorage()
	}
	if sm == nil {
		sm = NoopStateMachine{}
	}

	n := &Node{
		id:         cfg.ID,
		peers:      append([]NodeID(nil), cfg.Peers...),
		cfg:        cfg,
		role:       Follower,
		log:        []LogEntry{{Term: 0, Index: 0}},
		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		transport:  transport,
		clock:      clock,
		storage:    storage,
		sm:         sm,
		applyCh:    make(chan ApplyMsg, 64),
		rng:        rand.New(rand.NewSource(cfg.RandSeed)),
		done:       make(chan struct{}),
	}

	if hs, entries, err := storage.Load(); err == nil {
		n.currentTerm = hs.CurrentTerm
		n.votedFor = hs.VotedFor
		if len(entries) > 0 {
			n.log = entries
		}
	}
	n.resetElectionDeadlineLocked()
	return n
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// Role returns the current Raft role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Start launches the ticker goroutine. It is a no-op if already started.
func (n *Node) Start() {
	n.startOnce.Do(func() {
		n.wg.Add(1)
		go n.runTicker()
	})
}

// Stop joins background goroutines. It is idempotent.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.done)
		n.wg.Wait()
	})
}

func (n *Node) runTicker() {
	defer n.wg.Done()
	ticker := n.clock.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-ticker.Chan():
			n.onTick()
		}
	}
}

func (n *Node) onTick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Phase 1: election timeout is tracked but does not change role.
	// Phase 2 will start an election when n.clock.Now().After(n.electionDeadline).
}

func (n *Node) resetElectionDeadlineLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	jitter := n.cfg.ElectionTimeoutMin
	if span > 0 {
		jitter += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = n.clock.Now().Add(jitter)
}

// lastIndex/termAt/entryAt must be called with n.mu held.
// Never index n.log directly outside these helpers.

func (n *Node) firstIndex() uint64 {
	return n.log[0].Index
}

func (n *Node) lastIndex() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) termAt(index uint64) (uint64, bool) {
	off := int(index - n.log[0].Index)
	if off < 0 || off >= len(n.log) {
		return 0, false
	}
	return n.log[off].Term, true
}

func (n *Node) entryAt(index uint64) (LogEntry, bool) {
	off := int(index - n.log[0].Index)
	if off < 0 || off >= len(n.log) {
		return LogEntry{}, false
	}
	return n.log[off], true
}

// RequestVoteCount is a Phase 1 test hook: how many RequestVote RPCs this node handled.
func (n *Node) RequestVoteCount() int {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	return n.requestVotes
}

// RequestVote handles an incoming RequestVote RPC. Phase 1 only records it.
func (n *Node) RequestVote(_ context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.rpcMu.Lock()
	n.requestVotes++
	n.rpcMu.Unlock()

	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()
	_ = args
	return &RequestVoteReply{Term: term}, nil
}

// AppendEntries handles an incoming AppendEntries RPC. Phase 1 only records it.
func (n *Node) AppendEntries(_ context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.rpcMu.Lock()
	n.appendEntries++
	n.rpcMu.Unlock()

	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()
	_ = args
	return &AppendEntriesReply{Term: term}, nil
}

// InstallSnapshot handles an incoming InstallSnapshot RPC. Phase 1 only records it.
func (n *Node) InstallSnapshot(_ context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.rpcMu.Lock()
	n.installSnaps++
	n.rpcMu.Unlock()

	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()
	_ = args
	return &InstallSnapshotReply{Term: term}, nil
}
