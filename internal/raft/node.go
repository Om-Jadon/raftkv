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
	log         []LogEntry // log[0] is the sentinel, or the last-included snapshot entry

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
	applyWake chan struct{}
	waiters   map[uint64]waiter
	leaderID  NodeID

	snap    Snapshot
	hasSnap bool
	snapMu  sync.Mutex // serializes Apply / Snapshot / Restore

	rng              *rand.Rand
	electionDeadline time.Time
	lastHeartbeat    time.Time
	votes            map[NodeID]struct{}

	rpcMu         sync.Mutex
	requestVotes  int
	appendEntries int
	installSnaps  int

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
	wg        sync.WaitGroup

	replicateMu      sync.Mutex
	replicateRunning bool
	replicateAgain   bool
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
		applyWake:  make(chan struct{}, 1),
		waiters:    make(map[uint64]waiter),
		rng:        rand.New(rand.NewSource(cfg.RandSeed)),
		votes:      make(map[NodeID]struct{}),
		done:       make(chan struct{}),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())

	if hs, entries, err := storage.Load(); err == nil {
		n.currentTerm = hs.CurrentTerm
		n.votedFor = hs.VotedFor
		if len(entries) > 0 {
			n.log = entries
		}
	}
	if snap, ok, err := storage.LoadSnapshot(); err == nil && ok && snap.LastIncludedIndex > 0 {
		n.installRecoveredSnapshot(snap, n.log)
	}
	n.resetElectionDeadlineLocked()
	return n
}

func (n *Node) installRecoveredSnapshot(snap Snapshot, entries []LogEntry) {
	n.snap = snap
	n.hasSnap = true
	n.log = []LogEntry{{Term: snap.LastIncludedTerm, Index: snap.LastIncludedIndex}}
	for _, e := range entries {
		if e.Index > snap.LastIncludedIndex {
			n.log = append(n.log, e)
		}
	}
	n.lastApplied = snap.LastIncludedIndex
	n.commitIndex = snap.LastIncludedIndex
	if err := n.sm.Restore(append([]byte(nil), snap.Data...)); err != nil {
		panic(err)
	}
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

// RoleAndTerm returns role and term from one critical section.
func (n *Node) RoleAndTerm() (Role, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm
}

// CommitIndex returns the highest known committed log index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied returns the highest log index applied to the state machine.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// LeaderID returns the last known leader, or 0 if unknown.
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return n.leaderID
}

// Status returns a consistent snapshot of node metadata.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	leader := n.leaderID
	if n.role == Leader {
		leader = n.id
	}
	return Status{
		ID:          n.id,
		Role:        n.role,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LogSize:     uint64(len(n.log) - 1),
		Leader:      leader,
	}
}

// Run launches background goroutines. It is a no-op if already started.
func (n *Node) Run() {
	n.startOnce.Do(func() {
		n.wg.Add(2)
		go n.runApplier()
		go n.runTicker()
	})
}

// Stop joins background goroutines. It is idempotent.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.cancel()
		close(n.done)
		n.mu.Lock()
		n.failWaitersLocked()
		n.mu.Unlock()
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
	switch n.role {
	case Leader:
		due := n.clock.Now().Sub(n.lastHeartbeat) >= n.cfg.HeartbeatInterval
		if due {
			n.lastHeartbeat = n.clock.Now()
		}
		n.mu.Unlock()
		if due {
			n.triggerReplication()
		}
		return
	default:
		if !n.clock.Now().After(n.electionDeadline) {
			n.mu.Unlock()
			return
		}
		rv, rvPeers, becameLeader := n.startElectionLocked()
		n.mu.Unlock()
		if rv != nil {
			n.broadcastRequestVotes(*rv, rvPeers)
		}
		if becameLeader {
			n.triggerReplication()
		}
	}
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
	e, ok := n.entryAt(index)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

func (n *Node) entryAt(index uint64) (LogEntry, bool) {
	off := int(index - n.log[0].Index)
	if off < 0 || off >= len(n.log) {
		return LogEntry{}, false
	}
	return n.log[off], true
}

func (n *Node) sliceFrom(index uint64) []LogEntry {
	off := int(index - n.log[0].Index)
	if off < 0 || off >= len(n.log) {
		return nil
	}
	out := make([]LogEntry, len(n.log)-off)
	for i, e := range n.log[off:] {
		out[i] = LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: append([]byte(nil), e.Command...),
		}
	}
	return out
}

func (n *Node) logBytesLocked() uint64 {
	var nBytes uint64
	for i := 1; i < len(n.log); i++ {
		nBytes += 24 + uint64(len(n.log[i].Command))
	}
	return nBytes
}

func (n *Node) replacePrefixLocked(lastIncluded uint64, lastTerm uint64) {
	sentinel := LogEntry{Term: lastTerm, Index: lastIncluded}
	if e, ok := n.entryAt(lastIncluded); ok {
		sentinel.Command = append([]byte(nil), e.Command...)
	}
	var tail []LogEntry
	for i := 1; i < len(n.log); i++ {
		if n.log[i].Index > lastIncluded {
			tail = append(tail, n.log[i])
		}
	}
	n.log = append([]LogEntry{sentinel}, tail...)
}

// LogLen is the number of retained entries after the sentinel/snapshot base.
func (n *Node) LogLen() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) == 0 {
		return 0
	}
	return len(n.log) - 1
}

// StateMachine returns the applied state machine (test helper).
func (n *Node) StateMachine() StateMachine { return n.sm }

// RequestVoteCount is a Phase 1 test hook: how many RequestVote RPCs this node handled.
func (n *Node) RequestVoteCount() int {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	return n.requestVotes
}

// InstallSnapshotCount is how many InstallSnapshot RPCs this node handled.
func (n *Node) InstallSnapshotCount() int {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	return n.installSnaps
}

// RequestVote handles an incoming RequestVote RPC.
func (n *Node) RequestVote(_ context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.rpcMu.Lock()
	n.requestVotes++
	n.rpcMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
		reply.Term = n.currentTerm
	}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	canVote := n.votedFor == nil || *n.votedFor == args.CandidateID
	if canVote && n.logUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		cid := args.CandidateID
		n.votedFor = &cid
		n.persistLocked()
		n.resetElectionDeadlineLocked()
		reply.VoteGranted = true
	}
	reply.Term = n.currentTerm
	return reply, nil
}

// AppendEntries handles an incoming AppendEntries RPC.
func (n *Node) AppendEntries(_ context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.rpcMu.Lock()
	n.appendEntries++
	n.rpcMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
		reply.Term = n.currentTerm
	}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionDeadlineLocked()

	ok, cTerm, cIndex := CheckPrevLog(n.log, args.PrevLogIndex, args.PrevLogTerm)
	if !ok {
		reply.ConflictTerm = cTerm
		reply.ConflictIndex = cIndex
		reply.Term = n.currentTerm
		return reply, nil
	}

	truncFrom, toAppend := PlanLogMerge(n.log, args.PrevLogIndex, args.Entries)
	if truncFrom > 0 {
		n.truncateLogLocked(truncFrom)
	}
	n.appendLogLocked(toAppend)

	if args.LeaderCommit > n.commitIndex {
		lastNew := n.lastIndex()
		commitTo := args.LeaderCommit
		if commitTo > lastNew {
			commitTo = lastNew
		}
		n.bumpCommitLocked(commitTo)
	}

	reply.Success = true
	reply.Term = n.currentTerm
	return reply, nil
}
