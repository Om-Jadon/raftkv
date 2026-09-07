package raft

import (
	"context"
	"time"
)

// MaxSnapshotBytes is the un-chunked InstallSnapshot payload cap (16MiB),
// matching the gRPC transport limit. Larger snapshots are not taken or sent;
// production would split them with Done=false.
const MaxSnapshotBytes = 16 << 20

func (n *Node) maybeSnapshot() {
	n.snapMu.Lock()
	defer n.snapMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	thresh := n.cfg.SnapshotThresholdBytes
	if thresh == 0 || n.logBytesLocked() <= thresh {
		return
	}
	upto := n.lastApplied
	if upto <= n.firstIndex() {
		return
	}
	term, ok := n.termAt(upto)
	if !ok {
		return
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		panic(err)
	}
	if uint64(len(data)) > MaxSnapshotBytes {
		return
	}
	snap := Snapshot{
		LastIncludedIndex: upto,
		LastIncludedTerm:  term,
		Data:              data,
	}
	// Persist the snapshot (and compact the durable log) before dropping the
	// in-memory prefix. SaveSnapshot holds storage's mutex, and we hold n.mu
	// so AppendEntries cannot interleave a tail write that compact would drop.
	if err := n.storage.SaveSnapshot(snap); err != nil {
		panic(err)
	}
	n.snap = snap
	n.hasSnap = true
	n.replacePrefixLocked(upto, term)
}

func (n *Node) sendSnapshot(peer NodeID) {
	n.mu.Lock()
	if n.role != Leader || !n.hasSnap {
		n.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.snap.LastIncludedIndex,
		LastIncludedTerm:  n.snap.LastIncludedTerm,
		Data:              append([]byte(nil), n.snap.Data...),
		Done:              true,
	}
	if uint64(len(args.Data)) > MaxSnapshotBytes {
		n.mu.Unlock()
		return
	}
	sentTerm := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, 2*time.Second)
	defer cancel()
	reply, err := n.transport.SendInstallSnapshot(ctx, peer, &args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != sentTerm {
		return
	}
	n.nextIndex[peer] = args.LastIncludedIndex + 1
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.advanceCommitLocked()
}

// InstallSnapshot handles Figure 13. snapMu is acquired before mu, matching Apply.
func (n *Node) InstallSnapshot(_ context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.rpcMu.Lock()
	n.installSnaps++
	n.rpcMu.Unlock()

	n.snapMu.Lock()
	defer n.snapMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
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
	reply.Term = n.currentTerm

	if !args.Done || uint64(len(args.Data)) > MaxSnapshotBytes {
		return reply, nil
	}
	if args.LastIncludedIndex <= n.lastApplied {
		return reply, nil
	}

	if err := n.sm.Restore(append([]byte(nil), args.Data...)); err != nil {
		return reply, err
	}

	snap := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              append([]byte(nil), args.Data...),
	}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		panic(err)
	}
	if err := n.storage.TruncateFrom(snap.LastIncludedIndex + 1); err != nil {
		panic(err)
	}

	n.snap = snap
	n.hasSnap = true
	// Discard the log; the last-included entry is the new sentinel at log[0].
	n.log = []LogEntry{{Term: snap.LastIncludedTerm, Index: snap.LastIncludedIndex}}
	if snap.LastIncludedIndex > n.commitIndex {
		n.commitIndex = snap.LastIncludedIndex
	}
	n.lastApplied = snap.LastIncludedIndex
	return reply, nil
}
