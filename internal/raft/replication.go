package raft

import (
	"context"
	"time"
)

const maxAppendBatch = 500

// Start appends a command on the leader and returns its log index and term.
func (n *Node) Start(cmd []byte) (index, term uint64, isLeader bool) {
	if n.stopped() {
		return 0, 0, false
	}
	n.mu.Lock()
	if n.role != Leader {
		term = n.currentTerm
		n.mu.Unlock()
		return 0, term, false
	}
	index = n.lastIndex() + 1
	term = n.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: append([]byte(nil), cmd...),
	}
	n.appendLogLocked([]LogEntry{entry})
	n.mu.Unlock()
	n.triggerReplication()
	return index, term, true
}

func (n *Node) triggerReplication() {
	n.replicateMu.Lock()
	if n.replicateRunning {
		n.replicateAgain = true
		n.replicateMu.Unlock()
		return
	}
	n.replicateRunning = true
	n.wg.Add(1)
	n.replicateMu.Unlock()

	go func() {
		defer n.wg.Done()
		for {
			n.replicateAll()

			n.replicateMu.Lock()
			if !n.replicateAgain {
				n.replicateRunning = false
				n.replicateMu.Unlock()
				return
			}
			n.replicateAgain = false
			n.replicateMu.Unlock()
		}
	}()
}

func (n *Node) appendLogLocked(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	copied := make([]LogEntry, len(entries))
	copy(copied, entries)
	n.log = append(n.log, copied...)
	if err := n.storage.AppendEntries(copied); err != nil {
		panic(err)
	}
}

func (n *Node) truncateLogLocked(from uint64) {
	off := int(from - n.log[0].Index)
	if off <= 0 || off >= len(n.log) {
		return
	}
	n.log = append([]LogEntry(nil), n.log[:off]...)
	if err := n.storage.TruncateFrom(from); err != nil {
		panic(err)
	}
}

func (n *Node) replicateAll() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), n.peers...)
	n.mu.Unlock()
	for _, peer := range peers {
		n.replicateTo(peer)
	}
}

func (n *Node) replicateTo(peer NodeID) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if n.hasSnap && next <= n.firstIndex() {
		n.mu.Unlock()
		n.sendSnapshot(peer)
		return
	}
	prevIndex := next - 1
	prevTerm, ok := n.termAt(prevIndex)
	if !ok {
		n.mu.Unlock()
		if n.hasSnap {
			n.sendSnapshot(peer)
		}
		return
	}
	end := n.lastIndex() + 1
	if end > next+maxAppendBatch {
		end = next + maxAppendBatch
	}
	entries := n.sliceFrom(next)
	if uint64(len(entries)) > end-next {
		entries = entries[:end-next]
	}
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	sentTerm := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, 80*time.Millisecond)
	defer cancel()
	reply, err := n.transport.SendAppendEntries(ctx, peer, &args)
	if err != nil {
		return
	}
	n.handleReplicationReply(peer, sentTerm, args, reply)
}

func (n *Node) handleReplicationReply(peer NodeID, sentTerm uint64, args AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != sentTerm {
		return
	}
	if reply.Success {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if lastNew > n.matchIndex[peer] {
			n.matchIndex[peer] = lastNew
		}
		n.nextIndex[peer] = lastNew + 1
		n.advanceCommitLocked()
		return
	}
	if reply.ConflictTerm == 0 {
		n.nextIndex[peer] = reply.ConflictIndex
		return
	}
	if idx := n.findLastIndexOfTermLocked(reply.ConflictTerm); idx > 0 {
		n.nextIndex[peer] = idx + 1
	} else {
		n.nextIndex[peer] = reply.ConflictIndex
	}
}

func (n *Node) findLastIndexOfTermLocked(term uint64) uint64 {
	for i := int(n.lastIndex()); i >= int(n.firstIndex()); i-- {
		t, ok := n.termAt(uint64(i))
		if !ok {
			continue
		}
		if t == term {
			return uint64(i)
		}
		if t < term {
			break
		}
	}
	return 0
}

func (n *Node) advanceCommitLocked() {
	n.bumpCommitLocked(HighestCommittable(
		n.lastIndex(), n.commitIndex, n.currentTerm,
		n.termAt,
		func(index uint64) int {
			count := 1
			for _, p := range n.peers {
				if n.matchIndex[p] >= index {
					count++
				}
			}
			return count
		},
		n.majority(),
		!SkipFigure8.Load(),
	))
}
