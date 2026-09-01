package raft

import (
	"context"
	"time"
)

const maxAppendBatch = 500

// Start appends a command on the leader and returns its log index and term.
func (n *Node) Start(cmd []byte) (index, term uint64, isLeader bool) {
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
	go n.replicateAll()
	return index, term, true
}

func (n *Node) appendLogLocked(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	copied := make([]LogEntry, len(entries))
	copy(copied, entries)
	n.log = append(n.log, copied...)
	_ = n.storage.AppendEntries(copied)
}

func (n *Node) truncateLogLocked(from uint64) {
	off := int(from - n.log[0].Index)
	if off <= 0 || off >= len(n.log) {
		return
	}
	n.log = append([]LogEntry(nil), n.log[:off]...)
	_ = n.storage.TruncateFrom(from)
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
	prevIndex := next - 1
	prevTerm, ok := n.termAt(prevIndex)
	if !ok {
		n.mu.Unlock()
		return
	}
	end := n.lastIndex() + 1
	if end > next+maxAppendBatch {
		end = next + maxAppendBatch
	}
	var entries []LogEntry
	for i := next; i < end; i++ {
		e, ok := n.entryAt(i)
		if !ok {
			break
		}
		entries = append(entries, LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: append([]byte(nil), e.Command...),
		})
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
	for index := n.lastIndex(); index > n.commitIndex; index-- {
		term, ok := n.termAt(index)
		if !ok || term != n.currentTerm {
			continue
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= index {
				count++
			}
		}
		if count >= n.majority() {
			n.bumpCommitLocked(index)
			return
		}
	}
}
