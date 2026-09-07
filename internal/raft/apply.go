package raft

// runApplier is the only goroutine that advances lastApplied and calls Apply.
func (n *Node) runApplier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case <-n.applyWake:
			n.applyCommitted()
			n.maybeSnapshot()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		var entry LogEntry
		n.snapMu.Lock()
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			n.snapMu.Unlock()
			return
		}
		n.lastApplied++
		got, ok := n.entryAt(n.lastApplied)
		if !ok {
			n.lastApplied--
			n.mu.Unlock()
			n.snapMu.Unlock()
			return
		}
		entry = LogEntry{
			Term:    got.Term,
			Index:   got.Index,
			Command: append([]byte(nil), got.Command...),
		}
		n.mu.Unlock()
		result, err := n.sm.Apply(entry)
		n.snapMu.Unlock()
		n.notifyWaiter(entry.Index, entry.Term, result, err)
	}
}

func (n *Node) bumpCommitLocked(index uint64) {
	if index > n.commitIndex {
		n.commitIndex = index
		select {
		case n.applyWake <- struct{}{}:
		default:
		}
	}
}
