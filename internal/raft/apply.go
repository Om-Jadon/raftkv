package raft

// runApplier is the only goroutine that advances lastApplied and calls the state machine.
func (n *Node) runApplier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case <-n.applyWake:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		var entry LogEntry
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		entry, _ = n.entryAt(n.lastApplied)
		entry = LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: append([]byte(nil), entry.Command...),
		}
		n.mu.Unlock()

		result, err := n.sm.Apply(entry)
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
