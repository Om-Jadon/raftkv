package raft

import (
	"context"
	"time"
)

type waiter struct {
	term uint64
	ch   chan ApplyResult
}

func (n *Node) stopped() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

// Propose appends cmd on the leader and returns a buffered channel that receives
// the apply result. The waiter is registered under the same lock as the append
// so apply cannot miss it. Callers must consume or time out and CancelWait.
func (n *Node) Propose(cmd []byte) (index, term uint64, wait <-chan ApplyResult, isLeader bool) {
	if n.stopped() {
		return 0, 0, nil, false
	}
	n.mu.Lock()
	if n.role != Leader {
		term = n.currentTerm
		n.mu.Unlock()
		return 0, term, nil, false
	}
	index = n.lastIndex() + 1
	term = n.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: append([]byte(nil), cmd...),
	}
	n.appendLogLocked([]LogEntry{entry})
	ch := make(chan ApplyResult, 1)
	n.waiters[index] = waiter{term: term, ch: ch}
	n.mu.Unlock()
	n.triggerReplication()
	return index, term, ch, true
}

// CancelWait drops a waiter if it is still pending for the given term.
func (n *Node) CancelWait(index, term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w, ok := n.waiters[index]
	if ok && w.term == term {
		delete(n.waiters, index)
	}
}

// WaitApply waits for a Propose channel with a timeout.
func WaitApply(ctx context.Context, wait <-chan ApplyResult, timeout time.Duration) (ApplyResult, error) {
	if wait == nil {
		return ApplyResult{Err: ErrNotLeader}, ErrNotLeader
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r, ok := <-wait:
		if !ok {
			return ApplyResult{Err: ErrLostLeadership}, ErrLostLeadership
		}
		if r.Err != nil {
			return r, r.Err
		}
		return r, nil
	case <-timer.C:
		return ApplyResult{Err: ErrTimeout}, ErrTimeout
	case <-ctx.Done():
		return ApplyResult{Err: ctx.Err()}, ctx.Err()
	}
}

func (n *Node) notifyWaiter(index, term uint64, result any, err error) {
	n.mu.Lock()
	w, ok := n.waiters[index]
	if ok {
		delete(n.waiters, index)
	}
	n.mu.Unlock()
	if !ok {
		return
	}
	res := ApplyResult{Index: index, Term: term, Result: result}
	if w.term != term {
		res.Err = ErrLostLeadership
	} else {
		res.Err = err
	}
	select {
	case w.ch <- res:
	default:
	}
}

func (n *Node) failWaitersLocked() {
	for index, w := range n.waiters {
		delete(n.waiters, index)
		select {
		case w.ch <- ApplyResult{Index: index, Term: w.term, Err: ErrLostLeadership}:
		default:
		}
	}
}
