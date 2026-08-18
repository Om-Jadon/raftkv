package raft

import (
	"context"
	"time"
)

func (n *Node) persistLocked() {
	_ = n.storage.SaveHardState(n.currentTerm, n.votedFor)
}

func (n *Node) stepDownLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = nil
		n.persistLocked()
	}
	n.role = Follower
}

func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) hasMajorityLocked() bool {
	return len(n.votes) >= n.majority()
}

func (n *Node) logUpToDateLocked(lastLogIndex, lastLogTerm uint64) bool {
	myIndex := n.lastIndex()
	myTerm, ok := n.termAt(myIndex)
	if !ok {
		myTerm = 0
	}
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}

func (n *Node) heartbeatArgsLocked() AppendEntriesArgs {
	prevIndex := n.lastIndex()
	prevTerm, _ := n.termAt(prevIndex)
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}
}

func (n *Node) startElectionLocked() (rv *RequestVoteArgs, rvPeers []NodeID, hb *AppendEntriesArgs, hbPeers []NodeID) {
	n.currentTerm++
	n.role = Candidate
	self := n.id
	n.votedFor = &self
	n.persistLocked()
	n.resetElectionDeadlineLocked()
	n.votes = map[NodeID]struct{}{n.id: {}}

	lastIndex := n.lastIndex()
	lastTerm, _ := n.termAt(lastIndex)
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	peers := append([]NodeID(nil), n.peers...)
	if n.hasMajorityLocked() {
		h, hp := n.becomeLeaderLocked()
		return nil, nil, &h, hp
	}
	return &args, peers, nil, nil
}

func (n *Node) becomeLeaderLocked() (AppendEntriesArgs, []NodeID) {
	n.role = Leader
	n.lastHeartbeat = n.clock.Now()
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	return n.heartbeatArgsLocked(), append([]NodeID(nil), n.peers...)
}

func (n *Node) broadcastRequestVotes(args RequestVoteArgs, peers []NodeID) {
	for _, to := range peers {
		to := to
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, 80*time.Millisecond)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, to, &args)
			if err != nil {
				return
			}
			n.handleRequestVoteReply(to, args.Term, reply)
		}()
	}
}

func (n *Node) handleRequestVoteReply(from NodeID, sentTerm uint64, reply *RequestVoteReply) {
	var hb *AppendEntriesArgs
	var peers []NodeID
	n.mu.Lock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		n.mu.Unlock()
		return
	}
	if n.role != Candidate || n.currentTerm != sentTerm {
		n.mu.Unlock()
		return
	}
	if reply.VoteGranted {
		n.votes[from] = struct{}{}
		if n.hasMajorityLocked() {
			args, p := n.becomeLeaderLocked()
			hb, peers = &args, p
		}
	}
	n.mu.Unlock()
	if hb != nil {
		n.broadcastHeartbeats(*hb, peers)
	}
}

func (n *Node) broadcastHeartbeats(args AppendEntriesArgs, peers []NodeID) {
	for _, to := range peers {
		to := to
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, 80*time.Millisecond)
			defer cancel()
			reply, err := n.transport.SendAppendEntries(ctx, to, &args)
			if err != nil {
				return
			}
			n.handleAppendEntriesReply(args.Term, reply)
		}()
	}
}

func (n *Node) handleAppendEntriesReply(sentTerm uint64, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	_ = sentTerm
}
