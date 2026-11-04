package raft

import (
	"context"
	"time"
)

func (n *Node) persistLocked() {
	vf := n.votedFor
	if SkipVotePersist.Load() {
		vf = nil
	}
	if err := n.storage.SaveHardState(n.currentTerm, vf); err != nil {
		panic(err)
	}
}

func (n *Node) stepDownLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = nil
		n.persistLocked()
	}
	if n.leaderID == n.id {
		n.leaderID = 0
	}
	n.role = Follower
	n.failWaitersLocked()
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

func (n *Node) startElectionLocked() (rv *RequestVoteArgs, rvPeers []NodeID, becameLeader bool) {
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
		n.becomeLeaderLocked()
		return nil, nil, true
	}
	return &args, peers, false
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	n.lastHeartbeat = n.clock.Now()
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
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
	var becameLeader bool
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
			n.becomeLeaderLocked()
			becameLeader = true
		}
	}
	n.mu.Unlock()
	if becameLeader {
		n.triggerReplication()
	}
}
