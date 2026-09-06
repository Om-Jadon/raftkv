package raftpb

import "github.com/Om-Jadon/raftkv/internal/raft"

func ToVoteArgs(in *RequestVoteRequest) *raft.RequestVoteArgs {
	if in == nil {
		return &raft.RequestVoteArgs{}
	}
	return &raft.RequestVoteArgs{
		Term:         in.Term,
		CandidateID:  raft.NodeID(in.CandidateId),
		LastLogIndex: in.LastLogIndex,
		LastLogTerm:  in.LastLogTerm,
	}
}

func FromVoteReply(in *raft.RequestVoteReply) *RequestVoteResponse {
	if in == nil {
		return &RequestVoteResponse{}
	}
	return &RequestVoteResponse{Term: in.Term, VoteGranted: in.VoteGranted}
}

func ToVoteReply(in *RequestVoteResponse) *raft.RequestVoteReply {
	if in == nil {
		return &raft.RequestVoteReply{}
	}
	return &raft.RequestVoteReply{Term: in.Term, VoteGranted: in.VoteGranted}
}

func FromVoteArgs(in *raft.RequestVoteArgs) *RequestVoteRequest {
	if in == nil {
		return &RequestVoteRequest{}
	}
	return &RequestVoteRequest{
		Term:         in.Term,
		CandidateId:  uint64(in.CandidateID),
		LastLogIndex: in.LastLogIndex,
		LastLogTerm:  in.LastLogTerm,
	}
}

func ToAppendArgs(in *AppendEntriesRequest) *raft.AppendEntriesArgs {
	if in == nil {
		return &raft.AppendEntriesArgs{}
	}
	entries := make([]raft.LogEntry, 0, len(in.Entries))
	for _, e := range in.Entries {
		if e == nil {
			continue
		}
		entries = append(entries, raft.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: append([]byte(nil), e.Command...),
		})
	}
	return &raft.AppendEntriesArgs{
		Term:         in.Term,
		LeaderID:     raft.NodeID(in.LeaderId),
		PrevLogIndex: in.PrevLogIndex,
		PrevLogTerm:  in.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: in.LeaderCommit,
	}
}

func FromAppendArgs(in *raft.AppendEntriesArgs) *AppendEntriesRequest {
	if in == nil {
		return &AppendEntriesRequest{}
	}
	entries := make([]*LogEntry, 0, len(in.Entries))
	for _, e := range in.Entries {
		entries = append(entries, &LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: append([]byte(nil), e.Command...),
		})
	}
	return &AppendEntriesRequest{
		Term:         in.Term,
		LeaderId:     uint64(in.LeaderID),
		PrevLogIndex: in.PrevLogIndex,
		PrevLogTerm:  in.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: in.LeaderCommit,
	}
}

func ToAppendReply(in *AppendEntriesResponse) *raft.AppendEntriesReply {
	if in == nil {
		return &raft.AppendEntriesReply{}
	}
	return &raft.AppendEntriesReply{
		Term:          in.Term,
		Success:       in.Success,
		ConflictTerm:  in.ConflictTerm,
		ConflictIndex: in.ConflictIndex,
	}
}

func FromAppendReply(in *raft.AppendEntriesReply) *AppendEntriesResponse {
	if in == nil {
		return &AppendEntriesResponse{}
	}
	return &AppendEntriesResponse{
		Term:          in.Term,
		Success:       in.Success,
		ConflictTerm:  in.ConflictTerm,
		ConflictIndex: in.ConflictIndex,
	}
}

func ToSnapArgs(in *InstallSnapshotRequest) *raft.InstallSnapshotArgs {
	if in == nil {
		return &raft.InstallSnapshotArgs{}
	}
	return &raft.InstallSnapshotArgs{
		Term:              in.Term,
		LeaderID:          raft.NodeID(in.LeaderId),
		LastIncludedIndex: in.LastIncludedIndex,
		LastIncludedTerm:  in.LastIncludedTerm,
		Data:              append([]byte(nil), in.Data...),
		Done:              in.Done,
	}
}

func FromSnapArgs(in *raft.InstallSnapshotArgs) *InstallSnapshotRequest {
	if in == nil {
		return &InstallSnapshotRequest{}
	}
	return &InstallSnapshotRequest{
		Term:              in.Term,
		LeaderId:          uint64(in.LeaderID),
		LastIncludedIndex: in.LastIncludedIndex,
		LastIncludedTerm:  in.LastIncludedTerm,
		Data:              append([]byte(nil), in.Data...),
		Done:              in.Done,
	}
}

func ToSnapReply(in *InstallSnapshotResponse) *raft.InstallSnapshotReply {
	if in == nil {
		return &raft.InstallSnapshotReply{}
	}
	return &raft.InstallSnapshotReply{Term: in.Term}
}

func FromSnapReply(in *raft.InstallSnapshotReply) *InstallSnapshotResponse {
	if in == nil {
		return &InstallSnapshotResponse{}
	}
	return &InstallSnapshotResponse{Term: in.Term}
}
