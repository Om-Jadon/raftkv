package raft

import "sync/atomic"

// SkipFigure8, when true, commits an index by replica count even if log[N].Term
// is not currentTerm. Chaos tests flip this to prove the harness catches Figure 8.
var SkipFigure8 atomic.Bool

// SkipVotePersist, when true, persists currentTerm but not votedFor.
// Chaos tests flip this to prove a node can vote twice in one term.
var SkipVotePersist atomic.Bool
