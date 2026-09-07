package chaos

import (
	"bytes"
	"fmt"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

type checker struct {
	leaderOf   map[uint64]raft.NodeID
	committed  map[uint64]raft.LogEntry
	leaderSnap map[raft.NodeID]leaderView
}

type leaderView struct {
	term uint64
	log  []raft.LogEntry
}

func newChecker() *checker {
	return &checker{
		leaderOf:   make(map[uint64]raft.NodeID),
		committed:  make(map[uint64]raft.LogEntry),
		leaderSnap: make(map[raft.NodeID]leaderView),
	}
}

func (c *checker) check(states []raft.DebugState) error {
	if err := c.electionSafety(states); err != nil {
		return err
	}
	if err := c.observeCommits(states); err != nil {
		return err
	}
	if err := c.leaderAppendOnly(states); err != nil {
		return err
	}
	if err := c.logMatching(states); err != nil {
		return err
	}
	if err := c.leaderCompleteness(states); err != nil {
		return err
	}
	return c.stateMachineSafety(states)
}

func (c *checker) electionSafety(states []raft.DebugState) error {
	byTerm := make(map[uint64][]raft.NodeID)
	for _, s := range states {
		if s.Role != raft.Leader {
			continue
		}
		byTerm[s.Term] = append(byTerm[s.Term], s.ID)
		if prev, ok := c.leaderOf[s.Term]; ok && prev != s.ID {
			return fmt.Errorf("election safety: term %d had leader %d, now %d", s.Term, prev, s.ID)
		}
		c.leaderOf[s.Term] = s.ID
	}
	for term, ids := range byTerm {
		if len(ids) > 1 {
			return fmt.Errorf("election safety: leaders %v share term %d", ids, term)
		}
	}
	return nil
}

func (c *checker) observeCommits(states []raft.DebugState) error {
	for _, s := range states {
		if len(s.Log) == 0 {
			continue
		}
		first := s.Log[0].Index
		for i := uint64(1); i <= s.CommitIndex; i++ {
			e, ok := entryAt(s.Log, i)
			if !ok {
				continue
			}
			if old, exists := c.committed[i]; exists {
				if old.Term != e.Term {
					return fmt.Errorf("durability: index %d committed as term=%d %q then term=%d %q (node %d)",
						i, old.Term, old.Command, e.Term, e.Command, s.ID)
				}
				if i != first && len(old.Command) > 0 && len(e.Command) > 0 && !bytes.Equal(old.Command, e.Command) {
					return fmt.Errorf("durability: index %d committed as term=%d %q then term=%d %q (node %d)",
						i, old.Term, old.Command, e.Term, e.Command, s.ID)
				}
				if i != first && len(old.Command) == 0 && len(e.Command) > 0 {
					c.committed[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: append([]byte(nil), e.Command...)}
				}
				continue
			}
			cmd := append([]byte(nil), e.Command...)
			if i == first {
				cmd = nil
			}
			c.committed[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: cmd}
		}
	}
	return nil
}

func (c *checker) covered(s raft.DebugState, i uint64, e raft.LogEntry) bool {
	if len(s.Log) == 0 {
		return false
	}
	first := s.Log[0].Index
	if i < first {
		return true
	}
	got, ok := entryAt(s.Log, i)
	if !ok {
		return false
	}
	if got.Term != e.Term {
		return false
	}
	if i == first || len(e.Command) == 0 || len(got.Command) == 0 {
		return true
	}
	return bytes.Equal(got.Command, e.Command)
}

func lastIdx(log []raft.LogEntry) uint64 {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Index
}

func (c *checker) leaderAppendOnly(states []raft.DebugState) error {
	for _, s := range states {
		if s.Role != raft.Leader {
			continue
		}
		prev, ok := c.leaderSnap[s.ID]
		if ok && prev.term == s.Term {
			if lastIdx(s.Log) < lastIdx(prev.log) {
				return fmt.Errorf("leader append-only: node %d term %d last index shrank %d -> %d",
					s.ID, s.Term, lastIdx(prev.log), lastIdx(s.Log))
			}
			prevFirst, newFirst := uint64(0), uint64(0)
			if len(prev.log) > 0 {
				prevFirst = prev.log[0].Index
			}
			if len(s.Log) > 0 {
				newFirst = s.Log[0].Index
			}
			for _, e := range s.Log {
				pe, ok := entryAt(prev.log, e.Index)
				if !ok {
					continue
				}
				if pe.Term != e.Term || pe.Index != e.Index {
					return fmt.Errorf("leader append-only: node %d overwrote index %d in term %d", s.ID, e.Index, s.Term)
				}
				if e.Index == prevFirst || e.Index == newFirst {
					continue
				}
				if !bytes.Equal(pe.Command, e.Command) {
					return fmt.Errorf("leader append-only: node %d overwrote index %d in term %d", s.ID, e.Index, s.Term)
				}
			}
		}
		cp := make([]raft.LogEntry, len(s.Log))
		copy(cp, s.Log)
		c.leaderSnap[s.ID] = leaderView{term: s.Term, log: cp}
	}
	return nil
}

func (c *checker) logMatching(states []raft.DebugState) error {
	for i := 0; i < len(states); i++ {
		for j := i + 1; j < len(states); j++ {
			if !raft.LogsMatch(states[i].Log, states[j].Log) {
				return fmt.Errorf("log matching: nodes %d and %d", states[i].ID, states[j].ID)
			}
		}
	}
	return nil
}

func (c *checker) leaderCompleteness(states []raft.DebugState) error {
	for _, s := range states {
		if s.Role != raft.Leader {
			continue
		}
		for i, e := range c.committed {
			// Paper: committed in term T ⇒ present on leaders of terms > T.
			// A partitioned old leader must not be required to have newer commits.
			if e.Term >= s.Term {
				continue
			}
			if !c.covered(s, i, e) {
				return fmt.Errorf("leader completeness: leader %d missing committed index %d", s.ID, i)
			}
		}
	}
	return nil
}

func (c *checker) stateMachineSafety(states []raft.DebugState) error {
	applied := make(map[uint64][]byte)
	from := make(map[uint64]raft.NodeID)
	for _, s := range states {
		if len(s.Log) == 0 {
			continue
		}
		first := s.Log[0].Index
		for i := uint64(1); i <= s.LastApplied; i++ {
			e, ok := entryAt(s.Log, i)
			if !ok || i == first {
				continue
			}
			if old, exists := applied[i]; exists && !bytes.Equal(old, e.Command) {
				return fmt.Errorf("state machine safety: index %d node %d %q vs node %d %q",
					i, from[i], old, s.ID, e.Command)
			}
			applied[i] = append([]byte(nil), e.Command...)
			from[i] = s.ID
		}
	}
	return nil
}

func (c *checker) durabilityEverywhere(states []raft.DebugState) error {
	for _, s := range states {
		for i, e := range c.committed {
			if !c.covered(s, i, e) {
				return fmt.Errorf("durability: node %d missing committed index %d after heal", s.ID, i)
			}
		}
	}
	return nil
}
