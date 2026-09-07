package raft

import "sync"

// MemoryStorage is a volatile Storage used by unit tests that do not need durability.
type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor *NodeID
	log      []LogEntry
	snap     Snapshot
	hasSnap  bool
}

// NewMemoryStorage returns an empty in-memory log with a sentinel at index 0.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		log: []LogEntry{{Term: 0, Index: 0}},
	}
}

// SaveHardState implements Storage.
func (s *MemoryStorage) SaveHardState(term uint64, votedFor *NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

// AppendEntries implements Storage.
func (s *MemoryStorage) AppendEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	copied := make([]LogEntry, len(entries))
	copy(copied, entries)
	s.log = append(s.log, copied...)
	return nil
}

// TruncateFrom implements Storage by dropping entries with index >= index.
func (s *MemoryStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 {
		s.log = []LogEntry{{Term: 0, Index: 0}}
		return nil
	}
	kept := s.log[:1]
	for _, e := range s.log[1:] {
		if e.Index >= index {
			break
		}
		kept = append(kept, e)
	}
	s.log = kept
	return nil
}

// Load implements Storage.
func (s *MemoryStorage) Load() (HardState, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log := make([]LogEntry, len(s.log))
	copy(log, s.log)
	hs := HardState{CurrentTerm: s.term, VotedFor: s.votedFor}
	return hs, log, nil
}

// SaveSnapshot implements Storage.
func (s *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
	s.hasSnap = true
	if snap.LastIncludedIndex == 0 {
		return nil
	}
	kept := []LogEntry{{Term: snap.LastIncludedTerm, Index: snap.LastIncludedIndex}}
	for _, e := range s.log {
		if e.Index > snap.LastIncludedIndex {
			kept = append(kept, e)
		}
	}
	s.log = kept
	return nil
}

// LoadSnapshot implements Storage.
func (s *MemoryStorage) LoadSnapshot() (Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, s.hasSnap, nil
}
