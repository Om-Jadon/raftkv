package harness

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

// RecordSM records every applied command in order for test assertions.
type RecordSM struct {
	mu      sync.Mutex
	applied [][]byte
}

// Apply implements raft.StateMachine.
func (s *RecordSM) Apply(entry raft.LogEntry) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := append([]byte(nil), entry.Command...)
	s.applied = append(s.applied, cmd)
	return nil, nil
}

// Snapshot implements raft.StateMachine.
func (s *RecordSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.applied)
}

// Restore implements raft.StateMachine.
func (s *RecordSM) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(data, &s.applied)
}

// Reset clears recorded commands. Call when a node restarts so replayed applies rebuild state.
func (s *RecordSM) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = nil
}

// Applied returns a copy of applied commands.
func (s *RecordSM) Applied() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.applied))
	for i, cmd := range s.applied {
		out[i] = append([]byte(nil), cmd...)
	}
	return out
}

func bytesEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
