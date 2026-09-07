package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func TestHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := raft.NodeID(3)
	if err := s.SaveHardState(7, &id); err != nil {
		t.Fatal(err)
	}
	hs, entries, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.CurrentTerm != 7 || hs.VotedFor == nil || *hs.VotedFor != 3 {
		t.Fatalf("hardstate = %+v", hs)
	}
	if len(entries) != 1 || entries[0].Index != 0 {
		t.Fatalf("want sentinel only, got %v", entries)
	}
}

func TestLogAppendAndLoad(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	_, loaded, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 3 {
		t.Fatalf("len=%d want 3", len(loaded))
	}
	if string(loaded[1].Command) != "a" || string(loaded[2].Command) != "b" {
		t.Fatalf("loaded=%v", loaded)
	}
}

func TestTruncateFrom(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_ = s.AppendEntries([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	})
	if err := s.TruncateFrom(2); err != nil {
		t.Fatal(err)
	}
	_, loaded, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded[1].Index != 1 {
		t.Fatalf("after truncate: %v", loaded)
	}
}

func TestTornWriteTolerated(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.AppendEntries([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("keep")},
		{Term: 1, Index: 2, Command: []byte("maybe")},
	})
	_ = s.Close()

	path := filepath.Join(dir, logName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 10 {
		t.Fatalf("log too short: %d", len(data))
	}
	// Chop the last half of the second record to simulate a torn write.
	chop := len(data) - len(data)/4
	if err := os.WriteFile(path, data[:chop], 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	_, loaded, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) < 2 || string(loaded[1].Command) != "keep" {
		t.Fatalf("expected first entry kept, got %v", loaded)
	}
	for _, e := range loaded {
		if string(e.Command) == "maybe" {
			t.Fatalf("torn entry should not load: %v", loaded)
		}
	}
}

func TestSaveSnapshotCompactsLog(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.AppendEntries([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(raft.Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              []byte("snap"),
	}); err != nil {
		t.Fatal(err)
	}
	_, loaded, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range loaded {
		if e.Index > 0 && e.Index <= 2 {
			t.Fatalf("compact should drop index %d: %v", e.Index, loaded)
		}
	}
	if len(loaded) < 2 || loaded[len(loaded)-1].Index != 3 {
		t.Fatalf("want tail index 3, got %v", loaded)
	}
	snap, ok, err := s.LoadSnapshot()
	if err != nil || !ok {
		t.Fatalf("snapshot: ok=%v err=%v", ok, err)
	}
	if snap.LastIncludedIndex != 2 || string(snap.Data) != "snap" {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestReopenPreservesState(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id := raft.NodeID(2)
	_ = s.SaveHardState(5, &id)
	_ = s.AppendEntries([]raft.LogEntry{{Term: 5, Index: 1, Command: []byte("x")}})
	_ = s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	hs, log, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.CurrentTerm != 5 || hs.VotedFor == nil || *hs.VotedFor != 2 {
		t.Fatalf("hs=%+v", hs)
	}
	if len(log) != 2 || string(log[1].Command) != "x" {
		t.Fatalf("log=%v", log)
	}
}
