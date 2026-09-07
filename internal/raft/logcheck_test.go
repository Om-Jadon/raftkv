package raft

import (
	"bytes"
	"testing"
)

func TestCheckPrevLog(t *testing.T) {
	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}
	ok, _, _ := CheckPrevLog(log, 0, 0)
	if !ok {
		t.Fatal("prevIndex 0 should match")
	}
	ok, _, _ = CheckPrevLog(log, 2, 1)
	if !ok {
		t.Fatal("matching prev should succeed")
	}
	ok, cTerm, cIndex := CheckPrevLog(log, 2, 9)
	if ok {
		t.Fatal("term mismatch should fail")
	}
	if cTerm != 1 || cIndex != 1 {
		t.Fatalf("conflict term/index = %d/%d, want 1/1", cTerm, cIndex)
	}
	ok, _, cIndex = CheckPrevLog(log, 9, 1)
	if ok {
		t.Fatal("prev past end should fail")
	}
	if cIndex != 4 {
		t.Fatalf("conflict index = %d, want last+1=4", cIndex)
	}

	compacted := []LogEntry{
		{Term: 3, Index: 5},
		{Term: 3, Index: 6, Command: []byte("d")},
	}
	ok, _, cIndex = CheckPrevLog(compacted, 0, 0)
	if ok {
		t.Fatal("prevIndex 0 must fail once the log base is a snapshot sentinel")
	}
	if cIndex != 5 {
		t.Fatalf("conflict index = %d, want first=5", cIndex)
	}
	ok, _, _ = CheckPrevLog(compacted, 5, 3)
	if !ok {
		t.Fatal("prev at the snapshot sentinel should match")
	}
}

func TestHighestCommittableFigure8(t *testing.T) {
	termAt := func(index uint64) (uint64, bool) {
		switch index {
		case 1:
			return 1, true
		case 2:
			return 2, true
		default:
			return 0, false
		}
	}
	replicas := func(index uint64) int {
		if index <= 2 {
			return 3
		}
		return 1
	}
	if got := HighestCommittable(2, 0, 2, termAt, replicas, 3, true); got != 2 {
		t.Fatalf("current-term majority should commit 2, got %d", got)
	}
	if got := HighestCommittable(1, 0, 2, termAt, replicas, 3, true); got != 0 {
		t.Fatalf("old-term majority must not commit, got %d", got)
	}
	if got := HighestCommittable(1, 0, 2, termAt, replicas, 3, false); got != 1 {
		t.Fatalf("without Figure 8 guard, old-term majority commits, got %d", got)
	}
}

func TestPlanLogMerge(t *testing.T) {
	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	trunc, add := PlanLogMerge(log, 1, []LogEntry{{Term: 2, Index: 2, Command: []byte("c")}})
	if trunc != 2 {
		t.Fatalf("truncateFrom=%d, want 2", trunc)
	}
	if len(add) != 1 || add[0].Term != 2 || !bytes.Equal(add[0].Command, []byte("c")) {
		t.Fatalf("toAppend=%v", add)
	}
	trunc, add = PlanLogMerge(log, 2, []LogEntry{{Term: 2, Index: 3, Command: []byte("d")}})
	if trunc != 0 {
		t.Fatalf("no truncate, got %d", trunc)
	}
	if len(add) != 1 || add[0].Index != 3 {
		t.Fatalf("append tail: %v", add)
	}
}

func TestLogsMatch(t *testing.T) {
	a := []LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("x")}, {Index: 2, Term: 2, Command: []byte("y")}}
	b := []LogEntry{{Index: 0}, {Index: 1, Term: 1, Command: []byte("x")}}
	if !LogsMatch(a, b) {
		t.Fatal("shared prefix of equal terms should match")
	}
	b = append(b, LogEntry{Index: 2, Term: 2, Command: []byte("nope")})
	if LogsMatch(a, b) {
		t.Fatal("same index+term with different command must not match")
	}

	sentA := []LogEntry{{Index: 5, Term: 2}, {Index: 6, Term: 2, Command: []byte("x")}}
	sentB := []LogEntry{{Index: 5, Term: 2, Command: []byte("full")}, {Index: 6, Term: 2, Command: []byte("x")}}
	if !LogsMatch(sentA, sentB) {
		t.Fatal("snapshot sentinels should match on term even if Command is empty")
	}
}
