package raft

import "testing"

func FuzzCheckPrevLog(f *testing.F) {
	f.Add([]byte{1, 1, 2}, uint64(2), uint64(1))
	f.Add([]byte{1}, uint64(0), uint64(0))
	f.Fuzz(func(t *testing.T, terms []byte, prevIndex, prevTerm uint64) {
		if len(terms) > 48 {
			terms = terms[:48]
		}
		log := []LogEntry{{Term: 0, Index: 0}}
		for i, tm := range terms {
			log = append(log, LogEntry{Term: uint64(tm % 8), Index: uint64(i) + 1})
		}
		ok, _, cIndex := CheckPrevLog(log, prevIndex, prevTerm)
		last := log[len(log)-1].Index
		if prevIndex == 0 && !ok {
			t.Fatal("prevIndex 0 must succeed on a non-empty log")
		}
		if prevIndex > last && ok {
			t.Fatal("prevIndex past last must fail")
		}
		if !ok && prevIndex > last && cIndex != last+1 {
			t.Fatalf("conflictIndex=%d want last+1=%d", cIndex, last+1)
		}
	})
}

func FuzzHighestCommittable(f *testing.F) {
	f.Add(uint64(3), uint64(0), uint64(2), true)
	f.Add(uint64(3), uint64(1), uint64(2), false)
	f.Fuzz(func(t *testing.T, last, commit, currentTerm uint64, require bool) {
		if last > 32 {
			last %= 33
		}
		if commit > last {
			commit = last
		}
		if currentTerm > 16 {
			currentTerm %= 17
		}
		termAt := func(index uint64) (uint64, bool) {
			if index == 0 || index > last {
				return 0, false
			}
			return (index % 5) + 1, true
		}
		replicas := func(index uint64) int {
			if index%2 == 0 {
				return 3
			}
			return 1
		}
		got := HighestCommittable(last, commit, currentTerm, termAt, replicas, 3, require)
		if got < commit || got > last {
			t.Fatalf("commit index %d outside [%d,%d]", got, commit, last)
		}
		if require && got > commit {
			term, ok := termAt(got)
			if !ok || term != currentTerm {
				t.Fatalf("Figure 8: committed %d term %d want current %d", got, term, currentTerm)
			}
		}
	})
}

func FuzzPlanLogMerge(f *testing.F) {
	f.Add([]byte{1, 1, 2}, []byte{2, 2}, uint64(1))
	f.Fuzz(func(t *testing.T, follower, leader []byte, prevIndex uint64) {
		if len(follower) > 32 {
			follower = follower[:32]
		}
		if len(leader) > 32 {
			leader = leader[:32]
		}
		log := []LogEntry{{Term: 0, Index: 0}}
		for i, tm := range follower {
			log = append(log, LogEntry{Term: uint64(tm%8) + 1, Index: uint64(i) + 1, Command: []byte{tm}})
		}
		if prevIndex > uint64(len(follower)) {
			prevIndex = uint64(len(follower))
		}
		entries := make([]LogEntry, len(leader))
		for i, tm := range leader {
			entries[i] = LogEntry{Term: uint64(tm%8) + 1, Index: prevIndex + 1 + uint64(i), Command: []byte{tm}}
		}
		trunc, add := PlanLogMerge(log, prevIndex, entries)
		if trunc > 0 && trunc <= prevIndex {
			t.Fatalf("truncateFrom %d must be > prevIndex %d", trunc, prevIndex)
		}
		_ = add
	})
}
