package raft

import "bytes"

// CheckPrevLog is the Figure 2 AppendEntries consistency check plus Ongaro's
// fast-backup hints. log[0] is the sentinel (or snapshot base).
func CheckPrevLog(log []LogEntry, prevIndex, prevTerm uint64) (ok bool, conflictTerm, conflictIndex uint64) {
	if len(log) == 0 {
		return false, 0, 1
	}
	last := log[len(log)-1].Index
	first := log[0].Index
	if prevIndex > last {
		return false, 0, last + 1
	}
	if prevIndex == 0 {
		return true, 0, 0
	}
	var term uint64
	found := false
	for _, e := range log {
		if e.Index == prevIndex {
			term = e.Term
			found = true
			break
		}
	}
	if !found || term != prevTerm {
		if found {
			ci := first
			for _, e := range log {
				if e.Term == term {
					ci = e.Index
					break
				}
			}
			return false, term, ci
		}
		return false, 0, first
	}
	return true, 0, 0
}

// HighestCommittable is the Figure 2 commit rule. When requireCurrentTerm is
// true (correct Raft), N must be from currentTerm — the Figure 8 restriction.
func HighestCommittable(last, commit, currentTerm uint64, termAt func(uint64) (uint64, bool), replicas func(index uint64) int, majority int, requireCurrentTerm bool) uint64 {
	for index := last; index > commit; index-- {
		term, ok := termAt(index)
		if !ok {
			continue
		}
		if requireCurrentTerm && term != currentTerm {
			continue
		}
		if replicas(index) >= majority {
			return index
		}
	}
	return commit
}

// LogsMatch reports whether two logs satisfy the Log Matching property up to
// the last index they share with equal terms.
func LogsMatch(a, b []LogEntry) bool {
	byIndex := func(log []LogEntry) map[uint64]LogEntry {
		m := make(map[uint64]LogEntry, len(log))
		for _, e := range log {
			m[e.Index] = e
		}
		return m
	}
	ma, mb := byIndex(a), byIndex(b)
	var shared []uint64
	for i, ea := range ma {
		if eb, ok := mb[i]; ok && ea.Term == eb.Term {
			shared = append(shared, i)
		}
	}
	for _, i := range shared {
		for j := uint64(0); j <= i; j++ {
			ea, oka := ma[j]
			eb, okb := mb[j]
			if !oka || !okb || ea.Term != eb.Term || !bytes.Equal(ea.Command, eb.Command) {
				return false
			}
		}
	}
	return true
}

// PlanLogMerge is the Figure 2 conflict-resolution step: after prevLog matches,
// truncate at the first conflicting index and return the entries to append.
// truncateFrom == 0 means the follower log is not truncated.
func PlanLogMerge(log []LogEntry, prevIndex uint64, entries []LogEntry) (truncateFrom uint64, toAppend []LogEntry) {
	if len(log) == 0 {
		copied := copyEntries(entries)
		return 0, copied
	}
	last := log[len(log)-1].Index
	insertAt := prevIndex + 1
	for i, ent := range entries {
		idx := insertAt + uint64(i)
		if idx <= last {
			existing, ok := termAtLog(log, idx)
			if !ok || existing != ent.Term {
				return idx, copyEntries(entries[i:])
			}
			continue
		}
		return 0, copyEntries(entries[i:])
	}
	return 0, nil
}

func termAtLog(log []LogEntry, index uint64) (uint64, bool) {
	for _, e := range log {
		if e.Index == index {
			return e.Term, true
		}
	}
	return 0, false
}

func copyEntries(entries []LogEntry) []LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: append([]byte(nil), e.Command...),
		}
	}
	return out
}
