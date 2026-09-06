package chaos

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

type event struct {
	ts  time.Time
	msg string
}

// Trace is a seed-tagged event log dumped on failure.
type Trace struct {
	seed int64
	mu   sync.Mutex
	ev   []event
}

func newTrace(seed int64) *Trace {
	return &Trace{seed: seed, ev: make([]event, 0, 256)}
}

func (t *Trace) log(msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.ev) >= 10000 {
		return
	}
	t.ev = append(t.ev, event{ts: time.Now(), msg: msg})
}

func (t *Trace) dump() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "chaos seed=%d events=%d\n", t.seed, len(t.ev))
	start := time.Time{}
	if len(t.ev) > 0 {
		start = t.ev[0].ts
	}
	for _, e := range t.ev {
		fmt.Fprintf(&b, "+%6.0fms  %s\n", e.ts.Sub(start).Seconds()*1000, e.msg)
	}
	return b.String()
}

func entryAt(log []raft.LogEntry, index uint64) (raft.LogEntry, bool) {
	for _, e := range log {
		if e.Index == index {
			return e, true
		}
	}
	return raft.LogEntry{}, false
}
