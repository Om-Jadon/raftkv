package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/kv"
	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/test/harness"
)

type config struct {
	nodes      int
	nemesis    time.Duration
	stabilize  time.Duration
	persist    bool
	clientWait time.Duration
}

func defaultConfig() config {
	return config{
		nodes:      5,
		nemesis:    1200 * time.Millisecond,
		stabilize:  4 * time.Second,
		clientWait: 400 * time.Millisecond,
	}
}

type scenario struct {
	seed  int64
	cfg   config
	rng   *rand.Rand
	rngMu sync.Mutex
	trace *Trace
	chk   *checker

	failMu sync.Mutex
	fail   error

	kvsMu sync.Mutex
	kvs   map[raft.NodeID]*kv.KV

	modelMu sync.Mutex
	model   map[string]string
	acked   []ackedOp

	seq  uint64
	cid  uint64
	prev map[raft.NodeID]raft.DebugState
}

type ackedOp struct {
	op    string
	key   string
	value string
}

func runSeed(t *testing.T, seed int64, cfg config) error {
	s := &scenario{
		seed:  seed,
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(seed)),
		trace: newTrace(seed),
		chk:   newChecker(),
		kvs:   make(map[raft.NodeID]*kv.KV),
		model: make(map[string]string),
		cid:   1,
		prev:  make(map[raft.NodeID]raft.DebugState),
	}
	c := harness.NewClusterWith(t, cfg.nodes, harness.ClusterOptions{
		Persist: cfg.persist,
		Seed:    seed,
		NewSM: func(id raft.NodeID) raft.StateMachine {
			store := kv.New()
			s.kvsMu.Lock()
			s.kvs[id] = store
			s.kvsMu.Unlock()
			return store
		},
	})
	defer c.Shutdown()

	c.Network().SetHook(func(kind string, from, to raft.NodeID, err error) {
		msg := fmt.Sprintf("rpc %s %d->%d", kind, from, to)
		if err != nil {
			msg += " " + err.Error()
		}
		s.trace.log(msg)
	})

	s.trace.log(fmt.Sprintf("start nodes=%d persist=%v", cfg.nodes, cfg.persist))
	if err := s.waitLeader(c, 3*time.Second); err != nil {
		return s.wrap(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.writer(c, stop)
	}()
	go func() {
		defer wg.Done()
		s.checkerLoop(c, stop)
	}()

	s.nemesis(c, cfg.nemesis)
	close(stop)
	wg.Wait()

	if err := s.err(); err != nil {
		return s.wrap(err)
	}

	s.trace.log("heal")
	c.Heal()
	if err := s.waitLeader(c, 4*time.Second); err != nil {
		return s.wrap(err)
	}
	if err := s.settle(c); err != nil {
		return s.wrap(err)
	}
	if err := s.waitReady(c, cfg.stabilize); err != nil {
		return s.wrap(err)
	}
	s.trace.log("ok")
	return nil
}

func (s *scenario) wrap(err error) error {
	return fmt.Errorf("%v\n%s", err, s.trace.dump())
}

func (s *scenario) setErr(err error) {
	if err == nil {
		return
	}
	s.failMu.Lock()
	defer s.failMu.Unlock()
	if s.fail == nil {
		s.fail = err
		s.trace.log("FAIL " + err.Error())
	}
}

func (s *scenario) err() error {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	return s.fail
}

func (s *scenario) intn(n int) int {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	if n <= 0 {
		return 0
	}
	return s.rng.Intn(n)
}

func (s *scenario) float64() float64 {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Float64()
}

func (s *scenario) waitLeader(c *harness.Cluster, d time.Duration) error {
	deadline := time.Now().Add(d)
	var stable raft.NodeID
	var stableSince time.Time
	for time.Now().Before(deadline) {
		if err := s.snapshot(c); err != nil {
			return err
		}
		var leaders []raft.NodeID
		for _, st := range c.DebugStates() {
			if st.Role == raft.Leader {
				leaders = append(leaders, st.ID)
			}
		}
		if len(leaders) == 1 {
			if leaders[0] == stable && !stableSince.IsZero() {
				if time.Since(stableSince) >= 150*time.Millisecond {
					s.trace.log(fmt.Sprintf("leader=%d", leaders[0]))
					return nil
				}
			} else {
				stable = leaders[0]
				stableSince = time.Now()
			}
		} else {
			stable = 0
			stableSince = time.Time{}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("no unique leader")
}

func (s *scenario) snapshot(c *harness.Cluster) error {
	states := c.DebugStates()
	s.observeRoles(states)
	if err := s.chk.check(states); err != nil {
		return err
	}
	return nil
}

func (s *scenario) observeRoles(states []raft.DebugState) {
	for _, st := range states {
		prev, ok := s.prev[st.ID]
		if !ok || prev.Role != st.Role || prev.Term != st.Term || prev.CommitIndex != st.CommitIndex {
			s.trace.log(fmt.Sprintf("node %d role=%s term=%d commit=%d applied=%d entries=%d",
				st.ID, st.Role, st.Term, st.CommitIndex, st.LastApplied, len(st.Log)-1))
			s.prev[st.ID] = st
		}
	}
}

func (s *scenario) checkerLoop(c *harness.Cluster, stop <-chan struct{}) {
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			if s.err() != nil {
				return
			}
			if err := s.snapshot(c); err != nil {
				s.setErr(err)
				return
			}
		}
	}
}

func (s *scenario) writer(c *harness.Cluster, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if s.err() != nil {
			return
		}
		s.oneClientOp(c)
		time.Sleep(time.Duration(8+s.intn(25)) * time.Millisecond)
	}
}

func (s *scenario) settle(c *harness.Cluster) error {
	// A new-term entry must commit after heal so Figure 8 can advance commitIndex
	// on a leader whose log is recovered but whose commitIndex is still 0.
	deadline := time.Now().Add(3 * time.Second)
	acks := 0
	for time.Now().Before(deadline) && acks < 3 {
		if err := s.err(); err != nil {
			return err
		}
		before := s.ackCount()
		s.oneClientOp(c)
		if s.ackCount() > before {
			acks++
			continue
		}
		time.Sleep(20 * time.Millisecond)
	}
	if acks == 0 && len(s.chk.committed) > 0 {
		return fmt.Errorf("could not commit a new-term entry after heal")
	}
	return nil
}

func (s *scenario) ackCount() int {
	s.modelMu.Lock()
	defer s.modelMu.Unlock()
	return len(s.acked)
}

func (s *scenario) oneClientOp(c *harness.Cluster) {
	var leader *raft.Node
	for id := raft.NodeID(1); id <= raft.NodeID(c.Len()); id++ {
		n := c.TryNode(id)
		if n == nil {
			continue
		}
		role, _ := n.RoleAndTerm()
		if role == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		return
	}

	s.seq++
	seq := s.seq
	doGet := seq%5 == 0
	var cmd kv.Command
	s.modelMu.Lock()
	var getKey string
	if doGet && len(s.acked) > 0 {
		getKey = s.acked[s.intn(len(s.acked))].key
		cmd = kv.Command{Op: kv.OpGet, Key: getKey, ClientID: s.cid, SeqNum: seq}
	} else {
		doGet = false
		cmd = kv.Command{
			Op:       kv.OpPut,
			Key:      fmt.Sprintf("k-%d-%d", s.seed, seq),
			Value:    fmt.Sprintf("v-%d", seq),
			ClientID: s.cid,
			SeqNum:   seq,
		}
	}
	s.modelMu.Unlock()

	raw, err := json.Marshal(cmd)
	if err != nil {
		s.setErr(err)
		return
	}
	index, term, wait, isLeader := leader.Propose(raw)
	if !isLeader {
		leader.CancelWait(index, term)
		return
	}
	res, err := raft.WaitApply(context.Background(), wait, s.cfg.clientWait)
	if err != nil {
		leader.CancelWait(index, term)
		return
	}
	kr, ok := res.Result.(kv.Result)
	if !ok {
		s.setErr(fmt.Errorf("apply result type %T", res.Result))
		return
	}

	s.modelMu.Lock()
	defer s.modelMu.Unlock()
	switch cmd.Op {
	case kv.OpPut:
		s.model[cmd.Key] = cmd.Value
		s.acked = append(s.acked, ackedOp{op: cmd.Op, key: cmd.Key, value: cmd.Value})
		s.trace.log(fmt.Sprintf("ack PUT %s=%s idx=%d", cmd.Key, cmd.Value, res.Index))
	case kv.OpGet:
		want, exists := s.model[cmd.Key]
		if exists {
			if !kr.Found || kr.Value != want {
				s.setErr(fmt.Errorf("reference model: GET %s got found=%v %q want %q", cmd.Key, kr.Found, kr.Value, want))
			}
		}
		s.trace.log(fmt.Sprintf("ack GET %s=%q idx=%d", cmd.Key, kr.Value, res.Index))
	}
}

func (s *scenario) nemesis(c *harness.Cluster, d time.Duration) {
	n := c.Len()
	deadline := time.Now().Add(d)
	partitioned := make(map[raft.NodeID]time.Time)
	net := c.Network()
	for time.Now().Before(deadline) {
		if s.err() != nil {
			return
		}
		now := time.Now()
		for id, until := range partitioned {
			if now.After(until) {
				c.Reconnect(id)
				delete(partitioned, id)
				s.trace.log(fmt.Sprintf("expire isolate %d", id))
			}
		}

		id := raft.NodeID(s.intn(n) + 1)
		switch s.intn(8) {
		case 0:
			p := s.float64() * 0.35
			net.SetDropRate(p)
			s.trace.log(fmt.Sprintf("drop=%.2f", p))
		case 1:
			max := time.Duration(s.intn(450)) * time.Millisecond
			net.SetDelayRange(max)
			s.trace.log(fmt.Sprintf("delay<=%s", max))
		case 2:
			p := s.float64() * 0.25
			net.SetDupRate(p)
			s.trace.log(fmt.Sprintf("dup=%.2f", p))
		case 3:
			if len(partitioned) < n/2 {
				until := now.Add(time.Duration(120+s.intn(280)) * time.Millisecond)
				c.Disconnect(id)
				partitioned[id] = until
				s.trace.log(fmt.Sprintf("isolate %d until +%s", id, time.Until(until).Round(time.Millisecond)))
			}
		case 4:
			from := id
			to := raft.NodeID(s.intn(n) + 1)
			if from != to {
				net.Block(from, to)
				s.trace.log(fmt.Sprintf("block %d->%d", from, to))
			}
		case 5:
			s.trace.log(fmt.Sprintf("crash %d", id))
			c.Crash(id)
			time.Sleep(time.Duration(15+s.intn(60)) * time.Millisecond)
			c.Restart(id)
			s.trace.log(fmt.Sprintf("restart %d", id))
			delete(partitioned, id)
		case 6:
			net.SetDropRate(0)
			net.SetDelayRange(0)
			net.SetDupRate(0)
			s.trace.log("clear rates")
		case 7:
			c.Reconnect(id)
			delete(partitioned, id)
			s.trace.log(fmt.Sprintf("reconnect %d", id))
		}
		time.Sleep(time.Duration(25+s.intn(40)) * time.Millisecond)
	}
}

func (s *scenario) waitReady(c *harness.Cluster, d time.Duration) error {
	deadline := time.Now().Add(d)
	var last []raft.DebugState
	for time.Now().Before(deadline) {
		states := c.DebugStates()
		last = states
		s.observeRoles(states)
		if err := s.chk.check(states); err != nil {
			return err
		}
		if len(states) < s.cfg.nodes {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err := s.chk.durabilityEverywhere(states); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		need := s.maxCommitted()
		caught := true
		for _, st := range states {
			if st.LastApplied < need {
				caught = false
				break
			}
		}
		if !caught {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err := s.checkModel(c); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("nodes did not catch up before %s: %+v", d, summarize(last))
}

func (s *scenario) maxCommitted() uint64 {
	var need uint64
	for i := range s.chk.committed {
		if i > need {
			need = i
		}
	}
	return need
}

func (s *scenario) checkModel(c *harness.Cluster) error {
	s.modelMu.Lock()
	acked := append([]ackedOp(nil), s.acked...)
	s.modelMu.Unlock()

	s.kvsMu.Lock()
	defer s.kvsMu.Unlock()
	for id := raft.NodeID(1); id <= raft.NodeID(c.Len()); id++ {
		store, ok := s.kvs[id]
		if !ok || store == nil {
			return fmt.Errorf("reference model: missing kv on node %d", id)
		}
		got := store.Clone()
		for _, op := range acked {
			if op.op != kv.OpPut {
				continue
			}
			v, exists := got[op.key]
			if !exists || v != op.value {
				return fmt.Errorf("reference model: node %d key %s got %q exists=%v want %q", id, op.key, v, exists, op.value)
			}
		}
	}
	return nil
}

func summarize(states []raft.DebugState) string {
	var b []byte
	for _, st := range states {
		b = append(b, fmt.Sprintf(" [%d %s term=%d commit=%d applied=%d]", st.ID, st.Role, st.Term, st.CommitIndex, st.LastApplied)...)
	}
	return string(b)
}
