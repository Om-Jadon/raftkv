package kv

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

type session struct {
	LastSeq uint64 `json:"lastSeq"`
	Result  Result `json:"result"`
}

type snapState struct {
	Data     map[string]string
	Sessions map[string]session
}

// KV is a linearizable key-value state machine.
type KV struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[uint64]session
}

// New returns an empty KV store.
func New() *KV {
	return &KV{
		data:     make(map[string]string),
		sessions: make(map[uint64]session),
	}
}

// Apply implements raft.StateMachine.
func (kv *KV) Apply(entry raft.LogEntry) (any, error) {
	cmd, err := decode(entry.Command)
	if err != nil {
		return Result{}, err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.SeqNum > 0 {
		if s, ok := kv.sessions[cmd.ClientID]; ok && cmd.SeqNum <= s.LastSeq {
			return s.Result, nil
		}
	}

	res := kv.applyLocked(cmd)
	if cmd.SeqNum > 0 {
		kv.sessions[cmd.ClientID] = session{LastSeq: cmd.SeqNum, Result: res}
	}
	return res, nil
}

func (kv *KV) applyLocked(cmd Command) Result {
	switch cmd.Op {
	case OpGet:
		v, ok := kv.data[cmd.Key]
		return Result{Value: v, Found: ok, OK: ok}
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
		return Result{Value: cmd.Value, Found: true, OK: true}
	case OpDelete:
		_, ok := kv.data[cmd.Key]
		delete(kv.data, cmd.Key)
		return Result{Found: ok, OK: true}
	case OpAppend:
		kv.data[cmd.Key] += cmd.Value
		return Result{Value: kv.data[cmd.Key], Found: true, OK: true}
	case OpCAS:
		cur, ok := kv.data[cmd.Key]
		if ok && cur == cmd.Expected {
			kv.data[cmd.Key] = cmd.Value
			return Result{Value: cmd.Value, Found: true, OK: true}
		}
		return Result{Value: cur, Found: ok, OK: false}
	default:
		return Result{}
	}
}

// GetLocal returns the in-memory value. Test helper only — not a linearizable read.
func (kv *KV) GetLocal(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[key]
	return v, ok
}

// Clone returns a copy of the map for tests.
func (kv *KV) Clone() map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	out := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		out[k] = v
	}
	return out
}

// Snapshot implements raft.StateMachine.
func (kv *KV) Snapshot() ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	st := snapState{
		Data:     kv.data,
		Sessions: make(map[string]session, len(kv.sessions)),
	}
	for id, s := range kv.sessions {
		st.Sessions[strconv.FormatUint(id, 10)] = s
	}
	return json.Marshal(st)
}

// Restore implements raft.StateMachine.
func (kv *KV) Restore(data []byte) error {
	var st snapState
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if st.Data == nil {
		st.Data = make(map[string]string)
	}
	kv.data = st.Data
	kv.sessions = make(map[uint64]session, len(st.Sessions))
	for k, s := range st.Sessions {
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			continue
		}
		kv.sessions[id] = s
	}
	return nil
}
