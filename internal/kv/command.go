package kv

import (
	"encoding/json"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

const (
	OpGet    = "GET"
	OpPut    = "PUT"
	OpDelete = "DELETE"
	OpCAS    = "CAS"
	OpAppend = "APPEND"
)

// Command is a state-machine operation encoded in a Raft log entry.
type Command struct {
	Op       string
	Key      string
	Value    string
	Expected string // CAS compare value
	ClientID uint64
	SeqNum   uint64
}

// Result is returned from Apply.
type Result struct {
	Value string
	Found bool
	OK    bool // CAS success
}

func encode(cmd Command) []byte {
	b, err := json.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return b
}

func decode(raw []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(raw, &cmd)
	return cmd, err
}

var _ raft.StateMachine = (*KV)(nil)
