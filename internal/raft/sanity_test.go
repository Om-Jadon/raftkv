package raft

import "testing"

func TestSanity(t *testing.T) {
	t.Parallel()
	if 1+1 != 2 {
		t.Fatal("sanity check failed")
	}
}
