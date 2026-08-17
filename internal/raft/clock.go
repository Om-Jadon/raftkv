package raft

import "time"

// RealClock is a Clock backed by the process wall clock.
type RealClock struct{}

// Now returns the current wall time.
func (RealClock) Now() time.Time { return time.Now() }

// After waits for duration d and then sends the current time on the returned channel.
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTicker returns a Ticker that fires every d.
func (RealClock) NewTicker(d time.Duration) Ticker {
	return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) Chan() <-chan time.Time { return t.C }
