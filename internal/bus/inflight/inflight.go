// Package inflight runs a stream's handlers, a bounded number at once, for the bus's backends
// (internal/bus): a receiver reserves free slots, receives as many deliveries — so none waits for a
// slot, its visibility running — runs a handler in a slot for each, and releases the rest.
package inflight

import (
	"context"
	"sync"
)

// Slots run handlers, n at most at once.
type Slots struct {
	n     int
	freed chan struct{}
	wg    sync.WaitGroup

	mu    sync.Mutex
	busy  int
	first error
}

// New is n slots.
func New(n int) *Slots { return &Slots{n: max(n, 1), freed: make(chan struct{}, max(n, 1))} }

// Reserve waits until a slot is free and reserves up to most of the free ones, returning how many.
func (s *Slots) Reserve(ctx context.Context, most int) (int, error) {
	for {
		s.mu.Lock()
		if f := s.n - s.busy; f > 0 {
			k := min(f, max(most, 1))
			s.busy += k
			s.mu.Unlock()
			return k, nil
		}
		s.mu.Unlock()
		select {
		case <-s.freed:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Release frees n reserved slots no handler took.
func (s *Slots) Release(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.busy -= n
	s.mu.Unlock()
	s.wake()
}

func (s *Slots) wake() {
	select {
	case s.freed <- struct{}{}:
	default:
	}
}

// Run runs fn in a reserved slot, freeing it when fn returns.
func (s *Slots) Run(fn func() error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := fn()
		s.mu.Lock()
		s.busy--
		if err != nil && s.first == nil {
			s.first = err
		}
		s.mu.Unlock()
		s.wake()
	}()
}

// Err is the first error a handler returned.
func (s *Slots) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first
}

// Wait waits for every handler to finish.
func (s *Slots) Wait() { s.wg.Wait() }
