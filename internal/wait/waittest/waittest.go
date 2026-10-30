// Package waittest is a wait.Store in memory, on a queue in the process: for tests of what waits.
package waittest

import (
	"context"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// Store holds registrations in memory; its steps' jobs, and its inboxes, are on one LocalQueue.
type Store struct {
	mu   sync.Mutex
	regs map[string]map[string]time.Time // subject → inbox → when it lapses
	jobs work.Jobs
}

var _ wait.Store = (*Store)(nil)

// New is an empty store.
func New() *Store {
	return &Store{regs: map[string]map[string]time.Time{}, jobs: work.Jobs{Queue: bus.NewLocalQueue(), Prefix: "test"}}
}

func (s *Store) Register(ctx context.Context, inbox string, subjects []string, lapses time.Time) error {
	s.mu.Lock()
	for _, sub := range subjects {
		if s.regs[sub] == nil {
			s.regs[sub] = map[string]time.Time{}
		}
		s.regs[sub][inbox] = lapses
	}
	s.mu.Unlock()
	return s.jobs.Step(ctx, wait.LapseKind, wait.Key(inbox), lapses.Add(time.Millisecond), []byte(inbox))
}

func (s *Store) Unregister(_ context.Context, inbox string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.regs {
		delete(m, inbox)
	}
	return nil
}

func (s *Store) Waiting(_ context.Context, subjects []string, now time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, subject := range subjects {
		for in, lapses := range s.regs[subject] {
			if lapses.After(now) && !seen[in] {
				seen[in] = true
				out = append(out, in)
			}
		}
	}
	return out, nil
}

func (s *Store) Lapse(_ context.Context, inbox string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.regs {
		if l, ok := m[inbox]; ok && !l.After(now) {
			delete(m, inbox)
		}
	}
	return nil
}

func (s *Store) Jobs() work.Jobs { return s.jobs }

// Registered is how many inboxes are registered under subject.
func (s *Store) Registered(subject string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.regs[subject])
}

// Change says subject changed, as a committed transaction's step does.
func (s *Store) Change(ctx context.Context, subject string) error {
	kind, key, body := wait.Changed(subject)
	return s.jobs.Step(ctx, kind, key, time.Time{}, body)
}
