package secretstore

import (
	"bytes"
	"context"
	"sync"
)

// Memory is a store in memory: for tests, and the conformance suite's reference.
type Memory struct {
	mu sync.Mutex
	m  map[string][]byte
}

// NewMemory is an empty store in memory.
func NewMemory() *Memory { return &Memory{m: map[string][]byte{}} }

func (s *Memory) Put(_ context.Context, key string, sealed []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.m[key]; ok {
		if !bytes.Equal(have, sealed) {
			return ErrConflict
		}
		return nil
	}
	s.m[key] = bytes.Clone(sealed)
	return nil
}

func (s *Memory) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(v), nil
}

func (s *Memory) Delete(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.m, k)
	}
	return nil
}

// Len is how many keys the store holds, for tests.
func (s *Memory) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
