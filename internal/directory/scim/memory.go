package scim

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is a Store in memory, for tests and for trying ok out.
type Memory struct {
	mu     sync.Mutex
	users  map[string]User
	groups map[string]Group
}

// NewMemory is an empty store.
func NewMemory() *Memory { return &Memory{users: map[string]User{}, groups: map[string]Group{}} }

func (m *Memory) CreateUser(_ context.Context, u User) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range m.users {
		if strings.EqualFold(o.UserName, u.UserName) {
			return User{}, ErrConflict
		}
	}
	u.Created, u.Modified = time.Now(), time.Now()
	m.users[u.ID] = u
	return u, nil
}

func (m *Memory) ReplaceUser(_ context.Context, u User) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.users[u.ID]
	if !ok {
		return User{}, ErrNotFound
	}
	for _, o := range m.users {
		if o.ID != u.ID && strings.EqualFold(o.UserName, u.UserName) {
			return User{}, ErrConflict
		}
	}
	u.Created, u.Modified = old.Created, time.Now()
	m.users[u.ID] = u
	return u, nil
}

func (m *Memory) User(_ context.Context, id string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[id]
	if !ok {
		return User{}, ErrNotFound
	}
	return u, nil
}

func (m *Memory) Users(context.Context) ([]User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]User, 0, len(m.users))
	for _, u := range m.users {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) DeleteUser(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[id]; !ok {
		return ErrNotFound
	}
	delete(m.users, id)
	for gid, g := range m.groups {
		g.Members = minus(g.Members, []string{id})
		m.groups[gid] = g
	}
	return nil
}

func (m *Memory) CreateGroup(_ context.Context, g Group) (Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range m.groups {
		if strings.EqualFold(o.DisplayName, g.DisplayName) {
			return Group{}, ErrConflict
		}
	}
	g.Created, g.Modified = time.Now(), time.Now()
	m.groups[g.ID] = g
	return g, nil
}

func (m *Memory) ReplaceGroup(_ context.Context, g Group) (Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.groups[g.ID]
	if !ok {
		return Group{}, ErrNotFound
	}
	for _, o := range m.groups {
		if o.ID != g.ID && strings.EqualFold(o.DisplayName, g.DisplayName) {
			return Group{}, ErrConflict
		}
	}
	g.Created, g.Modified = old.Created, time.Now()
	m.groups[g.ID] = g
	return g, nil
}

func (m *Memory) Group(_ context.Context, id string) (Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[id]
	if !ok {
		return Group{}, ErrNotFound
	}
	return g, nil
}

func (m *Memory) Groups(context.Context) ([]Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) DeleteGroup(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.groups[id]; !ok {
		return ErrNotFound
	}
	delete(m.groups, id)
	for gid, g := range m.groups {
		g.Members = minus(g.Members, []string{id})
		m.groups[gid] = g
	}
	return nil
}
