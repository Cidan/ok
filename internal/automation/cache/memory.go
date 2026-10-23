package cache

import (
	"container/list"
	"sync"
)

// maxMemoryBlob is the largest blob the memory tier keeps: what a build reads most is small.
const maxMemoryBlob = 1 << 20

// memory keeps the most recently used small blobs of each namespace, up to a total of bytes.
type memory struct {
	mu    sync.Mutex
	limit int64
	used  int64
	order *list.List // front: the most recently used
	byKey map[string]*list.Element
}

type entry struct {
	key  string
	data []byte
}

func newMemory(limit int64) *memory {
	return &memory{limit: limit, order: list.New(), byKey: map[string]*list.Element{}}
}

func memKey(ns, hash string) string { return ns + "\x00" + hash }

func (m *memory) has(ns, hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.byKey[memKey(ns, hash)]
	return ok
}

func (m *memory) get(ns, hash string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.byKey[memKey(ns, hash)]
	if !ok {
		return nil, false
	}
	m.order.MoveToFront(e)
	return e.Value.(*entry).data, true
}

func (m *memory) put(ns, hash string, data []byte) {
	if int64(len(data)) > maxMemoryBlob || int64(len(data)) > m.limit {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey(ns, hash)
	if e, ok := m.byKey[k]; ok {
		m.order.MoveToFront(e)
		return
	}
	m.byKey[k] = m.order.PushFront(&entry{key: k, data: append([]byte(nil), data...)})
	m.used += int64(len(data))
	for m.used > m.limit {
		last := m.order.Back()
		e := last.Value.(*entry)
		m.order.Remove(last)
		delete(m.byKey, e.key)
		m.used -= int64(len(e.data))
	}
}
