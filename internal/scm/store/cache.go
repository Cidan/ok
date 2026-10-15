package store

import (
	"container/list"
	"sync"

	"github.com/Cidan/ok/internal/serve/metrics"
)

// How each cache is doing: reads that found their entry, entries admitted and to which queue,
// promotions out of the small queue, evictions, and the bytes each queue holds.
var (
	cacheLookups = metrics.NewCounter("ok_scm_cache_lookups_total",
		"Reads of a cache, by whether the entry was there.", "cache", "result")
	cacheAdmissions = metrics.NewCounter("ok_scm_cache_admissions_total",
		"Entries put in a cache, by the queue they joined: small, or main (remembered by the ghost queue, or while the cache fills).", "cache", "queue")
	cacheRefusals = metrics.NewCounter("ok_scm_cache_refusals_total",
		"Entries too large for a cache to take.", "cache")
	cachePromotions = metrics.NewCounter("ok_scm_cache_promotions_total",
		"Entries moved from a cache's small queue to its main queue, read twice there.", "cache")
	cacheEvictions = metrics.NewCounter("ok_scm_cache_evictions_total",
		"Entries a cache dropped, by the queue they left.", "cache", "queue")
	cacheBytes = metrics.NewGauge("ok_scm_cache_bytes",
		"Bytes of values a cache holds, by queue.", "cache", "queue")
)

// cacheSeries are one cache's series.
type cacheSeries struct {
	hit, miss, refused, promoted metrics.CounterSeries
	admittedSmall, admittedMain  metrics.CounterSeries
	evictedSmall, evictedMain    metrics.CounterSeries
	smallBytes, mainBytes        metrics.GaugeSeries
}

func newCacheSeries(name string) cacheSeries {
	return cacheSeries{
		hit:           cacheLookups.With(name, "hit"),
		miss:          cacheLookups.With(name, "miss"),
		refused:       cacheRefusals.With(name),
		promoted:      cachePromotions.With(name),
		admittedSmall: cacheAdmissions.With(name, "small"),
		admittedMain:  cacheAdmissions.With(name, "main"),
		evictedSmall:  cacheEvictions.With(name, "small"),
		evictedMain:   cacheEvictions.With(name, "main"),
		smallBytes:    cacheBytes.With(name, "small"),
		mainBytes:     cacheBytes.With(name, "main"),
	}
}

// cache holds immutable values under a total size bound — each value's size is given when it is
// put — evicting with S3-FIFO (Yang et
// al., "FIFO Queues are All You Need for Cache Eviction", SOSP 2023), as libCacheSim's reference
// implementation does by default:
//
//   - A new entry joins a small FIFO queue, a tenth of the budget. When it reaches the tail, it
//     moves to the main queue if it was read at least twice meanwhile; otherwise it leaves, and a
//     ghost queue remembers its key (keys only, up to nine tenths of the budget in the sizes of the
//     values they stood for).
//   - An entry whose key the ghost queue remembers joins the main queue directly.
//   - The main queue evicts like CLOCK: an entry at the tail that was read since it last passed
//     goes back to the head with its two-bit count lowered; one that was not leaves.
//
// So a scan — a clone, an export — passes through the small queue without displacing what is read
// repeatedly, which a least-recently-used cache cannot avoid. Values are content-addressed, so an
// entry is never stale.
type cache[V any] struct {
	mu       sync.Mutex
	max      int // bytes of values held, small and main together
	smallMax int
	ghostMax int // bytes the ghost queue's keys stood for
	// small and main hold *cacheEntry[V], ghost *ghostEntry; each list's front is its head.
	small, main, ghost    *list.List
	smallBytes, mainBytes int
	ghostBytes            int
	items                 map[string]*list.Element // entries in small or main
	ghosts                map[string]*list.Element
	// evicted is set by the first eviction: until then, once the small queue is full, new entries
	// join the main queue, so a cold cache fills all of its budget.
	evicted bool
	m       cacheSeries
}

type cacheEntry[V any] struct {
	key   string
	value V
	size  int
	freq  uint8 // reads since it joined its queue (small) or last passed the tail (main), at most 3
}

type ghostEntry struct {
	key  string
	size int
}

// promoteReads is how many reads in the small queue move an entry to the main queue.
const promoteReads = 2

// newCache returns a cache of at most maxBytes, reporting itself as name.
func newCache[V any](name string, maxBytes int) *cache[V] {
	return &cache[V]{
		m:        newCacheSeries(name),
		max:      maxBytes,
		smallMax: maxBytes / 10,
		ghostMax: maxBytes - maxBytes/10,
		small:    list.New(),
		main:     list.New(),
		ghost:    list.New(),
		items:    map[string]*list.Element{},
		ghosts:   map[string]*list.Element{},
	}
}

func (c *cache[V]) get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		c.m.miss.Inc()
		var zero V
		return zero, false
	}
	c.m.hit.Inc()
	ent := e.Value.(*cacheEntry[V])
	if ent.freq < 3 {
		ent.freq++
	}
	return ent.value, true
}

func (c *cache[V]) put(key string, value V, size int) {
	if size > c.max {
		c.m.refused.Inc()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		return
	}
	g, ghostHit := c.ghosts[key]
	if !ghostHit && size >= c.smallMax {
		// It could only fill the small queue and push everything else out of it.
		c.m.refused.Inc()
		return
	}
	if ghostHit {
		// Taken out before evicting, which may trim the ghost queue.
		c.ghost.Remove(g)
		delete(c.ghosts, key)
		c.ghostBytes -= g.Value.(*ghostEntry).size
	}
	for c.smallBytes+c.mainBytes+size > c.max {
		c.evict()
	}
	ent := &cacheEntry[V]{key: key, value: value, size: size}
	switch {
	case ghostHit:
		c.toMain(ent)
	case !c.evicted && c.smallBytes >= c.smallMax:
		c.toMain(ent)
	default:
		c.items[key] = c.small.PushFront(ent)
		c.smallBytes += size
		c.m.admittedSmall.Inc()
		c.m.smallBytes.Add(float64(size))
		return
	}
	c.m.admittedMain.Inc()
}

func (c *cache[V]) toMain(ent *cacheEntry[V]) {
	c.items[ent.key] = c.main.PushFront(ent)
	c.mainBytes += ent.size
	c.m.mainBytes.Add(float64(ent.size))
}

// evict makes room: from the main queue when it is over its share or the small queue is empty,
// otherwise from the small queue.
func (c *cache[V]) evict() {
	c.evicted = true
	if c.mainBytes > c.max-c.smallMax || c.smallBytes == 0 {
		c.evictMain()
	} else {
		c.evictSmall()
	}
}

// evictSmall moves entries from the small queue's tail to the main queue until one read fewer
// than promoteReads times leaves, its key kept in the ghost queue.
func (c *cache[V]) evictSmall() {
	for c.small.Len() > 0 {
		e := c.small.Back()
		ent := e.Value.(*cacheEntry[V])
		c.small.Remove(e)
		c.smallBytes -= ent.size
		c.m.smallBytes.Add(-float64(ent.size))
		if ent.freq >= promoteReads {
			ent.freq = 0
			c.toMain(ent)
			c.m.promoted.Inc()
			continue
		}
		delete(c.items, ent.key)
		c.remember(ent.key, ent.size)
		c.m.evictedSmall.Inc()
		return
	}
}

// evictMain removes the first entry from the main queue's tail not read since it last passed,
// sending read ones back to the head with one read fewer.
func (c *cache[V]) evictMain() {
	for c.main.Len() > 0 {
		e := c.main.Back()
		ent := e.Value.(*cacheEntry[V])
		if ent.freq > 0 {
			ent.freq--
			c.main.MoveToFront(e)
			continue
		}
		c.main.Remove(e)
		c.mainBytes -= ent.size
		delete(c.items, ent.key)
		c.m.mainBytes.Add(-float64(ent.size))
		c.m.evictedMain.Inc()
		return
	}
}

// remember keeps key in the ghost queue, forgetting the oldest keys beyond its bound.
func (c *cache[V]) remember(key string, size int) {
	c.ghosts[key] = c.ghost.PushFront(&ghostEntry{key: key, size: size})
	c.ghostBytes += size
	for c.ghostBytes > c.ghostMax && c.ghost.Len() > 0 {
		e := c.ghost.Back()
		g := e.Value.(*ghostEntry)
		c.ghost.Remove(e)
		delete(c.ghosts, g.key)
		c.ghostBytes -= g.size
	}
}
