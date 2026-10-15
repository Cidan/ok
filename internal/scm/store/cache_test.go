package store

import (
	"container/list"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/Cidan/ok/internal/serve/metrics"
)

// checkCache checks the cache's bookkeeping: its bounds, its byte counts, and that every key is
// in exactly one place.
func checkCache(t *testing.T, c *cache[[]byte]) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.smallBytes+c.mainBytes > c.max {
		t.Fatalf("holds %d bytes, over its %d", c.smallBytes+c.mainBytes, c.max)
	}
	if c.ghostBytes > c.ghostMax {
		t.Fatalf("ghost queue stands for %d bytes, over its %d", c.ghostBytes, c.ghostMax)
	}
	count := func(l *list.List, name string) int {
		n := 0
		for e := l.Front(); e != nil; e = e.Next() {
			ent := e.Value.(*cacheEntry[[]byte])
			if c.items[ent.key] != e {
				t.Fatalf("%s entry %q is not the one its key names", name, ent.key)
			}
			if _, ghost := c.ghosts[ent.key]; ghost {
				t.Fatalf("%q is both cached and a ghost", ent.key)
			}
			if ent.size != len(ent.value) {
				t.Fatalf("%s entry %q counted as %d bytes, holds %d", name, ent.key, ent.size, len(ent.value))
			}
			n += ent.size
		}
		return n
	}
	if n := count(c.small, "small"); n != c.smallBytes {
		t.Fatalf("small queue holds %d bytes, counted %d", n, c.smallBytes)
	}
	if n := count(c.main, "main"); n != c.mainBytes {
		t.Fatalf("main queue holds %d bytes, counted %d", n, c.mainBytes)
	}
	if len(c.items) != c.small.Len()+c.main.Len() {
		t.Fatalf("%d keys for %d entries", len(c.items), c.small.Len()+c.main.Len())
	}
	g := 0
	for e := c.ghost.Front(); e != nil; e = e.Next() {
		ge := e.Value.(*ghostEntry)
		if c.ghosts[ge.key] != e {
			t.Fatalf("ghost %q is not the one its key names", ge.key)
		}
		g += ge.size
	}
	if g != c.ghostBytes || len(c.ghosts) != c.ghost.Len() {
		t.Fatalf("ghost queue: %d bytes in %d keys, counted %d in %d", g, c.ghost.Len(), c.ghostBytes, len(c.ghosts))
	}
}

func value(n int) []byte { return make([]byte, n) }

// put caches a byte value under its length.
func put(c *cache[[]byte], key string, v []byte) { c.put(key, v, len(v)) }

func TestCacheGetPut(t *testing.T) {
	c := newCache[[]byte]("test", 100_000)
	if _, ok := c.get("a"); ok {
		t.Fatal("an empty cache has a")
	}
	c.put("a", []byte("first"), 5)
	c.put("a", []byte("second"), 6) // content-addressed: a key's value never changes, so a second put is ignored
	if v, ok := c.get("a"); !ok || string(v) != "first" {
		t.Fatalf("get(a) = %q, %v", v, ok)
	}
	c.put("huge", value(100_001), 100_001)
	c.put("small-queue-sized", value(10_000), 10_000) // as large as the small queue: never admitted
	for _, k := range []string{"huge", "small-queue-sized"} {
		if _, ok := c.get(k); ok {
			t.Fatalf("%s was admitted", k)
		}
	}
	checkCache(t, c)
}

// Random traffic never breaks the bounds or the bookkeeping.
func TestCacheStaysWithinBudget(t *testing.T) {
	c := newCache[[]byte]("test", 64<<10)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 20_000 {
		k := fmt.Sprintf("k%d", r.IntN(600))
		if r.IntN(3) == 0 {
			c.get(k)
		} else {
			put(c, k, value(1+r.IntN(4000)))
		}
		if i%97 == 0 {
			checkCache(t, c)
		}
	}
	checkCache(t, c)
}

// A scan many times the cache's size passes through without displacing entries read
// repeatedly — what a least-recently-used cache cannot do.
func TestCacheResistsScans(t *testing.T) {
	const size = 1000
	c := newCache[[]byte]("test", 100*size)
	var hot []string
	for i := range 50 {
		k := fmt.Sprintf("hot%d", i)
		hot = append(hot, k)
		put(c, k, value(size))
	}
	for range 2 {
		for _, k := range hot {
			c.get(k)
		}
	}
	for i := range 1000 {
		put(c, fmt.Sprintf("scan%d", i), value(size))
		if i%10 == 0 {
			for _, k := range hot {
				c.get(k)
			}
		}
	}
	for _, k := range hot {
		if _, ok := c.get(k); !ok {
			t.Fatalf("hot entry %s was displaced by a scan", k)
		}
	}
	if _, ok := c.get("scan0"); ok {
		t.Fatal("the scan's first entry, read once, is still cached")
	}
	checkCache(t, c)
}

// An entry that left the small queue unread is remembered: put again while the ghost queue still
// holds its key, it joins the main queue and survives the next scan.
func TestCacheGhostReadmits(t *testing.T) {
	const size = 1000
	c := newCache[[]byte]("test", 100*size)
	n := 0
	scanOne := func() {
		put(c, fmt.Sprintf("scan%d", n), value(size))
		n++
	}
	for range 300 { // fill the cache, so what follows is in steady state
		scanOne()
	}
	put(c, "x", value(size))
	for range 1000 {
		c.mu.Lock()
		_, ghost := c.ghosts["x"]
		c.mu.Unlock()
		if ghost {
			break
		}
		scanOne()
	}
	if _, ok := c.get("x"); ok {
		t.Fatal("x, never read, was still cached when the small queue let it go")
	}
	put(c, "x", value(size))
	c.mu.Lock()
	inMain := c.items["x"] != nil && c.mainBytes > 0
	for e := c.small.Front(); e != nil; e = e.Next() {
		if e.Value.(*cacheEntry[[]byte]).key == "x" {
			inMain = false
		}
	}
	c.mu.Unlock()
	if !inMain {
		t.Fatal("x, readmitted from the ghost queue, did not join the main queue")
	}
	for range 300 {
		scanOne()
	}
	if _, ok := c.get("x"); !ok {
		t.Fatal("x, readmitted from the ghost queue, did not survive the next scan")
	}
	checkCache(t, c)
}

func TestCacheConcurrent(t *testing.T) {
	c := newCache[[]byte]("test", 32<<10)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 9))
			for range 5000 {
				k := fmt.Sprintf("k%d", r.IntN(200))
				if r.IntN(2) == 0 {
					c.get(k)
				} else {
					put(c, k, value(1+r.IntN(2000)))
				}
			}
		}()
	}
	wg.Wait()
	checkCache(t, c)
}

// A cache reports its reads, admissions, promotions, evictions, and the bytes each queue holds.
func TestCacheMetrics(t *testing.T) {
	r := metrics.Default()
	read := func(name string, labels ...string) float64 {
		return r.Read(name, append([]string{"metered"}, labels...)...).Value
	}
	c := newCache[[]byte]("metered", 100*1000)
	put(c, "a", value(1000))
	c.get("a")
	c.get("a")
	c.get("nope")
	if read("ok_scm_cache_lookups_total", "hit") != 2 || read("ok_scm_cache_lookups_total", "miss") != 1 {
		t.Fatalf("lookups: %v hits, %v misses", read("ok_scm_cache_lookups_total", "hit"), read("ok_scm_cache_lookups_total", "miss"))
	}
	if read("ok_scm_cache_admissions_total", "small") != 1 || read("ok_scm_cache_bytes", "small") != 1000 {
		t.Fatalf("admission: %v, small bytes %v", read("ok_scm_cache_admissions_total", "small"), read("ok_scm_cache_bytes", "small"))
	}
	// Fill it far past its budget with entries read once: "a", read twice, is promoted; the rest
	// pass through the small queue and leave.
	for i := range 300 {
		put(c, fmt.Sprintf("scan%d", i), value(1000))
	}
	if read("ok_scm_cache_promotions_total") < 1 {
		t.Fatal("the entry read twice was not promoted")
	}
	if read("ok_scm_cache_evictions_total", "small") < 100 {
		t.Fatalf("%v evictions from the small queue", read("ok_scm_cache_evictions_total", "small"))
	}
	held := read("ok_scm_cache_bytes", "small") + read("ok_scm_cache_bytes", "main")
	c.mu.Lock()
	want := float64(c.smallBytes + c.mainBytes)
	c.mu.Unlock()
	if held != want {
		t.Fatalf("the gauges say %v bytes, the cache holds %v", held, want)
	}
	put(c, "huge", value(200*1000))
	if read("ok_scm_cache_refusals_total") != 1 {
		t.Fatal("an entry larger than the cache was not counted as refused")
	}
}
