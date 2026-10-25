package store

import "testing"

// A run queued wakes the first lease in each pool's line, not every lease; the next is woken in
// turn; one that left its line is never woken; and a reconnect wakes all.
func TestLeaseLines(t *testing.T) {
	l := newLeaseLines()
	a1, a2, a3 := l.join("a"), l.join("a"), l.join("a")
	b1 := l.join("b")
	woken := func(w chan struct{}) bool {
		select {
		case <-w:
			return true
		default:
			return false
		}
	}
	l.wakeEach()
	if !woken(a1) || woken(a2) || woken(a3) || !woken(b1) {
		t.Fatalf("a run queued woke a1 %v, a2 %v, a3 %v, b1 %v: want a1 and b1 alone", woken(a1), woken(a2), woken(a3), woken(b1))
	}
	l.leave("a", a2)
	l.wakeNext("a")
	if woken(a2) || !woken(a3) {
		t.Fatalf("the next in line: a2 (left) %v, a3 %v", woken(a2), woken(a3))
	}
	l.wakeNext("a") // nobody left
	c1, c2 := l.join("c"), l.join("c")
	l.wakeAll()
	if !woken(c1) || !woken(c2) {
		t.Fatal("a reconnect did not wake every lease in line")
	}
	l.leave("c", c1) // woken already: nothing to take out
}
