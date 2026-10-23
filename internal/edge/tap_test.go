package edge

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// A second attempt reads the whole body again while the history is held; after a commit, or once
// the history passes the limit, no attempt starts again, and the attempt reading still gets every
// byte, in order.
func TestTap(t *testing.T) {
	body := strings.Repeat("0123456789", 100) // 1,000 bytes
	tp := newTap(strings.NewReader(body), 4096)
	go tp.pump()
	first, ok := tp.reader()
	if !ok {
		t.Fatal("no first attempt")
	}
	part := make([]byte, 100)
	if _, err := io.ReadFull(first, part); err != nil || string(part) != body[:100] {
		t.Fatalf("the first attempt's start: %q, %v", part, err)
	}
	first.Close()
	if _, err := first.Read(part); !errors.Is(err, errAbandoned) {
		t.Errorf("reading an abandoned attempt: %v", err)
	}
	second, ok := tp.reader()
	if !ok {
		t.Fatal("no second attempt while the history is held")
	}
	if got, err := io.ReadAll(second); err != nil || string(got) != body {
		t.Fatalf("the second attempt read %d bytes, %v; want the whole body", len(got), err)
	}
	tp.commit()
	if _, ok := tp.reader(); ok || tp.replayable() {
		t.Error("an attempt after the commit")
	}

	// A body past the limit: the attempt reads it all, and none starts again.
	long := bytes.Repeat([]byte("x"), 10_000)
	tp = newTap(bytes.NewReader(long), 1000)
	go tp.pump()
	r, _ := tp.reader()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, long) {
		t.Fatalf("a long body: %d bytes, %v", len(got), err)
	}
	if tp.replayable() {
		t.Error("a body past the limit is still replayable")
	}
	if held := len(tp.data); held > 2*1000 {
		t.Errorf("the tap holds %d bytes of a body read, past twice its limit", held)
	}
	tp.stop()
}

// A pump waiting on a slow attempt stops when the request is answered.
func TestTapStop(t *testing.T) {
	src, w := io.Pipe()
	defer w.Close()
	tp := newTap(src, 10)
	done := make(chan struct{})
	go func() { tp.pump(); close(done) }()
	go w.Write(bytes.Repeat([]byte("y"), 100))
	tp.reader() // reads nothing: the pump fills to the limit and waits
	tp.stop()
	<-done
}

// A repository's requests go to one address, the same while the addresses are the same in any
// order; losing another address leaves it where it was; and repositories spread over them all.
func TestRendezvous(t *testing.T) {
	ips := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}
	seen := map[string]int{}
	for repo := int64(1); repo <= 4000; repo++ {
		a := rendezvous(ips, repo)
		if b := rendezvous([]string{ips[3], ips[1], ips[0], ips[2]}, repo); a != b {
			t.Fatalf("repository %d: %s, then %s in another order", repo, a, b)
		}
		var rest []string
		for _, ip := range ips {
			if ip != a {
				rest = append(rest, ip)
				break
			}
		}
		if lost := rendezvous(append([]string{a}, rest...), repo); lost != a {
			t.Fatalf("repository %d moved from %s when another address went", repo, a)
		}
		seen[a]++
	}
	for _, ip := range ips {
		if n := seen[ip]; n < 700 || n > 1300 {
			t.Errorf("%s holds %d of 4,000 repositories: %v", ip, n, fmt.Sprint(seen))
		}
	}
}
