package microvm

import (
	"io"
	"net"
	"sync"
)

// closeWrite half-closes a connection, where it can be.
func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// pipe copies a and b both ways until both ends are done.
func pipe(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(a, b); closeWrite(a) }()
	go func() { defer wg.Done(); _, _ = io.Copy(b, a); closeWrite(b) }()
	wg.Wait()
	a.Close()
	b.Close()
}
