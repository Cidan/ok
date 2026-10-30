// Command natsserver runs a NATS server with JetStream on a loopback port, for tests that start ok
// as a separate process (jj/testscm), whose roles keep their work on it: `natsserver -listen
// 127.0.0.1:0` prints the server's URL, one line on standard output, and serves until it is
// killed, its streams in a directory of its own that it removes then.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "the address to listen at")
	flag.Parse()
	host, port, err := net.SplitHostPort(*listen)
	if err != nil {
		fail(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		fail(err)
	}
	if p == 0 {
		p = -1 // NATS's "any free port"
	}
	dir, err := os.MkdirTemp("", "natsserver-")
	if err != nil {
		fail(err)
	}
	defer os.RemoveAll(dir)
	s, err := server.NewServer(&server.Options{Host: host, Port: p, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true})
	if err != nil {
		fail(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		fail(fmt.Errorf("the server did not start"))
	}
	fmt.Println(s.ClientURL())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	s.Shutdown()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "natsserver:", err)
	os.Exit(1)
}
