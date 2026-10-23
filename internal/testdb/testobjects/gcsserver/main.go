// Command gcsserver runs testobjects' GCS emulator on a loopback port, for tests that start ok as a
// separate process (jj/testscm): `gcsserver -listen 127.0.0.1:0 -bucket ok-scm` prints the address it
// listens at, one line on standard output, and serves until it is killed.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Cidan/ok/internal/testdb/testobjects"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "the address to listen at")
	buckets := flag.String("bucket", "", "the buckets to make, comma-separated")
	flag.Parse()
	var names []string
	for b := range strings.SplitSeq(*buckets, ",") {
		if b != "" {
			names = append(names, b)
		}
	}
	srv, err := testobjects.StartGCS(*listen, names...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gcsserver:", err)
		os.Exit(1)
	}
	fmt.Println(srv.Addr())
	select {}
}
