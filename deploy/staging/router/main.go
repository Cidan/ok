// Command router stands in for the chart's Ingress on staging (README.md), which has none: people's
// secrets service to ok secrets, everything else to ok's server, as the chart's HTTPRoute and
// Ingress route them; HTTP/2 without TLS both ways, through local port-forwards.
//
//	go run ./deploy/staging/router -listen 127.0.0.1:18480 -server 127.0.0.1:18472 -secrets 127.0.0.1:18473
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

func proxy(addr string) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: addr})
	t := &http.Transport{Protocols: new(http.Protocols)}
	t.Protocols.SetUnencryptedHTTP2(true)
	p.Transport = t
	p.FlushInterval = -1 // gRPC streams
	return p
}

func main() {
	listen := flag.String("listen", "127.0.0.1:18480", "where jj reaches ok (ok.endpoint)")
	server := flag.String("server", "127.0.0.1:18472", "ok's server, port-forwarded")
	secrets := flag.String("secrets", "127.0.0.1:18473", "ok secrets, port-forwarded")
	flag.Parse()
	toServer, toSecrets := proxy(*server), proxy(*secrets)
	srv := &http.Server{Addr: *listen, Protocols: new(http.Protocols), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ok.secrets.v1.Secrets/") {
			toSecrets.ServeHTTP(w, r)
			return
		}
		toServer.ServeHTTP(w, r)
	})}
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	log.Fatal(srv.ListenAndServe())
}
