package serve

import (
	"crypto/tls"
	"sync"
	"time"
)

// reloadEvery is how often a role re-reads its certificate, which cert-manager renews in place.
const reloadEvery = time.Minute

// LoadTLS is a TLS configuration serving the certificate and key in two files, re-read as they
// change, so a renewal needs no restart.
func LoadTLS(certFile, keyFile string) (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	cur, loaded := &pair, time.Now()
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			mu.Lock()
			defer mu.Unlock()
			if time.Since(loaded) > reloadEvery {
				loaded = time.Now()
				if p, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
					cur = &p
				}
			}
			return cur, nil
		},
	}, nil
}
