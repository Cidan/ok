// Package ldapdial connects to an LDAP directory over TLS — ldaps://, or ldap:// with StartTLS —
// the one way both LDAP sign-in and the LDAP system of record reach it.
package ldapdial

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

// Config is how to reach a directory.
type Config struct {
	// URL is the server: ldaps://ldap.example.com, or ldap://… with StartTLS.
	URL      string
	StartTLS bool
	// CAFile verifies the server's certificate, when it is not one the system trusts.
	CAFile string
}

// Conn is what ok asks of a connection.
type Conn interface {
	Bind(username, password string) error
	Search(*ldap.SearchRequest) (*ldap.SearchResult, error)
	SearchWithPaging(*ldap.SearchRequest, uint32) (*ldap.SearchResult, error)
	Close() error
}

// Dialer returns a function that connects to the directory, refusing a configuration that would
// send passwords in the clear.
func Dialer(cfg Config) (func() (Conn, error), error) {
	if cfg.URL == "" {
		return nil, errors.New("an LDAP directory needs a URL")
	}
	if !strings.HasPrefix(cfg.URL, "ldaps://") && !cfg.StartTLS {
		return nil, errors.New("an LDAP directory is reached over TLS: ldaps://, or StartTLS")
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s holds no certificate", cfg.CAFile)
		}
		tc.RootCAs = pool
	}
	return func() (Conn, error) {
		c, err := ldap.DialURL(cfg.URL, ldap.DialWithTLSConfig(tc))
		if err != nil {
			return nil, err
		}
		if cfg.StartTLS {
			if err := c.StartTLS(tc); err != nil {
				c.Close()
				return nil, err
			}
		}
		return c, nil
	}, nil
}

// NormalizeDN is a DN as ok compares DNs: attribute types and values lowercased, no spaces between
// RDNs. A DN that does not parse is lowercased as it is.
func NormalizeDN(dn string) string {
	parsed, err := ldap.ParseDN(dn)
	if err != nil {
		return strings.ToLower(dn)
	}
	rdns := make([]string, 0, len(parsed.RDNs))
	for _, rdn := range parsed.RDNs {
		parts := make([]string, 0, len(rdn.Attributes))
		for _, a := range rdn.Attributes {
			parts = append(parts, strings.ToLower(a.Type)+"="+strings.ToLower(a.Value))
		}
		rdns = append(rdns, strings.Join(parts, "+"))
	}
	return strings.Join(rdns, ",")
}

// GUIDString is an Active Directory objectGUID as a string: its first three fields are
// little-endian.
func GUIDString(b []byte) string {
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])
}

// ID is an entry's identifying attribute as a string: an objectGUID in its string form, any other
// attribute as it is.
func ID(e *ldap.Entry, attr string) string {
	if strings.EqualFold(attr, "objectGUID") {
		if raw := e.GetRawAttributeValue(attr); len(raw) == 16 {
			return GUIDString(raw)
		}
	}
	return e.GetAttributeValue(attr)
}
