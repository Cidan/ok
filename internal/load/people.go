package load

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/dial"
)

// PersonHandle is the i-th person's handle in a run with people.
func PersonHandle(i int) string { return fmt.Sprintf("p%05d", i) }

// personEmail is the i-th person's address: what the installation's sign-in matches them by.
func personEmail(i int) string { return PersonHandle(i) + "@example.com" }

// signInAttempts is how many times a person's sign-in is tried, signInBackoff apart: an
// installation just started may not have its system of record's people yet, and signs in only a
// person it has.
const (
	signInAttempts = 5
	signInBackoff  = 10 * time.Second
)

// SignInAll signs people [lo, hi) in, parallel at a time, each's credential kept under root, each
// tried again when it fails; the credentials are in order.
func SignInAll(ctx context.Context, server, root string, lo, hi int, password string, parallel int) ([]dial.Credential, error) {
	out := make([]dial.Credential, hi-lo)
	errs := make([]error, hi-lo)
	sem := make(chan struct{}, max(parallel, 1))
	var wg sync.WaitGroup
	for i := lo; i < hi; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			for attempt := range signInAttempts {
				if attempt > 0 {
					slog.Warn("load: signing a person in again", "person", PersonHandle(i), "err", errs[i-lo])
					select {
					case <-ctx.Done():
						return
					case <-time.After(signInBackoff):
					}
				}
				if out[i-lo], errs[i-lo] = SignIn(ctx, server, filepath.Join(root, PersonHandle(i)), personEmail(i), password); errs[i-lo] == nil || ctx.Err() != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// PeopleFiles are the files a run with n people needs: the system of record's people, beside the
// load's own person (load), and Dex's static users — the same people, each signing in with the
// password whose bcrypt hash is hash.
func PeopleFiles(n int, hash string) (people, dex []byte) {
	var p, d strings.Builder
	p.WriteString("people:\n  - {id: \"1\", handle: load, email: load@example.com, name: Load}\n")
	d.WriteString("# Dex's static users for ok load's people (deploy/staging/README.md): helm's second values file.\n")
	d.WriteString("config:\n  staticPasswords:\n")
	fmt.Fprintf(&d, "    - {email: load@example.com, hash: %q, username: load, userID: \"1\"}\n", hash)
	for i := range n {
		id := fmt.Sprint(1_000_000 + i)
		fmt.Fprintf(&p, "  - {id: %q, handle: %s, email: %s, name: Person %d}\n", id, PersonHandle(i), personEmail(i), i)
		fmt.Fprintf(&d, "    - {email: %s, hash: %q, username: %s, userID: %q}\n", personEmail(i), hash, PersonHandle(i), id)
	}
	return []byte(p.String()), []byte(d.String())
}
