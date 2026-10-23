package storetest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// Personal tokens: a person's, labeled, at most a role, the repositories named, a year at most;
// shown once and kept as a hash; live until revoked or expired, or their holder no longer stands;
// their last use noted; revoked by their holder alone.
func testTokens(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}}
	sync(t, s, src)
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	robot, err := s.CreateRobot(ctx, "ci.robot.example.com")
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: ada.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}

	for name, n := range map[string]store.NewToken{
		"no label":                {Principal: ada.ID, Role: tenancy.Read, Lifetime: time.Hour},
		"no role":                 {Principal: ada.ID, Label: "ci", Lifetime: time.Hour},
		"no lifetime":             {Principal: ada.ID, Label: "ci", Role: tenancy.Read},
		"over a year":             {Principal: ada.ID, Label: "ci", Role: tenancy.Read, Lifetime: store.MaxTokenLifetime + time.Hour},
		"a robot's":               {Principal: robot.ID, Label: "ci", Role: tenancy.Read, Lifetime: time.Hour},
		"a repository none is of": {Principal: ada.ID, Label: "ci", Role: tenancy.Read, Lifetime: time.Hour, Repositories: []int64{42}},
	} {
		if _, _, err := s.CreateToken(ctx, n); err == nil {
			t.Errorf("%s: made", name)
		}
	}
	secret, tok, err := s.CreateToken(ctx, store.NewToken{Principal: ada.ID, Label: " ci ", Role: tenancy.Write, Lifetime: 24 * time.Hour,
		Repositories: []int64{r.ID, r.ID}})
	if err != nil || !strings.HasPrefix(secret, store.PersonalTokenPrefix) || len(secret) < 40 || tok.Label != "ci" || len(tok.Repositories) != 1 ||
		!tok.Expires.Equal(now.Add(24*time.Hour)) {
		t.Fatalf("CreateToken = %q, %+v, %v", secret, tok, err)
	}
	got, holder, err := s.TokenFor(ctx, secret)
	if err != nil || got.ID != tok.ID || holder.ID != ada.ID || got.Role != tenancy.Write || !got.LastUsed.Equal(now) {
		t.Errorf("TokenFor = %+v, %+v, %v", got, holder, err)
	}
	if _, _, err := s.TokenFor(ctx, secret+"x"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a secret there is none of: %v", err)
	}

	// Expired, it is no token; nor once its holder is suspended.
	now = now.Add(24 * time.Hour)
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an expired token: %v", err)
	}
	now = now.Add(-time.Hour)
	src.snap.People[0] = person("1", "ada", false)
	sync(t, s, src)
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a suspended person's token: %v", err)
	}
	src.snap.People[0] = person("1", "ada", true)
	sync(t, s, src)

	// Listed newest first; revoked by their holder alone.
	second, _, err := s.CreateToken(ctx, store.NewToken{Principal: ada.ID, Label: "laptop script", Role: tenancy.Read, Lifetime: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if ts, err := s.Tokens(ctx, ada.ID); err != nil || len(ts) != 2 || ts[1].ID != tok.ID {
		t.Errorf("Tokens = %+v, %v", ts, err)
	}
	if ok, err := s.RevokeToken(ctx, bea.ID, tok.ID); err != nil || ok {
		t.Errorf("another revoking it: %v, %v", ok, err)
	}
	if ok, err := s.RevokeToken(ctx, ada.ID, tok.ID); err != nil || !ok {
		t.Errorf("its holder revoking it: %v, %v", ok, err)
	}
	if ok, _ := s.RevokeToken(ctx, ada.ID, tok.ID); ok {
		t.Error("revoked twice")
	}
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a revoked token: %v", err)
	}
	if _, _, err := s.TokenFor(ctx, second); err != nil {
		t.Errorf("another token of ada's: %v", err)
	}
}
