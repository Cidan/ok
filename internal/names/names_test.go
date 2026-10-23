package names

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/serve/authn"
)

// dir is a directory of principals by stable ID, counting what it is asked.
type dir struct {
	byID  map[string]authn.Principal
	asked int
	down  bool
}

func (d *dir) Principal(_ context.Context, name string) (authn.Principal, error) {
	d.asked++
	if d.down {
		return authn.Principal{}, errors.New("down")
	}
	for _, p := range d.byID {
		if p.Name == name {
			return p, nil
		}
	}
	return authn.Principal{}, authn.ErrNotFound
}

func (d *dir) PrincipalByID(_ context.Context, id string) (authn.Principal, error) {
	d.asked++
	if d.down {
		return authn.Principal{}, errors.New("down")
	}
	p, ok := d.byID[id]
	if !ok {
		return authn.Principal{}, authn.ErrNotFound
	}
	return p, nil
}

// A resolver names principals by ID and finds IDs by name, keeps each answer a while — unknowns
// too — and shows a rename once its answers expire.
func TestResolver(t *testing.T) {
	ctx := context.Background()
	d := &dir{byID: map[string]authn.Principal{"prn_1": {ID: "prn_1", Name: "ada.human.example.com", Human: true, Active: true}}}
	r := New(d)
	now := time.Unix(1_000_000, 0)
	r.now = func() time.Time { return now }

	if n, err := r.Name(ctx, "prn_1"); err != nil || n != "ada.human.example.com" {
		t.Fatalf("Name: %q, %v", n, err)
	}
	if id, err := r.ID(ctx, "ada.human.example.com"); err != nil || id != "prn_1" {
		t.Fatalf("ID: %q, %v", id, err)
	}
	if d.asked != 1 {
		t.Errorf("the directory was asked %d times; an answer by ID names it by name too", d.asked)
	}
	if _, err := r.ID(ctx, "nobody.human.example.com"); !errors.Is(err, ErrUnknown) {
		t.Errorf("an unknown name: %v", err)
	}
	if _, err := r.ID(ctx, "nobody.human.example.com"); !errors.Is(err, ErrUnknown) || d.asked != 2 {
		t.Errorf("an unknown name, again: %v, asked %d", err, d.asked)
	}
	if n, err := r.Name(ctx, "prn_gone"); err != nil || n != "prn_gone" {
		t.Errorf("a principal the directory cannot name: %q, %v; want its ID", n, err)
	}
	if n, err := r.Name(ctx, ""); err != nil || n != "" {
		t.Errorf("no one: %q, %v", n, err)
	}

	// Renamed: the old answer holds until it expires; then the new name, and the old one names no
	// one.
	d.byID["prn_1"] = authn.Principal{ID: "prn_1", Name: "ada2.human.example.com", Human: true, Active: true}
	if n, _ := r.Name(ctx, "prn_1"); n != "ada.human.example.com" {
		t.Errorf("within the TTL: %q", n)
	}
	now = now.Add(TTL + time.Second)
	if n, _ := r.Name(ctx, "prn_1"); n != "ada2.human.example.com" {
		t.Errorf("after the TTL: %q", n)
	}
	if _, err := r.ID(ctx, "ada.human.example.com"); !errors.Is(err, ErrUnknown) {
		t.Errorf("a name its principal no longer has: %v", err)
	}
	ids, err := r.IDs(ctx, []string{"ada2.human.example.com"})
	if err != nil || len(ids) != 1 || ids[0] != "prn_1" {
		t.Errorf("IDs: %v, %v", ids, err)
	}
	ns, err := r.Names(ctx, []string{"prn_1", "prn_gone"})
	if err != nil || len(ns) != 2 || ns[0] != "ada2.human.example.com" || ns[1] != "prn_gone" {
		t.Errorf("Names: %v, %v", ns, err)
	}

	// A directory that cannot answer is an error, never an unknown.
	now = now.Add(TTL + time.Second)
	d.down = true
	if _, err := r.Name(ctx, "prn_1"); err == nil || errors.Is(err, ErrUnknown) {
		t.Errorf("the directory down: %v", err)
	}
}
