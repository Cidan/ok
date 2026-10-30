package names

import (
	"context"
	"errors"
	"testing"

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

// A resolver names principals by ID and finds IDs by name, asking the directory each time — a
// rename shows at once — and each distinct one once in a call of many.
func TestResolver(t *testing.T) {
	ctx := context.Background()
	d := &dir{byID: map[string]authn.Principal{"prn_1": {ID: "prn_1", Name: "ada.human.example.com", Human: true, Active: true}}}
	r := New(d)

	if n, err := r.Name(ctx, "prn_1"); err != nil || n != "ada.human.example.com" {
		t.Fatalf("Name: %q, %v", n, err)
	}
	if id, err := r.ID(ctx, "ada.human.example.com"); err != nil || id != "prn_1" {
		t.Fatalf("ID: %q, %v", id, err)
	}
	if d.asked != 2 {
		t.Errorf("the directory was asked %d times, want each time", d.asked)
	}
	if _, err := r.ID(ctx, "nobody.human.example.com"); !errors.Is(err, ErrUnknown) {
		t.Errorf("an unknown name: %v", err)
	}
	if n, err := r.Name(ctx, "prn_gone"); err != nil || n != "prn_gone" {
		t.Errorf("a principal the directory cannot name: %q, %v; want its ID", n, err)
	}
	if n, err := r.Name(ctx, ""); err != nil || n != "" {
		t.Errorf("no one: %q, %v", n, err)
	}

	// Renamed: the new name at once, and the old one names no one.
	d.byID["prn_1"] = authn.Principal{ID: "prn_1", Name: "ada2.human.example.com", Human: true, Active: true}
	if n, _ := r.Name(ctx, "prn_1"); n != "ada2.human.example.com" {
		t.Errorf("renamed: %q", n)
	}
	if _, err := r.ID(ctx, "ada.human.example.com"); !errors.Is(err, ErrUnknown) {
		t.Errorf("a name its principal no longer has: %v", err)
	}
	ids, err := r.IDs(ctx, []string{"ada2.human.example.com"})
	if err != nil || len(ids) != 1 || ids[0] != "prn_1" {
		t.Errorf("IDs: %v, %v", ids, err)
	}
	asked := d.asked
	ns, err := r.Names(ctx, []string{"prn_1", "prn_gone", "prn_1", "prn_1"})
	if err != nil || len(ns) != 4 || ns[0] != "ada2.human.example.com" || ns[1] != "prn_gone" || ns[3] != "ada2.human.example.com" {
		t.Errorf("Names: %v, %v", ns, err)
	}
	if d.asked != asked+2 {
		t.Errorf("four IDs, two of them distinct, asked the directory %d times", d.asked-asked)
	}

	// A directory that cannot answer is an error, never an unknown.
	d.down = true
	if _, err := r.Name(ctx, "prn_1"); err == nil || errors.Is(err, ErrUnknown) {
		t.Errorf("the directory down: %v", err)
	}
}

// A scope resolves each principal once — found, or unknown — for its life; a directory that fails
// is asked again; a context without one, and a scope after it, ask each time.
func TestScope(t *testing.T) {
	d := &dir{byID: map[string]authn.Principal{"prn_1": {ID: "prn_1", Name: "ada.human.example.com", Human: true, Active: true}}}
	r := New(d)
	ctx := WithScope(context.Background())
	if WithScope(ctx) != ctx {
		t.Error("a scope within a scope is a new one")
	}
	for range 3 {
		if n, err := r.Name(ctx, "prn_1"); err != nil || n != "ada.human.example.com" {
			t.Fatalf("Name: %q, %v", n, err)
		}
		if _, err := r.ByID(ctx, "prn_9"); !errors.Is(err, ErrUnknown) {
			t.Fatalf("an unknown ID: %v", err)
		}
		if id, err := r.ID(ctx, "ada.human.example.com"); err != nil || id != "prn_1" {
			t.Fatalf("ID: %q, %v", id, err)
		}
	}
	if d.asked != 3 {
		t.Errorf("the directory was asked %d times in a scope, want once for each of 3 principals", d.asked)
	}
	d.down = true
	if _, err := r.ByName(ctx, "bea.human.example.com"); err == nil {
		t.Fatal("a failing directory answered")
	}
	d.down = false
	if _, err := r.ByName(ctx, "bea.human.example.com"); !errors.Is(err, ErrUnknown) {
		t.Errorf("after a failure, asked again: %v", err)
	}
	d.asked = 0
	d.byID["prn_1"] = authn.Principal{ID: "prn_1", Name: "ada2.human.example.com", Human: true, Active: true}
	if n, _ := r.Name(context.Background(), "prn_1"); n != "ada2.human.example.com" || d.asked != 1 {
		t.Errorf("outside the scope: %q, asked %d", n, d.asked)
	}
	if n, _ := r.Name(WithScope(context.Background()), "prn_1"); n != "ada2.human.example.com" || d.asked != 2 {
		t.Errorf("a new scope: %q, asked %d", n, d.asked)
	}
}
