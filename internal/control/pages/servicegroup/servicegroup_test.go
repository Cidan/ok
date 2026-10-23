package servicegroup

import (
	"reflect"
	"testing"
	"time"
)

// Services group by their deploy's group, across repositories; a service with no group is a group
// of its own; groups and their services are in name order.
func TestGroupServices(t *testing.T) {
	sv := func(id int64, name, group, repo, kind string) Service {
		return Service{ID: id, Name: name, Group: group, Repository: repo, Declared: true, StatusKind: kind, StatusLabel: kind, Reason: kind + " because"}
	}
	groups := Of([]Service{
		sv(1, "ok-scm", "ok", "acme", "healthy"),
		sv(2, "serve", "", "smoke", "healthy"),
		sv(3, "control", "ok", "acme", "healthy"),
		sv(4, "worker", "ok", "other", "healthy"),
		sv(5, "serve", "", "acme", "deploying"),
	})
	var got [][]int64
	for _, g := range groups {
		var ids []int64
		for _, m := range g.Members {
			ids = append(ids, m.ID)
		}
		got = append(got, ids)
	}
	if want := [][]int64{{3, 1, 4}, {5}, {2}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groups %v, want %v", got, want)
	}
	okGroup, serve := groups[0], groups[1]
	if okGroup.Name != "ok" || !okGroup.Named || !okGroup.Overview() || okGroup.Href() != "/services/group/ok" ||
		okGroup.Detail() != "3 services · acme, other" || okGroup.Reason != "3 of 3 services Healthy" || okGroup.Healthy() != "3 of 3 services" {
		t.Errorf("okGroup %+v (%s, %s)", okGroup, okGroup.Href(), okGroup.Detail())
	}
	if serve.Named || serve.Overview() || serve.Href() != "/services/5" || serve.StatusKind != "deploying" || serve.Reason != "deploying because" {
		t.Errorf("a service of its own %+v (%s)", serve, serve.Href())
	}
	if g, ok := Find(groups, 4); !ok || g.Name != "ok" {
		t.Errorf("the group of 4: %v %v", g.Name, ok)
	}
	if _, ok := FindNamed(groups, "serve"); ok {
		t.Errorf("a service of its own is no named group")
	}
	if one := Of([]Service{sv(9, "solo", "lonely", "acme", "healthy")})[0]; one.Overview() || one.Href() != "/services/9" {
		t.Errorf("a named group of one opens on its service: %s", one.Href())
	}
}

// A group's status is its worst service's, among those main still declares (all, when none is),
// and says which service and why.
func TestGroupStatus(t *testing.T) {
	sv := func(id int64, name, kind, reason string, declared bool) Service {
		return Service{ID: id, Name: name, Group: "g", Declared: declared, StatusKind: kind, StatusLabel: kind, Reason: reason}
	}
	for _, c := range []struct {
		name     string
		members  []Service
		kind     string
		reason   string
		declared bool
	}{
		{"all healthy", []Service{sv(1, "a", "healthy", "2 of 2", true), sv(2, "b", "healthy", "1 of 1", true)}, "healthy", "2 of 2 services Healthy", true},
		{"the worst", []Service{sv(1, "a", "deploying", "Deployment 3", true), sv(2, "b", "failed", "rolled back", true), sv(3, "c", "healthy", "", true)},
			"failed", "b: rolled back (and 1 other not Healthy)", true},
		{"no reason", []Service{sv(1, "a", "undeployed", "", true), sv(2, "b", "healthy", "", true)}, "undeployed", "a: undeployed", true},
		{"ranked", []Service{sv(1, "a", "drifted", "x", true), sv(2, "b", "degraded", "y", true), sv(3, "c", "unknown", "z", true)},
			"degraded", "b: y (and 2 others not Healthy)", true},
		{"undeclared does not count", []Service{sv(1, "a", "degraded", "gone", false), sv(2, "b", "healthy", "", true)}, "healthy", "1 of 1 services Healthy", true},
		{"all undeclared", []Service{sv(1, "a", "degraded", "gone", false), sv(2, "b", "healthy", "", false)}, "degraded", "a: gone", false},
	} {
		g := Of(c.members)[0]
		if g.StatusKind != c.kind || g.Reason != c.reason || g.Declared() != c.declared || (c.name == "undeclared does not count" && g.Healthy() != "1 of 1 service") {
			t.Errorf("%s: %s %q declared=%v, want %s %q %v", c.name, g.StatusKind, g.Reason, g.Declared(), c.kind, c.reason, c.declared)
		}
	}
}

// The newest deployment of a group is the one ok made last, among its services — by when, not by
// ID, which is random.
func TestGroupNewest(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	g := Of([]Service{{ID: 1, Name: "a", Group: "g", DeployedID: 9, DeployedAt: t0}, {ID: 2, Name: "b", Group: "g", DeployedID: 7, DeployedAt: t0.Add(time.Minute)},
		{ID: 3, Name: "c", Group: "g"}})[0]
	if sv, ok := g.Newest(); !ok || sv.ID != 2 {
		t.Errorf("newest %v %v", sv.ID, ok)
	}
	if _, ok := Of([]Service{{ID: 1, Name: "a"}})[0].Newest(); ok {
		t.Errorf("a service never deployed has no newest deployment")
	}
}
