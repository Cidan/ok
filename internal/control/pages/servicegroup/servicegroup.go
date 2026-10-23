// Package servicegroup groups services as ok's Services pages show them
// (docs/deploy.md, "Services"): the services a deploy stanza's `group` puts together,
// across repositories — one namespace, by name — and every service with no group as a group of
// its own. A group's status is its services' worst.
package servicegroup

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Service is a service as the Services pages list it: what a group is made of.
type Service struct {
	ID             int64
	Name           string
	Group          string // its deploy's group; "": a group of its own
	Repository     string
	RepositoryHref string
	Workload       string // "deployment smoke/serve"
	Declared       bool   // main still declares it
	StatusLabel    string
	StatusKind     string // a status's key: healthy, deploying, degraded, failed, drifted, unknown, undeployed
	Reason         string // why that status, in a line
	Found          bool   // the cluster has its workload
	Ready          int32
	Replicas       int32
	// What ok deployed: the image(s), short ("smoke/serve@674607bcb740"), the commit they
	// were built at, the CL that landed it, and when. DeployedID is that deployment (0: never
	// deployed), and DeployedAt when it was made, which orders a group's deployments.
	Version    string
	Commit     string
	CommitHref string
	CL         int64
	CLHref     string
	DeployedID int64
	DeployedAt time.Time
	Age        string
	AgeTitle   string
}

// Group is one group on the Services pages: a named group's services, or a service of
// its own (Named false). Its status is the worst of its services' — of those main still
// declares, while any is.
type Group struct {
	Name        string
	Named       bool
	Members     []Service // by name, then repository
	StatusLabel string
	StatusKind  string
	Reason      string // why, in a line: the worst service's reason, named
}

// statusRank orders services' statuses from best to worst: a group shows its worst.
var statusRank = map[string]int{
	"healthy":    0,
	"undeployed": 1,
	"deploying":  2,
	"drifted":    3,
	"unknown":    4,
	"degraded":   5,
	"failed":     6,
}

// Of groups services as the Services pages show them, by name.
func Of(services []Service) []Group {
	named := map[string]*Group{}
	var groups []*Group
	for _, sv := range services {
		if sv.Group == "" {
			groups = append(groups, &Group{Name: sv.Name, Members: []Service{sv}})
			continue
		}
		g := named[sv.Group]
		if g == nil {
			g = &Group{Name: sv.Group, Named: true}
			named[sv.Group] = g
			groups = append(groups, g)
		}
		g.Members = append(g.Members, sv)
	}
	out := make([]Group, 0, len(groups))
	for _, g := range groups {
		slices.SortStableFunc(g.Members, func(a, b Service) int {
			if c := strings.Compare(a.Name, b.Name); c != 0 {
				return c
			}
			return strings.Compare(a.Repository, b.Repository)
		})
		g.aggregate()
		out = append(out, *g)
	}
	slices.SortStableFunc(out, func(a, b Group) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(a.Members[0].Repository, b.Members[0].Repository)
	})
	return out
}

// aggregate sets the group's status: its worst service's.
func (g *Group) aggregate() {
	counted := g.counted()
	worst, others := counted[0], 0
	for _, sv := range counted[1:] {
		if statusRank[sv.StatusKind] > statusRank[worst.StatusKind] {
			worst = sv
		}
	}
	g.StatusLabel, g.StatusKind = worst.StatusLabel, worst.StatusKind
	if len(g.Members) == 1 {
		g.Reason = worst.Reason
		return
	}
	healthy := 0
	for _, sv := range counted {
		switch {
		case sv.StatusKind == "healthy":
			healthy++
		case sv.ID != worst.ID:
			others++
		}
	}
	if worst.StatusKind == "healthy" {
		g.Reason = fmt.Sprintf("%d of %d services Healthy", healthy, len(counted))
		return
	}
	g.Reason = worst.Name
	if worst.Reason != "" {
		g.Reason += ": " + worst.Reason
	} else {
		g.Reason += ": " + worst.StatusLabel
	}
	if others > 0 {
		g.Reason += fmt.Sprintf(" (and %s not Healthy)", pluralCount(others, "other", "others"))
	}
}

// Href is the group's page: a named group's overview, or the page of a group's one service.
func (g Group) Href() string {
	if g.Overview() {
		return "/services/group/" + g.Name
	}
	return ServicePage(g.Members[0].ID)
}

// Overview reports whether the group's page has an overview: a group of several services. A
// group of one opens on its service.
func (g Group) Overview() bool { return len(g.Members) > 1 }

// Detail is the group's line under its name: how many services in which repositories, or a
// service's repository and workload.
func (g Group) Detail() string {
	if !g.Overview() {
		return g.Members[0].Repository + " · " + g.Members[0].Workload
	}
	return pluralCount(len(g.Members), "service", "services") + " · " + strings.Join(g.Repositories(), ", ")
}

// Repositories are the repositories its services are declared in, by name.
func (g Group) Repositories() []string {
	var out []string
	for _, sv := range g.Members {
		if !slices.Contains(out, sv.Repository) {
			out = append(out, sv.Repository)
		}
	}
	slices.Sort(out)
	return out
}

// Healthy is how many of the services its status counts are Healthy, of how many: those main
// still declares, while any is.
func (g Group) Healthy() string {
	counted := g.counted()
	healthy := 0
	for _, sv := range counted {
		if sv.StatusKind == "healthy" {
			healthy++
		}
	}
	return fmt.Sprintf("%d of %s", healthy, pluralCount(len(counted), "service", "services"))
}

// counted are the services its status counts: those main still declares, while any is.
func (g Group) counted() []Service {
	if !g.Declared() {
		return g.Members
	}
	var out []Service
	for _, sv := range g.Members {
		if sv.Declared {
			out = append(out, sv)
		}
	}
	return out
}

// Declared reports whether main still declares any of its services.
func (g Group) Declared() bool {
	return slices.ContainsFunc(g.Members, func(sv Service) bool { return sv.Declared })
}

// Newest is the group's service deployed last, by ok; ok is false when none has been.
func (g Group) Newest() (Service, bool) {
	var out Service
	for _, sv := range g.Members {
		if sv.DeployedID != 0 && (out.DeployedID == 0 || sv.DeployedAt.After(out.DeployedAt) ||
			sv.DeployedAt.Equal(out.DeployedAt) && sv.DeployedID > out.DeployedID) {
			out = sv
		}
	}
	return out, out.DeployedID != 0
}

// Member is the group's service id; ok is false when it has none.
func (g Group) Member(id int64) (Service, bool) {
	for _, sv := range g.Members {
		if sv.ID == id {
			return sv, true
		}
	}
	return Service{}, false
}

// Find is the group a service is in, and FindNamed a named group; ok is false
// when there is none.
func Find(groups []Group, id int64) (Group, bool) {
	for _, g := range groups {
		if _, ok := g.Member(id); ok {
			return g, true
		}
	}
	return Group{}, false
}

// FindNamed is the named group name; ok is false when there is none.
func FindNamed(groups []Group, name string) (Group, bool) {
	for _, g := range groups {
		if g.Named && g.Name == name {
			return g, true
		}
	}
	return Group{}, false
}

// ServicePage is a service's page: its group's, opened on it.
func ServicePage(id int64) string { return "/services/" + strconv.FormatInt(id, 10) }

// pluralCount is a count with its noun: "1 service", "3 services".
func pluralCount(n int, singular, plural string) string {
	if n == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(n) + " " + plural
}
