package e2e

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// TestServices lists the groups of services ok deploys — a deploy's group, across repositories,
// or a service of its own — each with its status in aggregate, and shows a group's page: its
// overview, or a service in it, with its pods and recent deployments, beside the group's sub-nav.
// Each region renders on its own for the swaps and refreshes that ask for it.
func TestServices(t *testing.T) {
	e := newEnv(t)
	e.post(dana, "/repositories/new", url.Values{"name": {"svc"}})
	b := e.person(bob)
	b.jj("", "clone", "svc")
	b.write("svc/serve/main.go", "package main\n")
	b.jj("svc", "describe", "-m", "add serve")
	n := uploaded(b.jj("svc", "cl", "--ready"))[0]
	path := clPath("svc", n)
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", n)
	e.submitted(bob, path, "submit")
	cl, _, err := dbChangeList(e, "svc", n)
	if err != nil {
		t.Fatal(err)
	}
	repo := cl.Repo
	// Another repository, which a service of the group deploys from; a service whose repository no
	// one may read is listed to no one.
	e.post(dana, "/repositories/new", url.Values{"name": {"other"}})
	var otherRepo int64
	e.waitFor("the other repository in ok control", func() bool {
		r, ok, err := e.db.RepositoryByName(context.Background(), "other")
		otherRepo = r.ID
		return err == nil && ok
	})
	now := timestamppb.New(time.Now())
	const good = "registry.example.com/smoke/serve@sha256:674607bcb740114a2514ec3520eaa59f5e3199e9b364623042f8304bca02b019"
	const other = "registry.example.com/smoke/serve@sha256:badac5eedca09b2b87de1aa2d2af68f359d9416d5435518b6d30f81facc85253"
	dep := func(id int64, st deploypb.Deployment_Status, ref, reason string) *deploypb.Deployment {
		return &deploypb.Deployment{Id: id, RepositoryId: repo, CommitId: cl.LandedCommit, Status: st, Stage: deploypb.Deployment_STAGE_DONE, Reason: reason,
			CreatedAt: now, StartedAt: now, FinishedAt: now, Images: []*deploypb.Image{{Container: "serve", Target: "//serve:push", Reference: ref}}}
	}
	pods := func(ready int, images ...string) []*deploypb.Pod {
		var out []*deploypb.Pod
		for i, img := range images {
			out = append(out, &deploypb.Pod{Name: "serve-green-" + string(rune('a'+i)), Copy: "green", Ready: i < ready, Phase: "Running", Started: now,
				Images: []*deploypb.ContainerImage{{Container: "serve", Image: img}}})
		}
		return out
	}
	running := func(ready int32, image string, ps []*deploypb.Pod) *deploypb.Running {
		return &deploypb.Running{Found: true, Live: "green", Replicas: 2, Ready: ready, Images: []*deploypb.ContainerImage{{Container: "serve", Image: image}}, Pods: ps}
	}
	wl := func(name string) *deploypb.Workload {
		return &deploypb.Workload{Namespace: "smoke", Kind: "deployment", Name: name}
	}
	deploying := dep(5, deploypb.Deployment_STATUS_RUNNING, good, "")
	deploying.Stage, deploying.Roll = deploypb.Deployment_STAGE_ROLL, &deploypb.Roll{Stage: "canary"}
	e.deployer.mu.Lock()
	e.deployer.services = []*deploypb.Service{
		{Id: 1, RepositoryId: repo, Repository: "svc", Name: "serve", Group: "edge", Deploy: "//.ok/workflow/d.yaml:deploy:serve", Workload: wl("serve"), Declared: true,
			Deployed: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""), Latest: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""),
			Running: running(2, good, pods(2, good, good)),
			Recent:  []*deploypb.Deployment{dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, "")}},
		{Id: 2, RepositoryId: repo, Repository: "svc", Name: "api", Group: "edge", Workload: wl("api"), Declared: true,
			Deployed: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""), Latest: dep(2, deploypb.Deployment_STATUS_FAILED, other, "rolled back: canary: pod api-3 restarted 1 time(s)"),
			Running: running(2, good, pods(2, good, good))},
		{Id: 3, RepositoryId: repo, Repository: "svc", Name: "web", Group: "edge", Workload: wl("web"), Declared: true,
			Deployed: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""), Latest: deploying, Running: running(2, good, pods(2, good, good))},
		{Id: 4, RepositoryId: repo, Repository: "svc", Name: "drift", Workload: wl("drift"), Declared: true,
			Deployed: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""), Latest: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""),
			Running: running(2, other, pods(2, other, other)),
			Recent:  []*deploypb.Deployment{dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, "")}},
		{Id: 5, RepositoryId: repo, Repository: "svc", Name: "fresh", Group: "lonely", Workload: wl("fresh"), Declared: true, Running: &deploypb.Running{}},
		{Id: 6, RepositoryId: repo, Repository: "svc", Name: "gone", Workload: wl("gone"), Declared: false,
			Deployed: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""), Latest: dep(1, deploypb.Deployment_STATUS_SUCCEEDED, good, ""),
			Running: running(1, good, pods(1, good, good))},
		// edge spans repositories: one name, one group.
		{Id: 7, RepositoryId: otherRepo, Repository: "other", Name: "cache", Group: "edge", Workload: wl("cache"), Declared: true, Running: &deploypb.Running{}},
	}
	e.deployer.mu.Unlock()

	// The nav offers Services everywhere.
	if !regexp.MustCompile(`href="/services"`).MatchString(e.get(carol, "/").body) {
		t.Errorf("the nav lacks Services")
	}
	p := e.get(carol, "/services")
	rows := map[string]string{}
	raw := map[string]string{}
	hrefs := map[string]string{}
	for _, m := range regexp.MustCompile(`(?s)href="(/services/(?:group/[a-z0-9_.-]+|\d+))"[^>]*>([^<]+)</a>(.*?)(?:<div class="flex items-start gap-4 px-4 py-3|</section>)`).FindAllStringSubmatch(p.body, -1) {
		rows[m[2]], raw[m[2]], hrefs[m[2]] = excerpt(m[3]), m[3], m[1]
	}
	for name, want := range map[string][]string{
		// A group's status is its worst service's, named; a dot shows each service's.
		"edge":   {"Last Deploy Failed", "api: Deployment 2: rolled back: canary", "(and 2 others not Healthy)", "4 services · other, svc", "CL #" + itoa(n)},
		"drift":  {"Drifted", "Container serve runs smoke/serve@badac5eedca0; ok deployed smoke/serve@674607bcb740", "svc · deployment smoke/drift"},
		"lonely": {"Not Deployed Yet", "Not deployed", "svc · deployment smoke/fresh"},
		"gone":   {"Degraded", "1 of 2 pods Ready", "No Longer Declared"},
	} {
		got, ok := rows[name]
		if !ok {
			t.Errorf("no row for %s: %s", name, excerpt(p.body))
			continue
		}
		for _, w := range want {
			if !strings.Contains(got, w) {
				t.Errorf("%s's row lacks %q: %s", name, w, got)
			}
		}
	}
	for _, dot := range []string{`title="api: Last Deploy Failed"`, `title="serve: Healthy"`, `title="web: Deploying"`, `title="cache: Not Deployed Yet"`} {
		if !strings.Contains(raw["edge"], dot) {
			t.Errorf("edge's row lacks the dot %s", dot)
		}
	}
	// A group of several opens on its overview; a group of one, named or not, on its service.
	if hrefs["edge"] != "/services/group/edge" || hrefs["lonely"] != "/services/5" || hrefs["drift"] != "/services/4" || len(rows) != 4 {
		t.Errorf("the groups' pages: %v", hrefs)
	}
	if !p.has(`id="service-groups" data-live hx-get="/services" hx-trigger="every 5s"`) {
		t.Errorf("the list does not refresh itself: %s", excerpt(p.body))
	}

	// A group's overview: its status, its services, and its latest deployments across them,
	// beside the sub-nav — the overview selected.
	p = e.get(carol, "/services/group/edge")
	for _, want := range []string{"edge", "Last Deploy Failed", "1 of 4 services", "Recent Deployments", "Deployment 1", `href="/example/svc/deploy/1"`,
		`href="/services/7"`, "other · deployment smoke/cache", `aria-current="page"`, `hx-get="/services/group/edge" hx-trigger="every 5s"`} {
		if !p.has(want) {
			t.Errorf("edge's overview lacks %q: %s", want, excerpt(p.body))
		}
	}
	if !regexp.MustCompile(`href="/services/group/edge"[^>]*aria-current="page"`).MatchString(p.body) {
		t.Errorf("the overview is not selected: %s", excerpt(p.body))
	}

	// A service's page, in its group: its status, images, pods, and recent deployments, the
	// service selected in the sub-nav and its group in the breadcrumb.
	p = e.get(carol, "/services/1")
	for _, want := range []string{"serve", "Healthy", "Live Copy", "green", "2 of 2 pods", "Deployment 1", "CL #" + itoa(n), good,
		"serve-green-a", "Ready", "Recent Deployments", `href="/example/svc/deploy/1"`, ">edge</a>", "Overview"} {
		if !p.has(want) {
			t.Errorf("serve's page lacks %q: %s", want, excerpt(p.body))
		}
	}
	if !regexp.MustCompile(`href="/services/1"[^>]*aria-current="page"`).MatchString(p.body) {
		t.Errorf("serve is not selected: %s", excerpt(p.body))
	}
	if p.has("not deployed by ok") || p.has("Differs From Deployed") {
		t.Errorf("serve's page flags a match as a difference")
	}
	p = e.get(carol, "/services/4")
	if !p.has("Differs From Deployed") || strings.Count(p.body, "not deployed by ok") != 2 || p.has("Overview") {
		t.Errorf("drift's page: %s", excerpt(p.body))
	}
	// A named group of one is its service's page.
	if p := e.get(carol, "/services/group/lonely"); !p.has("Not Deployed Yet") || p.has("Overview") || !p.has(`href="/services/5"`) {
		t.Errorf("a named group of one: %s", excerpt(p.body))
	}
	for _, path := range []string{"/services/99", "/services/group/nope", "/services/group/drift"} {
		if p := e.do(carol, http.MethodGet, path, nil, nil); p.status != http.StatusNotFound {
			t.Errorf("%s: %d", path, p.status)
		}
	}

	// Each region alone, for what swaps or refreshes it.
	hx := func(path, target string) page {
		return e.do(carol, http.MethodGet, path, nil, map[string]string{"HX-Request": "true", "HX-Target": target})
	}
	if p := hx("/services", "div#service-groups"); !strings.HasPrefix(p.body, "<section") || p.has("data-hydrate") || !p.has("edge") {
		t.Errorf("the list's refresh: %s", excerpt(p.body))
	}
	if p := hx("/services/1", "div#service-main"); !strings.HasPrefix(p.body, `<div class="pb-16">`) || !p.has(`id="service-live"`) ||
		!p.has(`<nav id="service-nav" hx-swap-oob="innerHTML">`) || p.has("<h1") {
		t.Errorf("a navigation within the group: %s", excerpt(p.body))
	}
	if p := hx("/services/group/edge", "div#service-live"); p.has(`id="service-live"`) || p.has("Breadcrumb") || !p.has("Recent Deployments") ||
		!p.has(`<nav id="service-nav" hx-swap-oob="innerHTML">`) {
		t.Errorf("the pane's refresh: %s", excerpt(p.body))
	}
	if p := hx("/services/group/edge", "main#content"); !strings.HasPrefix(p.body, `<div class="flex h-full`) || !p.has(`id="service-main"`) {
		t.Errorf("entering the group: %s", excerpt(p.body))
	}

	e.deployer.mu.Lock()
	e.deployer.down = true
	e.deployer.mu.Unlock()
	if p := e.get(carol, "/services"); !p.has("The deployer could not be reached.") {
		t.Errorf("the deployer down: %s", excerpt(p.body))
	}
	// The pane's refresh keeps its place and says why; the sub-nav keeps its last reading.
	if p := hx("/services/1", "div#service-live"); p.status != http.StatusOK || !p.has("The deployer could not be reached.") || p.has("service-nav") {
		t.Errorf("the pane's refresh with the deployer down: %d %s", p.status, excerpt(p.body))
	}
	if p := e.do(carol, http.MethodGet, "/services/1", nil, nil); p.status != http.StatusServiceUnavailable {
		t.Errorf("a group's page with the deployer down: %d", p.status)
	}
	// "services" is ok's, not a repository's.
	if p := e.post(dana, "/repositories/new", url.Values{"name": {"services"}}); !strings.Contains(p.body, "reserved") {
		t.Errorf("a repository named services: %s", excerpt(p.body))
	}
}
