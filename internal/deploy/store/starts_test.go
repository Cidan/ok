package store

import (
	"testing"

	"github.com/Cidan/ok/internal/workflow"
)

// A deployment that runs alone waits for every running deployment, and none starts while it
// runs.
func TestOccupancy(t *testing.T) {
	w := func(ns, name string) *workflow.Workload {
		return &workflow.Workload{Namespace: ns, Kind: workflow.Deployment, Name: name}
	}
	web := &Deployment{Deploy: "//a:deploy:web", Workload: w("web", "web")}
	api := &Deployment{Deploy: "//a:deploy:api", Workload: w("api", "api")}
	db := &Deployment{Deploy: "//a:deploy:db", Workload: w("postgres", "postgres"), Alone: true}
	idle := occupancy{claims: map[string]bool{}}
	if !idle.admits(db) || !idle.admits(web) {
		t.Error("nothing running admits nothing")
	}
	busy := occupancy{claims: map[string]bool{}}
	busy.take(web)
	if busy.admits(db) || busy.admits(web) || !busy.admits(api) {
		t.Errorf("with web running: db %v, web %v, api %v", busy.admits(db), busy.admits(web), busy.admits(api))
	}
	alone := occupancy{claims: map[string]bool{}}
	alone.take(db)
	if alone.admits(api) || alone.admits(web) {
		t.Error("a deployment started while one that runs alone runs")
	}
}
