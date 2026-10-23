package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/load"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// ok load run drives ok serve through the whole flow — publishes, checkpoints, CLs uploaded,
// approved, submitted, and landed by synthetic workers — and its report's checks hold; ok load cost
// prices it.
func TestLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: load@example.com, name: Load}\n"), 0o644)
	secret := func(name, value string) string {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte(value+"\n"), 0o600)
		return p
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	bucket := func(field string) string { return field + `: ` + testobjects.New(t).YAML(secret) }
	config := filepath.Join(dir, "ok.yaml")
	os.WriteFile(config, []byte(`
domain: example.com
installation: Example ok
public_url: http://`+addr+`
listen: `+addr+`
metrics_listen: 127.0.0.1:0
database: {url_file: `+secret("database-url", testdb.Postgres(t))+`}
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok}
identity:
  ca_dir: `+filepath.Join(dir, "ca")+`
  system_of_record: {static: {file: `+filepath.Join(dir, "people.yaml")+`}}
scm:
  `+bucket("objects")+`
automation:
  `+bucket("logs")+`
`), 0o644)
	done := make(chan error, 1)
	go func() {
		done <- serveRolesUntil(ctx, []string{"-config", config, "-roles", "identity,scm,automation,control", "-dev"})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ok serve: %v", err)
		}
	})
	for i := 0; ; i++ {
		if resp, err := http.Get("http://" + addr + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if i == 200 {
			t.Fatal("ok serve never became ready")
		}
		time.Sleep(50 * time.Millisecond)
	}

	reportPath := filepath.Join(dir, "report.json")
	var out bytes.Buffer
	args := []string{"-address", addr, "-insecure", "-dev-person", "load.human.example.com", "-dev-worker", "worker.ok.robot.example.com",
		// ~14 CLs offered over the run: one at least, all but certainly (the offers are random).
		"-repositories", "5", "-speed", "400", "-cls-per-day", "40", "-duration", "15s", "-drain", "60s", "-run-time", "200ms", "-workers", "4",
		"-publish-bytes", "200000", "-report", reportPath}
	var runErr error
	for i := 0; i < 50; i++ { // the system of record's first sync makes the load's person
		out.Reset()
		if runErr = loadRun(args, &out); runErr == nil || !strings.Contains(runErr.Error(), "load.human.example.com") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if runErr != nil {
		t.Fatalf("ok load run: %v\n%s", runErr, out.String())
	}
	var rep load.Report
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatal(err)
	}
	if !rep.Pass || rep.CLs.Landed == 0 || rep.Operations["publish"].OK == 0 || rep.Operations["checkpoint"].OK == 0 || rep.Runs.Jobs == 0 {
		t.Errorf("the report:\n%s", out.String())
	}
	for name, op := range rep.Operations {
		if len(op.Errors) > 0 {
			t.Errorf("%s's errors: %v", name, op.ErrorExamples)
		}
	}

	measured := filepath.Join(dir, "measured.json")
	os.WriteFile(measured, []byte(`{"seconds": 20, "ok_core_seconds": 4, "database_core_seconds": 2, "database_rows_written": 4000,
		"database_rows_read": 40000, "database_bytes": 1000000, "object_bytes": 50000000, "objects_written": 300}`), 0o644)
	out.Reset()
	if err := loadCost([]string{"-report", reportPath, "-measured", measured, "-prices", "../../deploy/load/gcp-us-central1.yaml"}, &out); err != nil {
		t.Fatalf("ok load cost: %v", err)
	}
	if !strings.Contains(out.String(), "the cell's Spanner compute") || !strings.Contains(out.String(), "an account: $") {
		t.Errorf("ok load cost:\n%s", out.String())
	}
}
