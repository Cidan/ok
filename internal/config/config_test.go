package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The configuration docs/configuration.md shows is one ok reads.
func TestDocsExample(t *testing.T) {
	doc, err := os.ReadFile("../../docs/configuration.md")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(doc), "```yaml\ndomain:")
	if start < 0 {
		t.Fatal("docs/configuration.md shows no configuration")
	}
	body := string(doc)[start+len("```yaml\n"):]
	body = body[:strings.Index(body, "```")]
	c, err := Parse([]byte(body))
	if err != nil {
		t.Fatalf("the documented configuration: %v", err)
	}
	if c.Identity.SystemOfRecord.Google == nil || c.Identity.SystemOfRecord.Interval != 10*time.Minute || len(c.Identity.Federation) != 1 || c.Roles.Control != "control.ok" ||
		c.SCM.Objects.Bucket != "example-ok-scm" || c.SCM.GCHour == nil || *c.SCM.GCHour != 9 || c.Services.Identity != "ok-identity.ok.svc:8472" || c.Workload.TokenFile == "" || c.Database.MaxConnections != 20 ||
		c.Tenancy.Hosted() || c.Tenancy.Organization != "example" {
		t.Errorf("parsed %+v", c)
	}
}

func TestDefaultsAndRefusals(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nidentity: {system_of_record: {static: {file: p.yaml}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8472" || c.MetricsListen != ":8473" || c.Installation != "ok" || c.Identity.SystemOfRecord.Interval != 10*time.Minute ||
		c.Bus.Stream.Type != "postgres" || c.Bus.Queue.Type != "postgres" || c.Tenancy.Mode != "installation" || c.Tenancy.Organization != "example" {
		t.Errorf("defaults: %+v", c)
	}
	if c, err := Parse([]byte("domain: corp.example.com\npublic_url: https://ok.example.com\ntenancy: {organization: acme}\n")); err != nil || c.Tenancy.Organization != "acme" {
		t.Errorf("an installation's organization named: %+v, %v", c.Tenancy, err)
	}
	if c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\n")); err != nil || !c.Tenancy.Hosted() {
		t.Errorf("a hosted service: %+v, %v", c.Tenancy, err)
	}
	signUp := "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\nidentity: {sign_in: {oidc: [{id: github}]}, " +
		"sign_up: {email: {smtp: {address: smtp.example.com:587, from: ok@example.com}}, oidc: [github]}}\n"
	if c, err := Parse([]byte(signUp)); err != nil || c.Identity.SignUp.Email.SMTP.Address != "smtp.example.com:587" || c.Identity.SignUp.OIDC[0] != "github" {
		t.Errorf("a hosted service's sign-up: %+v, %v", c.Identity.SignUp, err)
	}
	for name, bad := range map[string]string{
		"empty":                          "",
		"unknown key":                    "domain: example.com\npublic_url: https://ok.example.com\ndomian: x\n",
		"no domain":                      "public_url: https://ok.example.com\n",
		"a path in the url":              "domain: example.com\npublic_url: https://ok.example.com/ok\n",
		"a bad robot":                    "domain: example.com\npublic_url: https://ok.example.com\nroles: {scm: Bad_Name}\n",
		"two sources":                    "domain: example.com\npublic_url: https://ok.example.com\nidentity: {system_of_record: {static: {file: a}, scim: {token_file: b}}}\n",
		"a bad admin":                    "domain: example.com\npublic_url: https://ok.example.com\nidentity: {bootstrap_admins: [Ada Lovelace]}\n",
		"oidc ids twice":                 "domain: example.com\npublic_url: https://ok.example.com\nidentity: {sign_in: {oidc: [{id: g}, {id: g}]}}\n",
		"no audience":                    "domain: example.com\npublic_url: https://ok.example.com\nidentity: {federation: [{issuer: https://x}]}\n",
		"an unknown stream":              "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: carrier-pigeon}}\n",
		"nats, no url":                   "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: nats}}\n",
		"nats on postgres":               "domain: example.com\npublic_url: https://ok.example.com\nbus: {queue: {type: postgres, nats: {url: nats://n:4222}}}\n",
		"pubsub, no project":             "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: pubsub, pubsub: {}}}\n",
		"aws, no region":                 "domain: example.com\npublic_url: https://ok.example.com\nbus: {queue: {type: aws, aws: {}}}\n",
		"aws, half a key":                "domain: example.com\npublic_url: https://ok.example.com\nbus: {queue: {type: aws, aws: {region: r, access_key_file: k}}}\n",
		"a kafka queue":                  "domain: example.com\npublic_url: https://ok.example.com\nbus: {queue: {type: kafka, kafka: {brokers: [b:9092]}}}\n",
		"kafka, no brokers":              "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: kafka, kafka: {}}}\n",
		"kafka, scram, no password":      "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: kafka, kafka: {brokers: [b:9092], sasl: {mechanism: scram-sha-512, username: ok}}}}\n",
		"kafka, gssapi":                  "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: kafka, kafka: {brokers: [b:9092], sasl: {mechanism: gssapi}}}}\n",
		"a tenancy mode":                 "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: federated}\n",
		"a reserved organization":        "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {organization: settings}\n",
		"a bad organization":             "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {organization: Acme Corp}\n",
		"a hosted bare tree repository":  "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\nidentity: {tree_repository: org}\n",
		"a hosted system of record":      "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\nidentity: {system_of_record: {static: {file: p.yaml}}}\n",
		"an installation's sign-up":      "domain: example.com\npublic_url: https://ok.example.com\nidentity: {sign_up: {oidc: []}}\n",
		"sign-up, no smtp":               "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\nidentity: {sign_up: {email: {smtp: {}}}}\n",
		"sign-up, an unknown oidc":       "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\nidentity: {sign_up: {oidc: [github]}}\n",
		"a database type":                "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: oracle}\n",
		"spanner, no instance":           "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner}\n" + natsBus,
		"spanner, a bad instance":        "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: ok-test}}\n" + natsBus,
		"spanner, a long prefix":         "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/p/instances/i, prefix: a-very-long-prefix-xx}}\n" + natsBus,
		"spanner, a postgres url":        "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, url_file: u, spanner: {instance: projects/p/instances/i}}\n" + natsBus,
		"spanner, the postgres bus":      "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/p/instances/i}}\n",
		"spanner on postgres":            "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {spanner: {instance: projects/p/instances/i}}\n",
		"omni, no address":               "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/default/instances/default, omni: {plain_text: true}}}\n" + natsBus,
		"omni, half a password":          "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/default/instances/default, omni: {address: o:15000, username: ok}}}\n" + natsBus,
		"omni, plain and a ca":           "domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/default/instances/default, omni: {address: o:15000, plain_text: true, ca_file: c}}}\n" + natsBus,
		"a bad cell":                     "domain: example.com\npublic_url: https://ok.example.com\ncell: {name: Cell_1}\n",
		"a cell twice":                   "domain: example.com\npublic_url: https://ok.example.com\nidentity: {cells: [{name: c1, scm: a:1}, {name: c1, scm: b:1}]}\n",
		"a cell, no scm":                 "domain: example.com\npublic_url: https://ok.example.com\nidentity: {cells: [{name: c1}]}\n",
		"every cell closed":              "domain: example.com\npublic_url: https://ok.example.com\nidentity: {cells: [{name: c1, scm: a:1, closed: true}]}\n",
		"a default region no cell is in": "domain: example.com\npublic_url: https://ok.example.com\nidentity: {cells: [{name: c1, region: us, scm: a:1}], default_region: eu}\n",
		"half an edge limit":             "domain: example.com\npublic_url: https://ok.example.com\nedge: {limits: {principal: {per_second: 5}}}\n",
		"a negative edge limit":          "domain: example.com\npublic_url: https://ok.example.com\nedge: {limits: {address: {per_second: -1, burst: 4}}}\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

const natsBus = "bus: {stream: {type: nats, nats: {url: nats://n:4222}}, queue: {type: nats, nats: {url: nats://n:4222}}}\n"

// A Spanner database reads its instance, its prefix (ok unless set), and a Spanner Omni deployment
// to reach in place of Google's; each role's database is named for it.
func TestSpannerDatabase(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/p/instances/ok-test}}\n" + natsBus))
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Database.Spanner; s.Prefix != "ok" || s.Omni != nil || s.DatabaseName("deploy") != "projects/p/instances/ok-test/databases/ok-deploy" {
		t.Errorf("the database: %+v", s)
	}
	c, err = Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\ndatabase: {type: spanner, spanner: {instance: projects/default/instances/default, prefix: cell-1, " +
		"omni: {address: omni:15000, ca_file: /etc/ok/tls/omni-ca.crt, username: ok, password_file: /etc/ok/secrets/omni}}}\n" + natsBus))
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Database.Spanner; s.Omni == nil || s.Omni.Address != "omni:15000" || s.Omni.CAFile == "" || s.Omni.Username != "ok" ||
		s.DatabaseName("automation") != "projects/default/instances/default/databases/cell-1-automation" {
		t.Errorf("the database on Omni: %+v %+v", s, s.Omni)
	}
	if c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\n")); err != nil || c.Database.Type != "postgres" {
		t.Errorf("the default database: %+v, %v", c.Database, err)
	}
}

// The stream on Kafka reads its brokers, how to sign in, and how to keep the topics there.
func TestKafkaBus(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\n" +
		"bus: {stream: {type: kafka, kafka: {brokers: [b-1:9098, b-2:9098], tls: true, sasl: {mechanism: aws-msk-iam}, partitions: 24, replication_factor: 3}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	k := c.Bus.Stream.Kafka
	if k == nil || len(k.Brokers) != 2 || !k.TLS || k.SASL == nil || k.SASL.Mechanism != "aws-msk-iam" || k.Partitions != 24 || k.ReplicationFactor != 3 || c.Bus.Queue.Type != "postgres" {
		t.Errorf("the bus: %+v, %+v", k, c.Bus.Queue)
	}
}

// The bus on AWS reads its region, its key, and how to keep the topics and queues there.
func TestAWSBus(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\n" +
		"bus: {stream: {type: aws, aws: {region: eu-west-1}}, queue: {type: aws, aws: {region: eu-west-1, access_key_file: k, secret_key_file: s, retention: 72h, max_attempts: 4}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	s, q := c.Bus.Stream.AWS, c.Bus.Queue.AWS
	if s == nil || s.Region != "eu-west-1" || q == nil || q.AccessKeyFile != "k" || q.SecretKeyFile != "s" || q.Retention != 72*time.Hour || q.MaxAttempts != 4 {
		t.Errorf("the bus: %+v, %+v", s, q)
	}
}

// The bus on Pub/Sub reads its project and how to keep the topics there.
func TestPubSubBus(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\n" +
		"bus: {stream: {type: pubsub, pubsub: {project: p, endpoint: pubsub.us-east1.rep.googleapis.com:443}}, queue: {type: pubsub, pubsub: {project: p, max_attempts: 7}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	s, q := c.Bus.Stream.PubSub, c.Bus.Queue.PubSub
	if s == nil || s.Project != "p" || s.Endpoint != "pubsub.us-east1.rep.googleapis.com:443" || q == nil || q.MaxAttempts != 7 {
		t.Errorf("the bus: %+v, %+v", s, q)
	}
}

// The bus on NATS reads its servers and how to keep the streams there.
func TestNATSBus(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\n" +
		"bus: {stream: {type: nats, nats: {url: nats://a:4222, replicas: 3, retention: 72h}}, queue: {type: nats, nats: {url: nats://a:4222, ack_wait: 45s, max_attempts: 5}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	s, q := c.Bus.Stream.NATS, c.Bus.Queue.NATS
	if c.Bus.Stream.Type != "nats" || s == nil || s.URL != "nats://a:4222" || s.Replicas != 3 || s.Retention != 72*time.Hour ||
		q == nil || q.AckWait != 45*time.Second || q.MaxAttempts != 5 {
		t.Errorf("the bus: %+v, %+v, %+v", c.Bus, s, q)
	}
}

func TestSecret(t *testing.T) {
	p := t.TempDir() + "/s"
	os.WriteFile(p, []byte("hunter2\r\n"), 0o600)
	if s, err := Secret(p); err != nil || s != "hunter2" {
		t.Errorf("Secret = %q, %v", s, err)
	}
	if s, err := Secret(""); err != nil || s != "" {
		t.Errorf("no file: %q, %v", s, err)
	}
	if _, err := Secret(p + "-missing"); err == nil {
		t.Error("a missing file was read")
	}
}

// Every secret a configuration names is among SecretPaths, wherever it is nested; a CA's
// certificate, which is no secret, is not.
func TestSecretPaths(t *testing.T) {
	cfg, err := Parse([]byte(`
domain: example.com
public_url: https://ok.example.com
tls: {cert: /tls/tls.crt, key: /tls/tls.key}
database: {url_file: /s/db}
roles: {identity: identity.ok, worker: worker.ok}
services: {tls: true, ca_file: /tls/ca.crt, identity: id:8472, scm: scm:8472, automation: a:8472}
workload: {token_file: /var/run/secrets/ok/token}
identity:
  ca_dir: /etc/ok/ca
  sign_in:
    oidc: [{id: g, name: G, issuer: https://g.example.com, client_id: c, client_secret_file: /s/oidc, ca_file: /s/oidc-ca.crt}]
  system_of_record: {static: {file: /s/people.yaml}}
scm: {objects: {endpoint: https://s3, bucket: b, region: r, access_key_file: /s/ak, secret_key_file: /s/sk}}
worker: {state: /var/lib/ok}
deploy:
  state: /var/lib/ok-deploy
  migration_env: {APP_DB: {file: /s/app-db}, PLAIN: {value: x}}
`))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(cfg.SecretPaths(), " ")
	want := "/etc/ok/ca /s/ak /s/app-db /s/db /s/oidc /s/people.yaml /s/sk /tls/tls.key /var/run/secrets/ok/token"
	if got != want {
		t.Errorf("SecretPaths:\n got %s\nwant %s", got, want)
	}
}

// A role serves the cell default unless it names one; the directory's cells are identity.cells,
// or the one cell default, its roles at services'.
func TestCells(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nservices: {scm: scm:8472, automation: auto:8472}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cell.Name != DefaultCell {
		t.Errorf("the cell: %+v", c.Cell)
	}
	if cells := c.Cells(); len(cells) != 1 || cells[0].Name != DefaultCell || cells[0].SCM != "scm:8472" || cells[0].Automation != "auto:8472" || cells[0].Closed {
		t.Errorf("the default cells: %+v", cells)
	}
	c, err = Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\ncell: {name: us-1, region: us-east1}\n" +
		"identity: {cells: [{name: us-1, region: us-east1, scm: scm.us-1:8472, control: control.us-1:8472}, {name: eu-1, region: europe-west1, scm: scm.eu-1:8472, closed: true}]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cell != (Cell{Name: "us-1", Region: "us-east1"}) {
		t.Errorf("the cell: %+v", c.Cell)
	}
	if cells := c.Cells(); len(cells) != 2 || cells[0].Control != "control.us-1:8472" || !cells[1].Closed || cells[1].Region != "europe-west1" {
		t.Errorf("the cells: %+v", cells)
	}
	if c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nidentity: {cells: [{name: c1, region: us, scm: a:1}], default_region: us}\n")); err != nil || c.Identity.DefaultRegion != "us" {
		t.Errorf("a default region: %+v, %v", c.Identity, err)
	}
}

// The edge's limits default unless both halves are set; its robot is a role's.
func TestEdge(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nroles: {edge: edge.ok}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Edge.Limits.Principal != DefaultPrincipalRate || c.Edge.Limits.Address != DefaultAddressRate || c.Roles.All()["edge"] != "edge.ok" {
		t.Errorf("the edge's defaults: %+v, roles %v", c.Edge, c.Roles.All())
	}
	c, err = Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nedge: {region: us-east1, address_header: X-Real-IP, limits: {principal: {per_second: 5, burst: 10}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Edge.Region != "us-east1" || c.Edge.AddressHeader != "X-Real-IP" || c.Edge.Limits.Principal != (Rate{PerSecond: 5, Burst: 10}) || c.Edge.Limits.Address != DefaultAddressRate {
		t.Errorf("the edge: %+v", c.Edge)
	}
}

// External secret stores: each kind's own fields, and nothing of another kind's; a name of a
// store's form, and never default.
func TestSecretStores(t *testing.T) {
	const base = "domain: example.com\npublic_url: https://ok.example.com\nroles: {secrets: secrets.ok}\nsecrets:\n  root: keyring:/etc/ok/keyring\n  stores:\n"
	c, err := Parse([]byte(base + "    vault: {kind: vault, address: https://vault.internal:8200, mount: ok, role: ok-secrets}\n" +
		"    aws: {kind: aws-secrets-manager, region: us-east-1}\n    gcp: {kind: gcp-secret-manager, project: acme-secrets, locations: [us-east1]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Secrets.Stores; s["vault"].Mount != "ok" || s["vault"].Role != "ok-secrets" || s["aws"].Region != "us-east-1" ||
		s["gcp"].Project != "acme-secrets" || len(s["gcp"].Locations) != 1 {
		t.Errorf("the stores: %+v", s)
	}
	for stores, want := range map[string]string{
		"    default: {kind: aws-secrets-manager, region: us-east-1}\n":                     "secrets.stores.default: \"default\" is ok secrets' own database's name",
		"    Vault: {kind: vault, address: https://v, mount: ok, role: r}\n":                "secrets.stores.Vault: a store's name is lowercase",
		"    v: {kind: vault, address: vault.internal, mount: ok, role: r}\n":               "secrets.stores.v.address: Vault's address",
		"    v: {kind: vault, address: https://v, mount: /ok/, role: r}\n":                  "secrets.stores.v.mount:",
		"    v: {kind: vault, address: https://v, mount: ok}\n":                             "secrets.stores.v.role:",
		"    v: {kind: vault, address: https://v, mount: ok, role: r, region: us-east-1}\n": "secrets.stores.v.kind: a vault store has address",
		"    a: {kind: aws-secrets-manager}\n":                                              "secrets.stores.a.region:",
		"    a: {kind: aws-secrets-manager, region: us-east-1, project: p}\n":               "secrets.stores.a.kind: an aws-secrets-manager store has",
		"    g: {kind: gcp-secret-manager}\n":                                               "secrets.stores.g.project:",
		"    x: {kind: s3}\n":                                                               `secrets.stores.x.kind: vault, aws-secrets-manager, or gcp-secret-manager, not "s3"`,
	} {
		if _, err := Parse([]byte(base + stores)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", stores, err, want)
		}
	}
}
