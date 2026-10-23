// Package config is an ok installation's configuration: one YAML file every role reads. Secrets
// are never in it: a field ending in _file names a file holding the secret — a mounted Kubernetes
// Secret — read when the role starts.
package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/identity"
)

// Config is an installation.
type Config struct {
	// Domain is what every name is under: ada.human.example.com.
	Domain string `yaml:"domain"`
	// Installation names it on pages: "Example ok".
	Installation string `yaml:"installation"`
	// PublicURL is where people and command lines reach ok: https://ok.example.com.
	PublicURL string `yaml:"public_url"`
	// Listen and MetricsListen are where a role serves, and serves its metrics.
	Listen        string `yaml:"listen"`
	MetricsListen string `yaml:"metrics_listen"`
	// TLS, when set, is the certificate a role serves with; without it, roles serve cleartext
	// HTTP/2 for a proxy or mesh in front to secure.
	TLS *TLS `yaml:"tls"`
	// Database is the PostgreSQL every role keeps its tables in, each under its own names.
	Database Database `yaml:"database"`
	// Bus is how the roles hear of what happened and hand out work.
	Bus Bus `yaml:"bus"`
	// Roles are the robots ok's roles run as, by role: what precedes .robot.<domain>.
	Roles Roles `yaml:"roles"`
	// Identity is the identity role's.
	Identity Identity `yaml:"identity"`
	// Services are where each role is reached: host:port, for roles in other processes.
	Services Services `yaml:"services"`
	// Workload is how a role other than identity proves itself to the others.
	Workload Workload `yaml:"workload"`
	// SCM is the SCM role's.
	SCM SCM `yaml:"scm"`
	// Automation is ok automation's, and Worker its workers'.
	Automation Automation `yaml:"automation"`
	Worker     Worker     `yaml:"worker"`
	// Deploy is the deployer's.
	Deploy Deploy `yaml:"deploy"`
}

// Deploy is the deployer's configuration.
type Deploy struct {
	// State is where it keeps its checkout, its okfs daemon's state and caches, the graphs it has
	// queried, and Bazel's output base.
	State string `yaml:"state"`
	// Self is the deployer's own Deployment, which it rolls last and from its old copy.
	Self DeploySelf `yaml:"self"`
	// Checkout is how it checks commits out: virtual (okfs, which needs /dev/fuse; the default) or
	// real (every file written to disk).
	Checkout string `yaml:"checkout"`
	// JJ, Bazel, and BazelFlags are as a worker's.
	JJ         string   `yaml:"jj"`
	Bazel      string   `yaml:"bazel"`
	BazelFlags []string `yaml:"bazel_flags"`
	// MigrationEnv are the variables the migrations it runs get, beside its PATH and HOME: where
	// each finds its database. Each is a value, or a file holding one (a mounted Secret).
	MigrationEnv map[string]EnvValue `yaml:"migration_env"`
	// Kubeconfig is a kubeconfig file, for a deployer outside the cluster it rolls; unset, it
	// uses its pod's ServiceAccount.
	Kubeconfig string `yaml:"kubeconfig"`
}

// DeploySelf is the deployer's own Deployment.
type DeploySelf struct {
	Namespace  string `yaml:"namespace"`
	Deployment string `yaml:"deployment"`
}

// EnvValue is an environment variable's value, or the file holding it.
type EnvValue struct {
	Value string `yaml:"value"`
	File  string `yaml:"file"`
}

// Resolve is the variable's value: its file's content, trimmed, or its value.
func (v EnvValue) Resolve() (string, error) {
	if v.File == "" {
		return v.Value, nil
	}
	return Secret(v.File)
}

// Automation is ok automation's configuration.
type Automation struct {
	// Logs is the object store finished runs' logs are kept in.
	Logs Objects `yaml:"logs"`
	// SpeculationDepth bounds how many earlier submissions the landing queue's attempt speculates
	// on: 8 unless set.
	SpeculationDepth *int `yaml:"speculation_depth"`
}

// Worker is an ok automation worker's configuration.
type Worker struct {
	// Name is the worker's name among the workers: the host's (its pod's) unless set.
	Name string `yaml:"name"`
	// State is where it keeps its slots' checkouts, Bazel's output bases, kept graphs, and its
	// okfs daemon's state and caches.
	State string `yaml:"state"`
	// Slots is how many runs it runs at once: 1 unless set.
	Slots int `yaml:"slots"`
	// RunMemoryGiB is the memory it keeps free under its cgroup's limit for each run it starts
	// beside the others: 4 unless set.
	RunMemoryGiB float64 `yaml:"run_memory_gib"`
	// Checkout is how it checks commits out: virtual (okfs, which needs /dev/fuse; the default) or
	// real (every file written to disk).
	Checkout string `yaml:"checkout"`
	// JJ is ok's jj, the okfs daemon, for virtual checkouts: jj on the PATH unless set.
	JJ string `yaml:"jj"`
	// Bazel is Bazel's path, or bazelisk's: bazelisk, else bazel, on the PATH unless set.
	Bazel string `yaml:"bazel"`
	// BazelFlags go to every Bazel command the worker runs: a remote cache, say.
	BazelFlags []string `yaml:"bazel_flags"`
	// Bwrap is bubblewrap's path: bwrap on the PATH unless set.
	Bwrap string `yaml:"bwrap"`
}

// SCM is the SCM role's configuration.
type SCM struct {
	// Objects is the object store file contents are kept in.
	Objects Objects `yaml:"objects"`
	// EmailDomain is what a person's handle is an address at in the commits ok writes for them
	// (landings, rollbacks), and what reviewers may be named with: <handle>@<email_domain>. It is
	// the domain unless set.
	EmailDomain string `yaml:"email_domain"`
	// GCHour is the hour (UTC, 0–23) each repository's daily garbage collection is due from.
	GCHour *int `yaml:"gc_hour"`
	// Cache bounds the SCM's caches, in bytes; zero for the defaults.
	Cache SCMCache `yaml:"cache"`
}

// SCMCache bounds the SCM's in-memory caches.
type SCMCache struct {
	Objects int `yaml:"objects"`
	Chunks  int `yaml:"chunks"`
	Diffs   int `yaml:"diffs"`
}

// Objects is an object store: an S3-compatible bucket (type s3, the default), a Google Cloud Storage
// bucket (gcs), or a directory (fs). Without its credential files, an S3 bucket's credentials come
// from AWS's chain (its variables, its credentials file, the instance's or pod's role), and a GCS
// bucket's from Google's application default chain (GOOGLE_APPLICATION_CREDENTIALS, the workload's
// identity).
type Objects struct {
	Type          string `yaml:"type"`     // s3, gcs, or fs
	Endpoint      string `yaml:"endpoint"` // s3: https://s3.us-east-1.amazonaws.com, http://minio:9000; gcs: an emulator's
	Bucket        string `yaml:"bucket"`   // s3, gcs
	Region        string `yaml:"region"`   // s3
	AccessKeyFile string `yaml:"access_key_file"`
	SecretKeyFile string `yaml:"secret_key_file"`
	// CredentialsFile is a GCS bucket's service-account JSON key.
	CredentialsFile string `yaml:"credentials_file"`
	// Dir is an fs store's directory.
	Dir string `yaml:"dir"`
}

// Bus is how the roles hear of what happened — the stream — and hand out work — the queue
// (docs/hosted.md, "Events and the bus"), each a backend of its own.
type Bus struct {
	Stream BusBackend `yaml:"stream"`
	Queue  BusBackend `yaml:"queue"`
}

// BusBackend is one of the bus's backends: postgres, the default, keeps it in the installation's
// database, in a schema of its own (bus).
type BusBackend struct {
	Type string `yaml:"type"`
}

// TLS is a certificate and key, as files.
type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// Database is how to reach PostgreSQL.
type Database struct {
	// URLFile holds the connection URL (postgres://…), which carries the password.
	URLFile string `yaml:"url_file"`
	// MaxConnections bounds each role's pool; 0 is the driver's default.
	MaxConnections int32 `yaml:"max_connections"`
}

// Roles are the robots the roles run as.
type Roles struct {
	Identity   string `yaml:"identity"`
	SCM        string `yaml:"scm"`
	Control    string `yaml:"control"`
	Automation string `yaml:"automation"`
	Worker     string `yaml:"worker"`
	Deploy     string `yaml:"deploy"`
}

// All are the roles' robots by role name, for those named.
func (r Roles) All() map[string]string {
	out := map[string]string{}
	for k, v := range map[string]string{"identity": r.Identity, "scm": r.SCM, "control": r.Control, "automation": r.Automation, "worker": r.Worker, "deploy": r.Deploy} {
		if v != "" {
			out[k] = v
		}
	}
	return out
}

// Services are where the roles reach each other: host:port each.
type Services struct {
	// TLS: the roles serve TLS, their certificates verified against CAFile, or the system's roots
	// without it. Off, they speak cleartext HTTP/2, for a network that secures it otherwise — a
	// service mesh.
	TLS        bool   `yaml:"tls"`
	CAFile     string `yaml:"ca_file"`
	Identity   string `yaml:"identity"`
	SCM        string `yaml:"scm"`
	Automation string `yaml:"automation"`
	Deploy     string `yaml:"deploy"`
}

// Workload is how a role other than identity proves itself to the others: a credential for a key
// it holds, from one of two sources.
type Workload struct {
	// TokenFile holds a token an issuer under identity.federation signs for the role's robot — a
	// projected Kubernetes ServiceAccount token — with which the role certifies a key it makes at
	// start, and again whenever its credential lapses.
	TokenFile string `yaml:"token_file"`
	// CredentialDir holds a key and certificate an administrator certified for the robot (ok admin
	// robot certify), renewed in place.
	CredentialDir string `yaml:"credential_dir"`
}

// Identity is the identity role's configuration.
type Identity struct {
	// CADir holds the identity CA: tls.crt and tls.key, and optionally trusted.crt.
	CADir string `yaml:"ca_dir"`
	// BootstrapAdmins administer ok until a groups-and-ACLs tree is applied: handles.
	BootstrapAdmins []string `yaml:"bootstrap_admins"`
	// TreeRepository names the repository whose groups-and-ACLs tree (the folder its settings name
	// as identity:) is ok's: the deployer applies it, and its changes are approved by the ACLs that
	// own what they change. Other repositories' identity: folders are ordinary files. Unset: no
	// repository's tree is ok's.
	TreeRepository string  `yaml:"tree_repository"`
	Session        Session `yaml:"session"`
	// TrustedHeader, when set, is a header an authenticating proxy in front of ok writes with the
	// principal's name, and strips from every request it forwards.
	TrustedHeader  string          `yaml:"trusted_header"`
	SignIn         SignIn          `yaml:"sign_in"`
	SystemOfRecord *SystemOfRecord `yaml:"system_of_record"`
	Federation     []Issuer        `yaml:"federation"`
	// Robots are made at start, if they are not, with their bindings: the roles' own, typically.
	Robots []Robot `yaml:"robots"`
}

// Session bounds a browser session.
type Session struct {
	Lifetime time.Duration `yaml:"lifetime"`
	Idle     time.Duration `yaml:"idle"`
}

// SignIn is how people sign in in a browser.
type SignIn struct {
	OIDC []OIDC `yaml:"oidc"`
	LDAP *LDAP  `yaml:"ldap"`
}

// OIDC is an OpenID Connect provider.
type OIDC struct {
	ID                   string   `yaml:"id"`
	Name                 string   `yaml:"name"`
	Issuer               string   `yaml:"issuer"`
	ClientID             string   `yaml:"client_id"`
	ClientSecretFile     string   `yaml:"client_secret_file"`
	Scopes               []string `yaml:"scopes"`
	Match                string   `yaml:"match"` // email, or external_id
	Claim                string   `yaml:"claim"`
	TrustUnverifiedEmail bool     `yaml:"trust_unverified_email"`
	HostedDomain         string   `yaml:"hosted_domain"`
	// CAFile verifies the provider's certificate, when the system does not trust its CA.
	CAFile string `yaml:"ca_file"`
}

// LDAP is an LDAP directory people sign in to.
type LDAP struct {
	Name             string `yaml:"name"`
	URL              string `yaml:"url"`
	StartTLS         bool   `yaml:"start_tls"`
	CAFile           string `yaml:"ca_file"`
	BindDN           string `yaml:"bind_dn"`
	BindPasswordFile string `yaml:"bind_password_file"`
	BaseDN           string `yaml:"base_dn"`
	UserFilter       string `yaml:"user_filter"`
	Match            string `yaml:"match"`
	Attribute        string `yaml:"attribute"`
}

// SystemOfRecord is where people come from: exactly one of its sources.
type SystemOfRecord struct {
	// Interval is how often the sync runs; default ten minutes.
	Interval time.Duration `yaml:"interval"`
	Static   *StaticSource `yaml:"static"`
	Google   *GoogleSource `yaml:"google"`
	Entra    *EntraSource  `yaml:"entra"`
	LDAP     *LDAPSource   `yaml:"ldap"`
	SCIM     *SCIMSource   `yaml:"scim"`
}

// StaticSource is a YAML file of people and groups.
type StaticSource struct {
	File string `yaml:"file"`
}

// GoogleSource is a Google Workspace.
type GoogleSource struct {
	KeyFile    string `yaml:"key_file"`
	Subject    string `yaml:"subject"`
	Customer   string `yaml:"customer"`
	GroupQuery string `yaml:"group_query"`
}

// EntraSource is a Microsoft Entra ID tenant.
type EntraSource struct {
	TenantID         string `yaml:"tenant_id"`
	ClientID         string `yaml:"client_id"`
	ClientSecretFile string `yaml:"client_secret_file"`
	GroupFilter      string `yaml:"group_filter"`
	IncludeGuests    bool   `yaml:"include_guests"`
}

// LDAPSource is an LDAP directory.
type LDAPSource struct {
	URL              string     `yaml:"url"`
	StartTLS         bool       `yaml:"start_tls"`
	CAFile           string     `yaml:"ca_file"`
	BindDN           string     `yaml:"bind_dn"`
	BindPasswordFile string     `yaml:"bind_password_file"`
	People           LDAPPeople `yaml:"people"`
	Groups           LDAPGroups `yaml:"groups"`
}

// LDAPPeople is where people are in an LDAP directory.
type LDAPPeople struct {
	BaseDN          string `yaml:"base_dn"`
	Filter          string `yaml:"filter"`
	DisabledFilter  string `yaml:"disabled_filter"`
	IDAttribute     string `yaml:"id_attribute"`
	HandleAttribute string `yaml:"handle_attribute"`
	EmailAttribute  string `yaml:"email_attribute"`
	NameAttribute   string `yaml:"name_attribute"`
}

// LDAPGroups is where groups are in an LDAP directory.
type LDAPGroups struct {
	BaseDN          string `yaml:"base_dn"`
	Filter          string `yaml:"filter"`
	IDAttribute     string `yaml:"id_attribute"`
	NameAttribute   string `yaml:"name_attribute"`
	MemberAttribute string `yaml:"member_attribute"`
}

// SCIMSource is what an identity provider provisions over SCIM, at <public_url>/id/scim/v2.
type SCIMSource struct {
	TokenFile string `yaml:"token_file"`
}

// Issuer is an issuer whose tokens robots sign in with.
type Issuer struct {
	Issuer    string `yaml:"issuer"`
	Audience  string `yaml:"audience"`
	JWKSURL   string `yaml:"jwks_url"`
	CAFile    string `yaml:"ca_file"`
	TokenFile string `yaml:"token_file"`
}

// Robot is a robot to make at start, and its bindings.
type Robot struct {
	Name     string    `yaml:"name"`
	Bindings []Binding `yaml:"bindings"`
}

// Binding is an issuer's subject that names a robot.
type Binding struct {
	Issuer  string `yaml:"issuer"`
	Subject string `yaml:"subject"`
}

// Load reads and checks a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse reads and checks a configuration: unknown keys are refused, so a misspelling is not a
// silently missing setting.
func Parse(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("the configuration is empty")
		}
		return nil, fmt.Errorf("the configuration: %w", err)
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Names is the naming under the installation's domain.
func (c *Config) Names() identity.Names { return identity.MustNames(c.Domain) }

// RobotName is a robot's full name from what precedes .robot.<domain>.
func (c *Config) RobotName(short string) string { return c.Names().Robot(short) }

func (c *Config) check() error {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	if _, err := identity.NewNames(c.Domain); err != nil {
		add("domain: %v", err)
	}
	if u, err := url.Parse(c.PublicURL); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || (u.Path != "" && u.Path != "/") {
		add("public_url is where ok is reached: https://ok.example.com")
	}
	if c.Installation == "" {
		c.Installation = "ok"
	}
	if c.Listen == "" {
		c.Listen = ":8472"
	}
	if c.MetricsListen == "" {
		c.MetricsListen = ":8473"
	}
	if c.TLS != nil && (c.TLS.Cert == "" || c.TLS.Key == "") {
		add("tls names a cert and a key")
	}
	for field, b := range map[string]*BusBackend{"bus.stream": &c.Bus.Stream, "bus.queue": &c.Bus.Queue} {
		switch b.Type {
		case "":
			b.Type = "postgres"
		case "postgres":
		default:
			add("%s.type: %q is not postgres", field, b.Type)
		}
	}
	if c.Domain != "" && len(problems) == 0 {
		names := c.Names()
		for role, robot := range c.Roles.All() {
			if k, ok := names.KindOf(names.Robot(robot)); !ok || k != identity.Robot {
				add("roles.%s: %q is not a robot's name, one or two lowercase DNS labels", role, robot)
			}
		}
		for _, r := range c.Identity.Robots {
			if k, ok := names.KindOf(names.Robot(r.Name)); !ok || k != identity.Robot {
				add("identity.robots: %q is not a robot's name", r.Name)
			}
			for _, b := range r.Bindings {
				if b.Issuer == "" || b.Subject == "" {
					add("identity.robots.%s: a binding names an issuer and a subject", r.Name)
				}
			}
		}
		for _, h := range c.Identity.BootstrapAdmins {
			if k, ok := names.KindOf(names.Human(h)); !ok || k != identity.Human {
				add("identity.bootstrap_admins: %q is not a handle", h)
			}
		}
	}
	seen := map[string]bool{}
	for _, o := range c.Identity.SignIn.OIDC {
		if o.ID == "" || !identity.ValidLabel(o.ID) || seen[o.ID] {
			add("identity.sign_in.oidc: each provider has its own id, a lowercase DNS label")
		}
		seen[o.ID] = true
		if o.Match != "" && o.Match != "email" && o.Match != "external_id" {
			add("identity.sign_in.oidc.%s: match is email or external_id", o.ID)
		}
	}
	if s := c.Identity.SystemOfRecord; s != nil {
		n := 0
		for _, set := range []bool{s.Static != nil, s.Google != nil, s.Entra != nil, s.LDAP != nil, s.SCIM != nil} {
			if set {
				n++
			}
		}
		if n != 1 {
			add("identity.system_of_record names exactly one source: static, google, entra, ldap, or scim")
		}
		if s.Interval == 0 {
			s.Interval = 10 * time.Minute
		}
	}
	if h := c.SCM.GCHour; h != nil && (*h < 0 || *h > 23) {
		add("scm.gc_hour is an hour, 0 to 23")
	}
	if c.Workload.TokenFile != "" && c.Workload.CredentialDir != "" {
		add("workload names a token_file or a credential_dir, not both")
	}
	if d := c.Automation.SpeculationDepth; d != nil && *d < 0 {
		add("automation.speculation_depth is a count, 0 or more")
	}
	switch c.Worker.Checkout {
	case "":
		c.Worker.Checkout = "virtual"
	case "virtual", "real":
	default:
		add("worker.checkout is virtual or real, not %q", c.Worker.Checkout)
	}
	if c.Worker.Slots < 0 || c.Worker.RunMemoryGiB < 0 {
		add("worker.slots and worker.run_memory_gib are counts, 0 or more")
	}
	switch c.Deploy.Checkout {
	case "":
		c.Deploy.Checkout = "virtual"
	case "virtual", "real":
	default:
		add("deploy.checkout is virtual or real, not %q", c.Deploy.Checkout)
	}
	if c.Deploy.Self.Namespace == "" && c.Deploy.Self.Deployment == "" {
		c.Deploy.Self = DeploySelf{Namespace: "ok", Deployment: "ok-deploy"}
	}
	for k, v := range c.Deploy.MigrationEnv {
		if (v.Value == "") == (v.File == "") {
			add("deploy.migration_env.%s is a value or a file, one of them", k)
		}
	}
	if c.Worker.Slots == 0 {
		c.Worker.Slots = 1
	}
	if c.Worker.RunMemoryGiB == 0 {
		c.Worker.RunMemoryGiB = 4
	}
	if c.SCM.EmailDomain == "" {
		c.SCM.EmailDomain = c.Domain
	}
	for _, i := range c.Identity.Federation {
		if i.Issuer == "" || i.Audience == "" {
			add("identity.federation: each issuer names its URL and the audience ok expects")
		}
	}
	if len(problems) > 0 {
		return errors.New("the configuration:\n  " + strings.Join(problems, "\n  "))
	}
	return nil
}

// Secret reads a secret's file: its content, without the trailing newline editors and kubectl add.
func Secret(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("the secret file: %w", err)
	}
	return strings.TrimRight(string(data), "\r\n"), nil
}

// Open makes a client of the store, reading its key files; field names it in errors
// (scm.objects). With ensure, an S3 bucket is made if it does not exist: local development.
func (o Objects) Open(ctx context.Context, field string, ensure bool) (blob.Store, error) {
	switch o.Type {
	case "", "s3":
		return o.openS3(ctx, field, ensure)
	case "gcs":
		g, err := blob.NewGCS(ctx, blob.GCSConfig{Bucket: o.Bucket, CredentialsFile: o.CredentialsFile, Endpoint: o.Endpoint})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		return g, nil
	case "fs":
		f, err := blob.NewFS(o.Dir)
		if err != nil {
			return nil, fmt.Errorf("%s.dir: %w", field, err)
		}
		return f, nil
	}
	return nil, fmt.Errorf("%s.type: %q is none of s3, gcs, and fs", field, o.Type)
}

func (o Objects) openS3(ctx context.Context, field string, ensure bool) (blob.Store, error) {
	access, err := Secret(o.AccessKeyFile)
	if err != nil {
		return nil, fmt.Errorf("%s.access_key_file: %w", field, err)
	}
	secret, err := Secret(o.SecretKeyFile)
	if err != nil {
		return nil, fmt.Errorf("%s.secret_key_file: %w", field, err)
	}
	s3, err := blob.NewS3(blob.Config{Endpoint: o.Endpoint, Bucket: o.Bucket, Region: o.Region, AccessKey: access, SecretKey: secret})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", field, err)
	}
	if ensure {
		if err := s3.EnsureBucket(ctx, o.Region); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	return s3, nil
}
