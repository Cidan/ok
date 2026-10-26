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
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/workflow"
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
	// Profiling serves Go's profiles (/debug/pprof/) beside the metrics: what a role spends its
	// CPU and memory on under load. The metrics' port is the cluster's alone, not the public one.
	Profiling bool `yaml:"profiling"`
	// TLS, when set, is the certificate a role serves with; without it, roles serve cleartext
	// HTTP/2 for a proxy or mesh in front to secure.
	TLS *TLS `yaml:"tls"`
	// Database is where the roles keep their tables: one PostgreSQL database, each role's under its
	// own names, or a Spanner instance, each role's in a database of its own.
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
	// Control is ok control's.
	Control Control `yaml:"control"`
	// Deploy is the deployer's.
	Deploy Deploy `yaml:"deploy"`
	// Secrets is ok secrets' (docs/secrets.md).
	Secrets Secrets `yaml:"secrets"`
	// Tenancy is who owns what (docs/hosted.md, "Tenancy, as built").
	Tenancy Tenancy `yaml:"tenancy"`
	// Cell is the cell a role serves (docs/hosted.md, "Cells, as built"): every role's but
	// identity's, which serves them all. A single installation's is default.
	Cell Cell `yaml:"cell"`
	// Edge is the edge's, which serves no cell.
	Edge Edge `yaml:"edge"`
}

// Edge is how the edge, in front of the cells, serves (docs/hosted.md, "Cells, as built").
type Edge struct {
	// Region is the edge's: a request that names no repository goes to a cell there.
	Region string `yaml:"region"`
	// AddressHeader, when set, is a header a proxy in front of the edge writes with the client's
	// address, by which rate limits count it; without it, the connection's.
	AddressHeader string `yaml:"address_header"`
	// Limits bound each principal's requests, and each address's, at each edge replica.
	Limits EdgeLimits `yaml:"limits"`
}

// EdgeLimits are the edge's rate limits.
type EdgeLimits struct {
	Principal Rate `yaml:"principal"`
	Address   Rate `yaml:"address"`
}

// Rate is a token bucket: requests a second, and how many at once beyond it.
type Rate struct {
	PerSecond float64 `yaml:"per_second"`
	Burst     int     `yaml:"burst"`
}

// The edge's default limits, for a configuration that leaves them zero: an address may be many
// people behind one gateway.
var (
	DefaultPrincipalRate = Rate{PerSecond: 50, Burst: 200}
	DefaultAddressRate   = Rate{PerSecond: 200, Burst: 800}
)

// Cell names a role's cell, and its region.
type Cell struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"`
}

// DefaultCell is the name of a cell no configuration names: a single installation's.
const DefaultCell = "default"

// IdentityCell is a cell as the directory knows it.
type IdentityCell struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"`
	// Closed: no new repository is placed there; it keeps what it holds.
	Closed bool `yaml:"closed"`
	// Where its roles are reached, host:port each, by the edge and by other cells.
	SCM        string `yaml:"scm"`
	Control    string `yaml:"control"`
	Automation string `yaml:"automation"`
	Deploy     string `yaml:"deploy"`
	Secrets    string `yaml:"secrets"`
}

// Cells are the cells the directory places repositories in: identity.cells, or with none the one
// cell default, its roles at services'. The first holds the repositories made before there were
// cells.
func (c *Config) Cells() []IdentityCell {
	if len(c.Identity.Cells) > 0 {
		return c.Identity.Cells
	}
	return []IdentityCell{{Name: DefaultCell, Region: c.Cell.Region, SCM: c.Services.SCM, Automation: c.Services.Automation,
		Deploy: c.Services.Deploy, Secrets: c.Services.Secrets}}
}

// Tenancy is how the installation holds organizations.
type Tenancy struct {
	// Mode is installation (the default: one organization, the system of record's people its
	// members) or hosted (any number of organizations, which people make).
	Mode string `yaml:"mode"`
	// Organization is an installation's organization's handle: by default the domain's first
	// label.
	Organization string `yaml:"organization"`
}

// Hosted reports whether the installation is a hosted service.
func (t Tenancy) Hosted() bool { return t.Mode == "hosted" }

// Secrets is ok secrets' configuration (docs/secrets.md, "Keys").
type Secrets struct {
	// Root is the root that wraps repositories' keys, unless an organization names its own:
	// keyring:<file> (a file of AES-256 keys, outside any database), gcp-kms:<crypto key's
	// resource name>, or aws-kms:<key ARN>.
	Root string `yaml:"root"`
	// Database, when set, is ok secrets' own database, apart from the installation's: so a
	// credential to the others' reaches none of its tables.
	Database *Database `yaml:"database"`
	// Stores are the external stores a repository's settings may name (secrets.store) to keep its
	// sealed values in, by name: each a service ok secrets reaches as its workload, with no
	// credential here (docs/secrets.md, "Stores").
	Stores map[string]SecretStore `yaml:"stores"`
}

// Kinds of external secret store.
const (
	StoreVault             = "vault"
	StoreAWSSecretsManager = "aws-secrets-manager"
	StoreGCPSecretManager  = "gcp-secret-manager"
)

// SecretStore is an external store of sealed values.
type SecretStore struct {
	// Kind is vault, aws-secrets-manager, or gcp-secret-manager.
	Kind string `yaml:"kind"`
	// Vault's: its address; its KV version 2 mount; the role ok secrets logs in as with its
	// Kubernetes service account's token — token_file, the pod's own unless set — at
	// auth/<auth_mount> (kubernetes unless set); its namespace (Vault Enterprise's), if any; and
	// the CA certificates its own is checked against (the system's unless set).
	Address   string `yaml:"address"`
	Mount     string `yaml:"mount"`
	Role      string `yaml:"role"`
	TokenFile string `yaml:"token_file"`
	AuthMount string `yaml:"auth_mount"`
	Namespace string `yaml:"namespace"`
	CAFile    string `yaml:"ca_file"`
	// AWS Secrets Manager's: its region, and the KMS key it encrypts what it keeps under (its
	// own, aws/secretsmanager, unless set).
	Region   string `yaml:"region"`
	KMSKeyID string `yaml:"kms_key_id"`
	// Google Secret Manager's: the project, and the locations each secret is replicated to
	// (where Google chooses, unless set).
	Project   string   `yaml:"project"`
	Locations []string `yaml:"locations"`
}

// problems are what is wrong with a store's configuration, each starting with where.
func (s SecretStore) problems(where string) []string {
	var out []string
	add := func(format string, args ...any) { out = append(out, where+"."+fmt.Sprintf(format, args...)) }
	vault := s.Address != "" || s.Mount != "" || s.Role != "" || s.TokenFile != "" || s.AuthMount != "" || s.Namespace != "" || s.CAFile != ""
	aws := s.Region != "" || s.KMSKeyID != ""
	gcp := s.Project != "" || len(s.Locations) > 0
	switch s.Kind {
	case StoreVault:
		if u, err := url.Parse(s.Address); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			add("address: Vault's address, https://vault.example.com:8200")
		}
		if s.Mount == "" || strings.Trim(s.Mount, "/") != s.Mount {
			add("mount: the KV version 2 mount ok secrets keeps sealed values in, with no slash at either end")
		}
		if s.Role == "" {
			add("role: the role ok secrets logs in as, with its Kubernetes service account's token")
		}
		if aws || gcp {
			add("kind: a vault store has address, mount, role, token_file, auth_mount, namespace, and ca_file")
		}
	case StoreAWSSecretsManager:
		if s.Region == "" {
			add("region: the AWS region Secrets Manager is reached in")
		}
		if vault || gcp {
			add("kind: an aws-secrets-manager store has region and kms_key_id")
		}
	case StoreGCPSecretManager:
		if s.Project == "" {
			add("project: the Google Cloud project Secret Manager keeps the secrets in")
		}
		if vault || aws {
			add("kind: a gcp-secret-manager store has project and locations")
		}
	default:
		add("kind: vault, aws-secrets-manager, or gcp-secret-manager, not %q", s.Kind)
	}
	return out
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

// Control is ok control's configuration.
type Control struct {
	// SealingKeyFile is the key webhooks' secrets are sealed under (docs/hosted.md, "ok control at
	// scale, as built"): the same file in every cell, so a repository moved keeps its webhooks.
	// Unset, repositories have no webhooks.
	SealingKeyFile string   `yaml:"sealing_key_file"`
	Webhooks       Webhooks `yaml:"webhooks"`
}

// Webhooks are how ok control delivers webhooks.
type Webhooks struct {
	// AllowPrivateNetworks lets webhooks reach private, loopback, and link-local addresses, and over
	// http: an installation whose endpoints are its own (and tests). Unset, a webhook is https to a
	// public address, as a hosted service must, so no one aims ok at its own network.
	AllowPrivateNetworks bool `yaml:"allow_private_networks"`
}

// Automation is ok automation's configuration.
type Automation struct {
	// Logs is the object store finished runs' logs are kept in.
	Logs Objects `yaml:"logs"`
	// SpeculationDepth bounds how many earlier submissions the landing queue's attempt speculates
	// on: 8 unless set.
	SpeculationDepth *int `yaml:"speculation_depth"`
	// LandingBatch bounds how many submissions the landing queue lands in one landing: 32 unless
	// set; 1 lands each alone.
	LandingBatch *int `yaml:"landing_batch"`
	// PlanPool is the pool of workers planning runs run in (docs/hosted.md, "Hosted automation, as
	// built"): default unless set.
	PlanPool string `yaml:"plan_pool"`
	// Cache, when set, is the remote cache ok automation serves its workers' Bazel, in
	// automation.logs' bucket under cache/ (docs/hosted.md, "Hosted automation, as built").
	Cache *AutomationCache `yaml:"cache"`
	// Pools are the pools of workers ok automation sizes, each its worker Deployment's replicas
	// (docs/hosted.md, "Hosted automation, as built"); Kubeconfig reaches their cluster from
	// outside it, unset the cluster ok automation runs in.
	Pools      []Pool `yaml:"pools"`
	Kubeconfig string `yaml:"kubeconfig"`
}

// Pool is a pool of workers ok automation sizes: its name, as jobs and workers name it; its
// workers' Deployment; each worker's slots; and the fewest and most workers it has.
type Pool struct {
	Name       string `yaml:"name"`
	Namespace  string `yaml:"namespace"`
	Deployment string `yaml:"deployment"`
	Slots      int    `yaml:"slots"`
	Min        int    `yaml:"min"`
	Max        int    `yaml:"max"`
}

// AutomationCache is ok automation's remote cache.
type AutomationCache struct {
	// TTL is how long an entry is kept after it is written: 720h (30 days) unless set.
	TTL time.Duration `yaml:"ttl"`
	// MemoryMiB is each replica's memory tier of small blobs: 256 unless set.
	MemoryMiB int `yaml:"memory_mib"`
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
	// Pool is the pool of workers it serves (docs/hosted.md, "Hosted automation, as built"):
	// default unless set.
	Pool string `yaml:"pool"`
	// MicroVM, when set, runs each run in a microVM of its own (docs/hosted.md, "Hosted automation,
	// as built").
	MicroVM *MicroVM `yaml:"microvm"`
}

// MicroVM is how a worker runs its runs in microVMs: Cloud Hypervisor on KVM.
type MicroVM struct {
	// Kernel is the guest's kernel, a bzImage or vmlinux with virtio-fs and vsock built in.
	Kernel string `yaml:"kernel"`
	// Hypervisor is Cloud Hypervisor's path, and Virtiofsd virtiofsd's (1.13 or later): on the PATH
	// unless set.
	Hypervisor string `yaml:"hypervisor"`
	Virtiofsd  string `yaml:"virtiofsd"`
	// CPUs and MemoryGiB are each microVM's: the node's CPUs over the slots, and run_memory_gib,
	// unless set.
	CPUs      int     `yaml:"cpus"`
	MemoryGiB float64 `yaml:"memory_gib"`
	// Share are the host's directories every guest sees read-only, beside /usr and what TLS trusts.
	Share []string `yaml:"share"`
	// Egress are the hosts a run may reach through the worker — "host", "*.domain", or with a
	// port, "host:port" (443 otherwise): DefaultEgress unless set, nothing when empty. A push run
	// may reach PushEgress too, its registries.
	Egress     *[]string `yaml:"egress"`
	PushEgress []string  `yaml:"push_egress"`
	// ReserveGiB is the space kept free on worker.state's filesystem, Domains the copies of trust
	// domains kept: past either, the least recently used go. 20 GiB and 32 unless set.
	ReserveGiB float64 `yaml:"reserve_gib"`
	Domains    int     `yaml:"domains"`
}

// DefaultEgress are the hosts a run in a microVM may reach unless worker.microvm.egress says:
// where Bazel, and the rules most repositories use, download from.
var DefaultEgress = []string{
	"bcr.bazel.build", "releases.bazel.build", "mirror.bazel.build", "github.com", "*.githubusercontent.com",
	"proxy.golang.org", "sum.golang.org", "dl.google.com", "repo1.maven.org", "repo.maven.apache.org",
	"registry.npmjs.org", "pypi.org", "files.pythonhosted.org", "static.crates.io", "index.crates.io",
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
	// PathCommits are the newest commits of main that changed paths, which tree listings with their
	// entries' last commits read.
	PathCommits int `yaml:"path_commits"`
	// CodeIndexes are the code search indexes a replica holds (docs/hosted.md, "ok control at
	// scale, as built").
	CodeIndexes int `yaml:"code_indexes"`
	// Deltas are files' deltas against bases, and Checkouts commits' checkouts' layers (docs/scm.md,
	// "Deltas", "Checkouts").
	Deltas    int `yaml:"deltas"`
	Checkouts int `yaml:"checkouts"`
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
// database, in a schema of its own (bus); nats keeps it in NATS JetStream; pubsub in Google Pub/Sub;
// aws in Amazon SNS (the stream) and SQS; kafka, the stream alone, in Kafka.
type BusBackend struct {
	Type   string     `yaml:"type"`
	NATS   *NATSBus   `yaml:"nats"`
	PubSub *PubSubBus `yaml:"pubsub"`
	AWS    *AWSBus    `yaml:"aws"`
	Kafka  *KafkaBus  `yaml:"kafka"`
}

// KafkaBus is how to reach a Kafka cluster and how to keep the stream there (internal/bus/kafkabus).
type KafkaBus struct {
	// Brokers are some of the cluster's, host:port, to learn the rest from.
	Brokers []string `yaml:"brokers"`
	// TLS reaches them over TLS; CAFile is their certificate authority when not the system's.
	TLS    bool   `yaml:"tls"`
	CAFile string `yaml:"ca_file"`
	// SASL is how ok signs in, when the cluster asks.
	SASL *KafkaSASL `yaml:"sasl"`
	// Prefix begins every topic and group name. Default ok.
	Prefix string `yaml:"prefix"`
	// Partitions is how many partitions a topic is made with. Default 12.
	Partitions int32 `yaml:"partitions"`
	// ReplicationFactor is how many brokers keep each partition. Default the cluster's.
	ReplicationFactor int16 `yaml:"replication_factor"`
	// Retention is how long a topic keeps its events. Default 168h.
	Retention time.Duration `yaml:"retention"`
	// Concurrency is how many events a role's member handles at once, and fetches at most.
	// Default 32.
	Concurrency int `yaml:"concurrency"`
}

// KafkaSASL is how ok signs in to a Kafka cluster.
type KafkaSASL struct {
	// Mechanism is plain, scram-sha-256, scram-sha-512, or aws-msk-iam (an MSK cluster's IAM
	// authentication, with credentials from AWS's chain and the region from the brokers' names).
	Mechanism string `yaml:"mechanism"`
	// Username and PasswordFile are plain's and SCRAM's.
	Username     string `yaml:"username"`
	PasswordFile string `yaml:"password_file"`
}

// AWSBus is how to reach SNS and SQS and how to keep the bus there (internal/bus/awsbus).
type AWSBus struct {
	// Region is the AWS region the topics and queues are in.
	Region string `yaml:"region"`
	// Endpoint is SNS's and SQS's address when not AWS's: a fake's, or LocalStack's.
	Endpoint string `yaml:"endpoint"`
	// AccessKeyFile and SecretKeyFile hold an access key; unset, credentials come from AWS's chain
	// (its environment variables, its credentials file, the instance's or pod's role).
	AccessKeyFile string `yaml:"access_key_file"`
	SecretKeyFile string `yaml:"secret_key_file"`
	// Prefix begins every topic and queue name. Default ok.
	Prefix string `yaml:"prefix"`
	// Retention is how long a queue keeps what it holds: at most 336h. Default 168h.
	Retention time.Duration `yaml:"retention"`
	// MaxAttempts is how many deliveries a job gets before it is a dead letter. Default 10.
	MaxAttempts int `yaml:"max_attempts"`
	// Concurrency is how many events a role's member handles at once. Default 32.
	Concurrency int `yaml:"concurrency"`
}

// PubSubBus is how to reach Google Pub/Sub and how to keep the bus there (internal/bus/pubsubbus).
type PubSubBus struct {
	// Project is the Google Cloud project the topics and subscriptions are in.
	Project string `yaml:"project"`
	// CredentialsFile holds a service account's key; unset, the workload's own identity is used.
	CredentialsFile string `yaml:"credentials_file"`
	// Endpoint is the service's address when not the global one: pubsub.<region>.rep.googleapis.com:443.
	Endpoint string `yaml:"endpoint"`
	// EmulatorHost is a Pub/Sub emulator's host:port, reached without TLS or credentials.
	EmulatorHost string `yaml:"emulator_host"`
	// Prefix begins every topic and subscription name. Default ok.
	Prefix string `yaml:"prefix"`
	// Retention is how long what is not acknowledged is kept: at most 168h, the default.
	Retention time.Duration `yaml:"retention"`
	// MaxAttempts is how many deliveries a job gets before it is a dead letter: at most 100.
	// Default 10.
	MaxAttempts int `yaml:"max_attempts"`
	// Concurrency is how many events a role's member handles, and holds, at once. Default 32.
	Concurrency int `yaml:"concurrency"`
}

// NATSBus is how to reach NATS JetStream and how to keep the bus there (internal/bus/natsbus).
type NATSBus struct {
	// URL is the servers: nats://host:4222, several comma-separated.
	URL string `yaml:"url"`
	// CredentialsFile holds the user's JWT and seed (a .creds file), when the servers ask for one.
	CredentialsFile string `yaml:"credentials_file"`
	// CAFile is the servers' certificate authority, when they use TLS under one of your own.
	CAFile string `yaml:"ca_file"`
	// Prefix begins every subject and stream name: installations sharing an account keep apart.
	Prefix string `yaml:"prefix"`
	// Replicas is how many servers keep each stream: 3 in a cluster. Default 1.
	Replicas int `yaml:"replicas"`
	// Retention is how long events, jobs, and dead letters are kept. Default 168h.
	Retention time.Duration `yaml:"retention"`
	// AckWait is the floor of a job's lease, and how long a dead worker holds it. Default 30s.
	AckWait time.Duration `yaml:"ack_wait"`
	// MaxAttempts is how many deliveries a job gets before it is a dead letter. Default 10.
	MaxAttempts int `yaml:"max_attempts"`
	// Concurrency is how many events a role's member handles at once. Default 16.
	Concurrency int `yaml:"concurrency"`
}

// TLS is a certificate and key, as files.
type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// Database is where the roles keep their tables, by its type: postgres, the default, or spanner
// (docs/hosted.md, "Spanner").
type Database struct {
	// Type is postgres or spanner.
	Type string `yaml:"type"`
	// URLFile holds PostgreSQL's connection URL (postgres://…), which carries the password.
	URLFile string `yaml:"url_file"`
	// MaxConnections bounds each role's PostgreSQL pool; 0 is the driver's default.
	MaxConnections int32 `yaml:"max_connections"`
	// Spanner is the instance the roles keep their databases in, for type spanner.
	Spanner *SpannerDatabase `yaml:"spanner"`
}

// SpannerDatabase is a Spanner instance, each role's tables in a database of its own there,
// <prefix>-<role> (ok-deploy, …), made when the role first starts: Google's, reached with the
// workload's credentials, or a Spanner Omni deployment's.
type SpannerDatabase struct {
	// Instance is projects/P/instances/I; a Spanner Omni deployment's is
	// projects/default/instances/default.
	Instance string `yaml:"instance"`
	// Prefix begins each role's database's name. Default ok.
	Prefix string `yaml:"prefix"`
	// Omni, when set, is a Spanner Omni deployment, reached in place of Google's service.
	Omni *SpannerOmni `yaml:"omni"`
	// Channels are each role's gRPC connections to it: 16 unless set (spannerconn.DefaultChannels).
	Channels int `yaml:"channels"`
}

// SpannerOmni is how to reach a Spanner Omni deployment.
type SpannerOmni struct {
	// Address is its clients' address, host:port.
	Address string `yaml:"address"`
	// PlainText reaches it without TLS: one server on its own loopback.
	PlainText bool `yaml:"plain_text"`
	// CAFile, for TLS, is the CA its certificate is under; the system's roots when unset.
	CAFile string `yaml:"ca_file"`
	// Username and PasswordFile sign in, when its deployment asks for a password.
	Username     string `yaml:"username"`
	PasswordFile string `yaml:"password_file"`
}

// DatabaseName is a role's database in the instance.
func (s *SpannerDatabase) DatabaseName(role string) string {
	return s.Instance + "/databases/" + s.Prefix + "-" + role
}

var (
	spannerInstance = regexp.MustCompile(`^projects/[a-z0-9][a-z0-9.:-]*/instances/[a-z][a-z0-9-]*$`)
	// A database's ID is 2 to 30 of [a-z0-9_-], starting with a letter; the longest role's name
	// (automation) and its hyphen leave 19.
	spannerPrefix = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,18}$`)
)

// Roles are the robots the roles run as.
type Roles struct {
	Identity   string `yaml:"identity"`
	SCM        string `yaml:"scm"`
	Control    string `yaml:"control"`
	Automation string `yaml:"automation"`
	Worker     string `yaml:"worker"`
	Deploy     string `yaml:"deploy"`
	Edge       string `yaml:"edge"`
	Secrets    string `yaml:"secrets"`
}

// All are the roles' robots by role name, for those named.
func (r Roles) All() map[string]string {
	out := map[string]string{}
	for k, v := range map[string]string{"identity": r.Identity, "scm": r.SCM, "control": r.Control, "automation": r.Automation, "worker": r.Worker, "deploy": r.Deploy,
		"edge": r.Edge, "secrets": r.Secrets} {
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
	Secrets    string `yaml:"secrets"`
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
	TrustedHeader string `yaml:"trusted_header"`
	SignIn        SignIn `yaml:"sign_in"`
	// SignUp, in a hosted service, is how people make their own accounts.
	SignUp *SignUp `yaml:"sign_up"`
	// Mail is the server people's notifications are mailed through (docs/hosted.md, "ok control at
	// scale, as built"); unset, none is mailed.
	Mail *Mail `yaml:"mail"`
	// SealingKeyFile holds the key (32 bytes, base64) organizations' identity providers' secrets
	// are sealed with; without it, no organization keeps one.
	SealingKeyFile string          `yaml:"sealing_key_file"`
	SystemOfRecord *SystemOfRecord `yaml:"system_of_record"`
	Federation     []Issuer        `yaml:"federation"`
	// Robots are made at start, if they are not, with their bindings: the roles' own, typically.
	Robots []Robot `yaml:"robots"`
	// Cells are the cells repositories are placed in (docs/hosted.md, "Cells, as built"), the
	// first holding those made before there were cells; with none, the one cell default.
	Cells []IdentityCell `yaml:"cells"`
	// DefaultRegion is where an owner's new repositories are placed when it names no region of its
	// own: one of the cells'. Unset, any open cell's.
	DefaultRegion string `yaml:"default_region"`
	// DefaultQuota is an account's quota of CI when an administrator set it none (docs/hosted.md,
	// "Hosted automation, as built"): zero concurrent runs or minutes for unlimited; weight 1
	// unless set.
	DefaultQuota Quota `yaml:"default_quota"`
}

// Quota is an account's quota of CI: concurrent runs, run minutes in a calendar month (UTC), and
// its weight in fair share.
type Quota struct {
	Concurrent int   `yaml:"concurrent"`
	Minutes    int64 `yaml:"minutes"`
	Weight     int   `yaml:"weight"`
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

// SignUp is how a hosted service's people make accounts (docs/identity.md, "Accounts"): with an
// email they verify and a password, mailed through SMTP; or with a social provider's sign-in, among
// sign_in.oidc, linked by its subject.
type SignUp struct {
	Email *SignUpEmail `yaml:"email"`
	// OIDC are the sign_in.oidc providers, by id, whose people may make accounts with them.
	OIDC []string `yaml:"oidc"`
}

// SignUpEmail is email-and-password sign-up, and the SMTP server its links go through.
type SignUpEmail struct {
	SMTP SMTP `yaml:"smtp"`
}

// Mail is how the directory mails people.
type Mail struct {
	SMTP SMTP `yaml:"smtp"`
}

// SMTP is a mail server: STARTTLS is required but to one on loopback.
type SMTP struct {
	Address      string `yaml:"address"` // host:port
	Username     string `yaml:"username"`
	PasswordFile string `yaml:"password_file"`
	From         string `yaml:"from"` // "ok <no-reply@example.com>"
	CAFile       string `yaml:"ca_file"`
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

// cellName is a cell's name.
var cellName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,29}$`)

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
	switch c.Tenancy.Mode {
	case "":
		c.Tenancy.Mode = "installation"
	case "installation", "hosted":
	default:
		add("tenancy.mode is installation or hosted, not %q", c.Tenancy.Mode)
	}
	if c.Tenancy.Mode == "installation" {
		if c.Tenancy.Organization == "" {
			c.Tenancy.Organization, _, _ = strings.Cut(c.Domain, ".")
		}
		if err := tenancy.ValidHandle(c.Tenancy.Organization); err != nil {
			add("tenancy.organization: %v (the domain's first label unless set)", err)
		}
	}
	if c.Cell.Name == "" {
		c.Cell.Name = DefaultCell
	}
	if !cellName.MatchString(c.Cell.Name) {
		add("cell.name: %q is 1 to 30 of lowercase letters, digits, and hyphens, starting with a letter", c.Cell.Name)
	}
	cellsSeen := map[string]bool{}
	cellOpen := false
	for i, cell := range c.Identity.Cells {
		switch {
		case !cellName.MatchString(cell.Name):
			add("identity.cells[%d].name: %q is 1 to 30 of lowercase letters, digits, and hyphens, starting with a letter", i, cell.Name)
		case cellsSeen[cell.Name]:
			add("identity.cells[%d].name: %s is named twice", i, cell.Name)
		case cell.SCM == "":
			add("identity.cells[%d].scm: where %s's SCM is reached", i, cell.Name)
		}
		cellsSeen[cell.Name] = true
		cellOpen = cellOpen || !cell.Closed
	}
	if len(c.Identity.Cells) > 0 && !cellOpen {
		add("identity.cells: every cell is closed: new repositories have nowhere to go")
	}
	if q := &c.Identity.DefaultQuota; q.Concurrent < 0 || q.Minutes < 0 || q.Weight < 0 {
		add("identity.default_quota: concurrent, minutes, and weight are not negative")
	} else if q.Weight == 0 {
		q.Weight = 1
	}
	if r := c.Identity.DefaultRegion; r != "" {
		known := false
		for _, cell := range c.Cells() {
			known = known || cell.Region == r
		}
		if !known {
			add("identity.default_region: no cell is in region %s", r)
		}
	}
	for field, r := range map[string]*Rate{"edge.limits.principal": &c.Edge.Limits.Principal, "edge.limits.address": &c.Edge.Limits.Address} {
		def := DefaultPrincipalRate
		if field == "edge.limits.address" {
			def = DefaultAddressRate
		}
		switch {
		case r.PerSecond < 0 || r.Burst < 0:
			add("%s: per_second and burst are not negative", field)
		case r.PerSecond == 0 && r.Burst == 0:
			*r = def
		case r.PerSecond == 0 || r.Burst == 0:
			add("%s: per_second and burst together, or neither", field)
		}
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
		case "nats":
			if b.NATS == nil || b.NATS.URL == "" {
				add("%s.nats.url: the NATS servers, for type nats", field)
			}
		case "pubsub":
			if b.PubSub == nil || b.PubSub.Project == "" {
				add("%s.pubsub.project: the Google Cloud project, for type pubsub", field)
			}
		case "aws":
			if b.AWS == nil || b.AWS.Region == "" {
				add("%s.aws.region: the AWS region, for type aws", field)
			} else if (b.AWS.AccessKeyFile == "") != (b.AWS.SecretKeyFile == "") {
				add("%s.aws: access_key_file and secret_key_file together, or neither", field)
			}
		case "kafka":
			switch {
			case field == "bus.queue":
				add("bus.queue.type: kafka carries the stream alone (it acknowledges no single message): take the queue from another backend")
			case b.Kafka == nil || len(b.Kafka.Brokers) == 0:
				add("%s.kafka.brokers: some of the cluster's brokers, for type kafka", field)
			case b.Kafka.SASL != nil:
				switch b.Kafka.SASL.Mechanism {
				case "plain", "scram-sha-256", "scram-sha-512":
					if b.Kafka.SASL.Username == "" || b.Kafka.SASL.PasswordFile == "" {
						add("%s.kafka.sasl: %s takes a username and a password_file", field, b.Kafka.SASL.Mechanism)
					}
				case "aws-msk-iam":
				default:
					add("%s.kafka.sasl.mechanism: %q is none of plain, scram-sha-256, scram-sha-512, and aws-msk-iam", field, b.Kafka.SASL.Mechanism)
				}
			}
		default:
			add("%s.type: %q is none of postgres, nats, pubsub, aws, and kafka", field, b.Type)
		}
		if b.NATS != nil && b.Type != "nats" {
			add("%s.nats: for type nats, not %s", field, b.Type)
		}
		if b.PubSub != nil && b.Type != "pubsub" {
			add("%s.pubsub: for type pubsub, not %s", field, b.Type)
		}
		if b.AWS != nil && b.Type != "aws" {
			add("%s.aws: for type aws, not %s", field, b.Type)
		}
		if b.Kafka != nil && b.Type != "kafka" {
			add("%s.kafka: for type kafka, not %s", field, b.Type)
		}
	}
	switch c.Database.Type {
	case "":
		c.Database.Type = "postgres"
	case "postgres", "spanner":
	default:
		add("database.type: %q is neither postgres nor spanner", c.Database.Type)
	}
	if s := c.Database.Spanner; c.Database.Type == "spanner" {
		switch {
		case s == nil || s.Instance == "":
			add("database.spanner.instance: the Spanner instance, projects/P/instances/I, for type spanner")
		case !spannerInstance.MatchString(s.Instance):
			add("database.spanner.instance: %q is not projects/P/instances/I", s.Instance)
		}
		if s != nil {
			if s.Prefix == "" {
				s.Prefix = "ok"
			}
			if !spannerPrefix.MatchString(s.Prefix) {
				add("database.spanner.prefix: %q is not 1 to 19 of [a-z0-9_-], starting with a letter", s.Prefix)
			}
			if o := s.Omni; o != nil {
				if o.Address == "" {
					add("database.spanner.omni.address: the deployment's host:port")
				}
				if o.PlainText && o.CAFile != "" {
					add("database.spanner.omni: plain_text or ca_file, not both")
				}
				if (o.Username == "") != (o.PasswordFile == "") {
					add("database.spanner.omni: username and password_file together, or neither")
				}
			}
		}
		if c.Database.URLFile != "" || c.Database.MaxConnections != 0 {
			add("database.url_file and database.max_connections: for type postgres, not spanner")
		}
		for _, field := range []string{"stream", "queue"} {
			b := c.Bus.Stream
			if field == "queue" {
				b = c.Bus.Queue
			}
			if b.Type == "postgres" {
				add("bus.%s.type: postgres keeps the bus in PostgreSQL, which database.type spanner leaves none of: take it from another backend", field)
			}
		}
	} else if s != nil {
		add("database.spanner: for type spanner, not %s", c.Database.Type)
	}
	if c.Secrets.Root != "" && c.Roles.Secrets == "" {
		add("secrets.root: ok secrets runs as its own robot, which roles.secrets names (secrets.ok, say)")
	}
	for _, name := range slices.Sorted(maps.Keys(c.Secrets.Stores)) {
		st, where := c.Secrets.Stores[name], "secrets.stores."+name
		switch {
		case name == workflow.DefaultSecretStore:
			add("%s: %q is ok secrets' own database's name, which needs no configuration", where, name)
		case !workflow.ValidStoreName(name):
			add("%s: a store's name is lowercase letters, digits, and dashes", where)
		default:
			problems = append(problems, st.problems(where)...)
		}
	}
	if d := c.Secrets.Database; d != nil {
		switch d.Type {
		case "", "postgres":
			if d.URLFile == "" {
				add("secrets.database.url_file: ok secrets' own database's connection URL")
			}
		case "spanner":
			if d.Spanner == nil || d.Spanner.Instance == "" {
				add("secrets.database.spanner.instance: the instance ok secrets' own database is in")
			}
		default:
			add("secrets.database.type: postgres or spanner, not %q", d.Type)
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
	if tr := c.Identity.TreeRepository; c.Tenancy.Hosted() && tr != "" && !strings.Contains(tr, "/") {
		add("identity.tree_repository is <owner>/<name> in a hosted service, where every repository has an owner: %s names none", tr)
	}
	if c.Tenancy.Hosted() && c.Identity.SystemOfRecord != nil {
		add("identity.system_of_record is an installation's: a hosted service's people sign up, or an organization's identity provider makes them (ok identity move-in makes an installation's its organization's)")
	}
	if su := c.Identity.SignUp; su != nil {
		if !c.Tenancy.Hosted() {
			add("identity.sign_up is a hosted service's: an installation's people come from its system of record")
		}
		if e := su.Email; e != nil && (e.SMTP.Address == "" || e.SMTP.From == "") {
			add("identity.sign_up.email.smtp names its address and from")
		}
		for _, id := range su.OIDC {
			if !seen[id] {
				add("identity.sign_up.oidc: %q is no identity.sign_in.oidc provider", id)
			}
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
	for _, p := range []struct {
		what string
		name *string
	}{{"automation.plan_pool", &c.Automation.PlanPool}, {"worker.pool", &c.Worker.Pool}} {
		if *p.name == "" {
			*p.name = workflow.DefaultPool
		} else if err := workflow.ValidPool(*p.name); err != nil {
			add("%s: %v", p.what, err)
		}
	}
	seenPools := map[string]bool{}
	for i := range c.Automation.Pools {
		p := &c.Automation.Pools[i]
		if err := workflow.ValidPool(p.Name); err != nil {
			add("automation.pools: %v", err)
		}
		if seenPools[p.Name] {
			add("automation.pools: %s is named twice", p.Name)
		}
		seenPools[p.Name] = true
		if p.Namespace == "" || p.Deployment == "" {
			add("automation.pools: pool %s names its workers' namespace and deployment", p.Name)
		}
		if p.Slots == 0 {
			p.Slots = 1
		}
		if p.Slots < 0 || p.Min < 0 || p.Max < p.Min || p.Max == 0 {
			add("automation.pools: pool %s's slots, min, and max are positive, min at most max", p.Name)
		}
	}
	if ac := c.Automation.Cache; ac != nil {
		if ac.TTL < 0 || ac.MemoryMiB < 0 {
			add("automation.cache's ttl and memory_mib are not negative")
		}
		if ac.TTL == 0 {
			ac.TTL = 30 * 24 * time.Hour
		}
		if ac.MemoryMiB == 0 {
			ac.MemoryMiB = 256
		}
	}
	if d := c.Automation.SpeculationDepth; d != nil && *d < 0 {
		add("automation.speculation_depth is a count, 0 or more")
	}
	if b := c.Automation.LandingBatch; b != nil && *b < 1 {
		add("automation.landing_batch is a count, 1 or more")
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
	if m := c.Worker.MicroVM; m != nil {
		if !filepath.IsAbs(m.Kernel) {
			add("worker.microvm.kernel is the guest's kernel, an absolute path")
		}
		for _, s := range m.Share {
			if !filepath.IsAbs(s) {
				add("worker.microvm.share: %q is no absolute path", s)
			}
		}
		if m.CPUs < 0 || m.MemoryGiB < 0 || m.ReserveGiB < 0 || m.Domains < 0 {
			add("worker.microvm's cpus, memory_gib, reserve_gib, and domains are not negative")
		}
		if m.Egress == nil {
			e := slices.Clone(DefaultEgress)
			m.Egress = &e
		}
		if m.ReserveGiB == 0 {
			m.ReserveGiB = 20
		}
		if m.Domains == 0 {
			m.Domains = 32
		}
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
