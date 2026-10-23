package testdb

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// KubeServer is a real Kubernetes API server of a test's own, its etcd beside it, with RBAC: no
// controllers and no nodes, so a Deployment made is stored and served, and nothing runs it. Its
// people are bearer tokens: an administrator (system:masters), and those a test names.
type KubeServer struct {
	// URL is where it serves, https; CAData verifies it.
	URL    string
	CAData []byte
	tokens map[string]string
}

// Admin is a client as its administrator.
func (k *KubeServer) Admin(t testing.TB) *kubernetes.Clientset { return k.Client(t, "admin") }

// Config is the client configuration of one of its users.
func (k *KubeServer) Config(user string) *rest.Config {
	return &rest.Config{Host: k.URL, BearerToken: k.tokens[user], TLSClientConfig: rest.TLSClientConfig{CAData: k.CAData}}
}

// Client is a client as one of its users.
func (k *KubeServer) Client(t testing.TB, user string) *kubernetes.Clientset {
	t.Helper()
	c, err := kubernetes.NewForConfig(k.Config(user))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Kube starts a Kubernetes API server for a test, stopped when it ends, with users beside its
// administrator, each in no group: RBAC gives them what a test grants. Its binaries are
// OK_TEST_KUBE_ASSETS's (etcd and kube-apiserver, as setup-envtest fetches them); with none, the
// test is skipped — or fails, when OK_TEST_KUBE=require.
func Kube(t testing.TB, users ...string) *KubeServer {
	t.Helper()
	assets := os.Getenv("OK_TEST_KUBE_ASSETS")
	if assets == "" {
		if os.Getenv("OK_TEST_KUBE") == "require" {
			t.Fatal("testdb: OK_TEST_KUBE=require, and OK_TEST_KUBE_ASSETS is unset")
		}
		t.Skip("testdb: no Kubernetes API server's binaries (OK_TEST_KUBE_ASSETS)")
	}
	dir := t.TempDir()
	k := &KubeServer{tokens: map[string]string{}}
	var tokens strings.Builder
	for i, u := range append([]string{"admin"}, users...) {
		b := make([]byte, 16)
		rand.Read(b)
		k.tokens[u] = hex.EncodeToString(b)
		groups := ""
		if u == "admin" {
			groups = ",system:masters"
		}
		fmt.Fprintf(&tokens, "%s,%s,%d%s\n", k.tokens[u], u, i+1, groups)
	}
	write := func(name string, data []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	tokenFile := write("tokens.csv", []byte(tokens.String()))
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	saKey := write("sa.key", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	saPub := write("sa.pub", pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub}))

	client, peer, secure := freePort(t), freePort(t), freePort(t)
	etcdURL := "http://127.0.0.1:" + client
	start(t, filepath.Join(assets, "etcd"), filepath.Join(dir, "etcd.log"),
		"--data-dir", filepath.Join(dir, "etcd"), "--listen-client-urls", etcdURL, "--advertise-client-urls", etcdURL,
		"--listen-peer-urls", "http://127.0.0.1:"+peer, "--initial-advertise-peer-urls", "http://127.0.0.1:"+peer,
		"--initial-cluster", "default=http://127.0.0.1:"+peer, "--unsafe-no-fsync")
	certDir := filepath.Join(dir, "certs")
	start(t, filepath.Join(assets, "kube-apiserver"), filepath.Join(dir, "apiserver.log"),
		"--etcd-servers", etcdURL, "--cert-dir", certDir, "--bind-address", "127.0.0.1", "--advertise-address", "127.0.0.1",
		"--secure-port", secure, "--service-cluster-ip-range", "10.0.0.0/24", "--token-auth-file", tokenFile,
		"--authorization-mode", "RBAC", "--service-account-issuer", "https://kubernetes.default.svc",
		"--service-account-key-file", saPub, "--service-account-signing-key-file", saKey,
		"--disable-admission-plugins", "ServiceAccount")
	k.URL = "https://127.0.0.1:" + secure
	deadline := time.Now().Add(90 * time.Second)
	for {
		if k.CAData == nil {
			k.CAData, _ = os.ReadFile(filepath.Join(certDir, "apiserver.crt"))
		}
		if k.CAData != nil {
			if c, err := kubernetes.NewForConfig(k.Config("admin")); err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				body, err := c.Discovery().RESTClient().Get().AbsPath("/readyz").DoRaw(ctx)
				cancel()
				if err == nil && strings.TrimSpace(string(body)) == "ok" {
					return k
				}
			}
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(filepath.Join(dir, "apiserver.log"))
			t.Fatalf("testdb: the Kubernetes API server is not ready after 90s:\n%s", tail(log, 4000))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// start runs a server for the rest of the test, its output in a log.
func start(t testing.TB, bin, logPath string, args ...string) {
	t.Helper()
	logf, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("testdb: starting %s: %v", bin, err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		logf.Close()
	})
}

func freePort(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}
