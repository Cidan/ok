package agent_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/Cidan/ok/internal/deploy/agent"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/deploy/tunnel"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// asCaller names who a test's call is from: a robot's ID, or "deploy" for the deployer.
const asCaller = "x-test-caller"

func callerOf(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	who := md.Get(asCaller)
	if len(who) == 0 {
		return ctx
	}
	if who[0] == "deploy" {
		return authn.NewContext(ctx, authn.Caller{Principal: "deploy.ok.robot.example.com", ID: "deploy", Role: okpb.Caller_CALLER_DEPLOY})
	}
	return authn.NewContext(ctx, authn.Caller{Principal: who[0] + ".acme.robot.example.com", ID: who[0]})
}

type wrapped struct {
	grpc.ServerStream
	ctx context.Context
}

func (w wrapped) Context() context.Context { return w.ctx }

func as(ctx context.Context, who string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, asCaller, who)
}

// replica is a deployer replica's hub, serving on its address.
type replica struct {
	hub  *tunnel.Hub
	addr string
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func startReplica(t *testing.T, st *store.Store) replica {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := tunnel.NewHub(tunnel.HubConfig{Store: st, Address: lis.Addr().String(), Cell: "c1", Renew: 200 * time.Millisecond,
		Account: func(_ context.Context, robot string) (string, error) {
			if robot == "agent" {
				return "org_acme", nil
			}
			return "", status.Error(codes.NotFound, "no agent")
		},
		Dial: func(ctx context.Context, addr string) (deploypb.AgentRelayClient, error) {
			c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer,
					opts ...grpc.CallOption) (grpc.ClientStream, error) {
					return streamer(as(ctx, "deploy"), desc, cc, method, opts...)
				}))
			if err != nil {
				return nil, err
			}
			return deploypb.NewAgentRelayClient(c), nil
		}})
	srv := grpc.NewServer(grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return h(srv, wrapped{ss, callerOf(ss.Context())})
	}))
	deploypb.RegisterAgentsServer(srv, hub)
	deploypb.RegisterAgentRelayServer(srv, hub)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop(); hub.Close() })
	return replica{hub: hub, addr: lis.Addr().String()}
}

// grant gives the cluster's user agent what the docs' grants say, in namespace web.
func grant(t *testing.T, k *testdb.KubeServer) {
	t.Helper()
	ctx := context.Background()
	admin := k.Admin(t)
	for _, ns := range []string{"web", "other"} {
		if _, err := admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.RbacV1().Roles("web").Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "deploy"}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{"apps"}, Resources: []string{"deployments", "deployments/scale"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"pods", "secrets"}, Verbs: []string{"get", "list", "watch", "create"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"create"}},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.RbacV1().RoleBindings("web").Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "deploy"},
		RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "deploy"},
		Subjects: []rbacv1.Subject{{Kind: "User", Name: "agent"}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func deployment(name string, replicas int32) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: appsv1.DeploymentSpec{Replicas: &replicas,
		Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.com/app:1"}}}}}}
}

// Against a real API server, the deployer's client-go on one replica creates, reads, updates, lists,
// and watches Deployments through an agent connected to another, its ServiceAccount's RBAC bounding
// it; the agent refuses Secrets, exec, and deletes even where RBAC would allow them; and a replica
// reaches no agent once it disconnects, nor one never connected.
func TestThroughAnAgent(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	k := testdb.Kube(t, "agent")
	grant(t, k)
	b, err := postgres.Open(ctx, testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(b)
	a, other := startReplica(t, st), startReplica(t, st)

	// A robot no organization designated is refused.
	stranger, err := deploypb.NewAgentsClient(dial(t, a.addr)).Connect(as(ctx, "ci"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&agent.Agent{Kube: k.Config("agent")}).Serve(ctx, stranger); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a robot designated by no one connecting: %v", err)
	}

	actx, disconnect := context.WithCancel(ctx)
	stream, err := deploypb.NewAgentsClient(dial(t, a.addr)).Connect(as(actx, "agent"))
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		_, err := (&agent.Agent{Kube: k.Config("agent"), Migrate: func(_ context.Context, req *deploypb.MigrationRequest) *deploypb.MigrationResult {
			return &deploypb.MigrationResult{Report: []byte(`{"executed":true}`), Output: "applied " + req.GetTarget() + " at the agent"}
		}}).Serve(actx, stream)
		served <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !a.hub.Connected("org_acme") {
		if time.Now().After(deadline) {
			t.Fatal("the agent never connected")
		}
		time.Sleep(20 * time.Millisecond)
	}

	kube, err := kubernetes.NewForConfig(&rest.Config{Host: "https://cluster.invalid",
		Transport: &tunnel.Transport{Link: func(ctx context.Context) (tunnel.Link, error) { return other.hub.Link(ctx, "org_acme") }}})
	if err != nil {
		t.Fatal(err)
	}
	deps := kube.AppsV1().Deployments("web")
	if _, err := deps.Create(ctx, deployment("api", 2), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating through the agent: %v", err)
	}
	got, err := deps.Get(ctx, "api", metav1.GetOptions{})
	if err != nil || *got.Spec.Replicas != 2 {
		t.Fatalf("reading through the agent: %v, %v", got, err)
	}
	w, err := deps.Watch(ctx, metav1.ListOptions{ResourceVersion: got.ResourceVersion})
	if err != nil {
		t.Fatalf("watching through the agent: %v", err)
	}
	defer w.Stop()
	*got.Spec.Replicas = 5
	if _, err := deps.Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating through the agent: %v", err)
	}
	select {
	case ev := <-w.ResultChan():
		if d, ok := ev.Object.(*appsv1.Deployment); ev.Type != watch.Modified || !ok || *d.Spec.Replicas != 5 {
			t.Errorf("the watch's event: %s %v", ev.Type, ev.Object)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no watch event through the agent")
	}
	if list, err := deps.List(ctx, metav1.ListOptions{}); err != nil || len(list.Items) != 1 {
		t.Errorf("listing through the agent: %v, %v", list, err)
	}
	if _, err := kube.CoreV1().Pods("web").List(ctx, metav1.ListOptions{}); err != nil {
		t.Errorf("listing pods through the agent: %v", err)
	}
	// The cluster's RBAC: nothing in a namespace not granted.
	if _, err := kube.AppsV1().Deployments("other").List(ctx, metav1.ListOptions{}); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("a namespace the agent is not granted: %v", err)
	}
	// The agent's own refusals, whatever RBAC grants.
	for what, err := range map[string]error{
		"listing Secrets": func() error { _, err := kube.CoreV1().Secrets("web").List(ctx, metav1.ListOptions{}); return err }(),
		"deleting":        deps.Delete(ctx, "api", metav1.DeleteOptions{}),
		"making a pod": func() error {
			_, err := kube.CoreV1().Pods("web").Create(ctx, &corev1.Pod{}, metav1.CreateOptions{})
			return err
		}(),
		"exec in a pod":     kube.CoreV1().RESTClient().Post().Namespace("web").Resource("pods").Name("api-1").SubResource("exec").Do(ctx).Error(),
		"a pod's log":       kube.CoreV1().RESTClient().Get().Namespace("web").Resource("pods").Name("api-1").SubResource("log").Do(ctx).Error(),
		"a service's proxy": kube.CoreV1().RESTClient().Get().Namespace("web").Resource("services").Name("api").SubResource("proxy").Do(ctx).Error(),
	} {
		if err == nil || !strings.Contains(err.Error(), "deploy agent makes no") {
			t.Errorf("%s: %v; want the agent's refusal", what, err)
		}
	}

	// A migration, run by the agent.
	link, err := other.hub.Link(ctx, "org_acme")
	if err != nil {
		t.Fatal(err)
	}
	res, err := tunnel.Migrate(ctx, link, &deploypb.MigrationRequest{RepositoryId: 1, CommitId: []byte{1}, Target: "//db:migrate"})
	if err != nil || res.GetOutput() != "applied //db:migrate at the agent" {
		t.Errorf("a migration through the agent: %v, %v", res, err)
	}

	if _, err := other.hub.Link(ctx, "org_globex"); !errors.Is(err, tunnel.ErrNoAgent) {
		t.Errorf("an organization with no agent: %v", err)
	}
	disconnect()
	<-served
	deadline = time.Now().Add(10 * time.Second)
	for {
		_, err := kube.AppsV1().Deployments("web").Get(ctx, "api", metav1.GetOptions{})
		if errors.Is(err, tunnel.ErrNoAgent) || err != nil && strings.Contains(err.Error(), tunnel.ErrNoAgent.Error()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the agent disconnected: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
