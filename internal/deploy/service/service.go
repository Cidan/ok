// Package service is the deployer's Deployments service: ok control reads deployments for the
// CLs they cover, and retries, halts, and allows hazards on behalf of the people it has authorized.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/serve/authn"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// Service is the Deployments service. Only ok control reaches it, acting for the person each
// request names: ok's runtime holds it to its protocol's ok.v1.callers.
type Service struct {
	deploypb.UnimplementedDeploymentsServer
	store *store.Store
}

// New builds the service over a store.
func New(s *store.Store) *Service {
	return &Service{store: s}
}

func caller(ctx context.Context) (string, error) {
	c, ok := authn.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "the deployer serves identified principals only")
	}
	return c.Principal, nil
}

// requester is who a request that changes a deployment is for: the person ok control names.
func (s *Service) requester(ctx context.Context, forPerson string) (string, error) {
	if _, err := caller(ctx); err != nil {
		return "", err
	}
	if forPerson == "" {
		return "", status.Error(codes.InvalidArgument, "ok control names the person it acts for")
	}
	return forPerson, nil
}

func (s *Service) reader(ctx context.Context) error {
	_, err := caller(ctx)
	return err
}

func toStatus(err error) error {
	var conflict *store.ConflictError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.As(err, &conflict):
		return status.Error(codes.FailedPrecondition, conflict.Message)
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Service) ListDeployments(ctx context.Context, req *deploypb.ListDeploymentsRequest) (*deploypb.ListDeploymentsResponse, error) {
	if err := s.reader(ctx); err != nil {
		return nil, err
	}
	if req.GetRepositoryId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "a repository is required")
	}
	limit := int(req.GetLimit())
	switch {
	case limit <= 0:
		limit = 50
	case limit > 500:
		limit = 500
	}
	var ds []*store.Deployment
	var planned bool
	var err error
	if len(req.GetCommitId()) > 0 {
		ds, planned, err = s.store.Covering(ctx, req.GetRepositoryId(), req.GetCommitId())
		if len(ds) > limit {
			ds = ds[:limit]
		}
	} else {
		ds, err = s.store.Latest(ctx, req.GetRepositoryId(), limit)
	}
	if err != nil {
		return nil, toStatus(err)
	}
	out := &deploypb.ListDeploymentsResponse{Planned: planned}
	for _, d := range ds {
		out.Deployments = append(out.Deployments, deploymentOut(d, nil))
	}
	return out, nil
}

func (s *Service) GetDeployment(ctx context.Context, req *deploypb.GetDeploymentRequest) (*deploypb.Deployment, error) {
	if err := s.reader(ctx); err != nil {
		return nil, err
	}
	return s.get(ctx, req.GetId())
}

func (s *Service) get(ctx context.Context, id int64) (*deploypb.Deployment, error) {
	d, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	evs, err := s.store.Events(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return deploymentOut(d, evs), nil
}

func (s *Service) RetryDeployment(ctx context.Context, req *deploypb.RetryDeploymentRequest) (*deploypb.Deployment, error) {
	who, err := s.requester(ctx, req.GetForPerson())
	if err != nil {
		return nil, err
	}
	d, err := s.store.Retry(ctx, req.GetId(), who)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.get(ctx, d.ID)
}

func (s *Service) HaltDeployment(ctx context.Context, req *deploypb.HaltDeploymentRequest) (*deploypb.Deployment, error) {
	who, err := s.requester(ctx, req.GetForPerson())
	if err != nil {
		return nil, err
	}
	if _, err := s.store.Halt(ctx, req.GetId(), who); err != nil {
		return nil, toStatus(err)
	}
	return s.get(ctx, req.GetId())
}

func (s *Service) AllowHazards(ctx context.Context, req *deploypb.AllowHazardsRequest) (*deploypb.Deployment, error) {
	who, err := s.requester(ctx, req.GetForPerson())
	if err != nil {
		return nil, err
	}
	if _, err := s.store.Allow(ctx, req.GetId(), who); err != nil {
		return nil, toStatus(err)
	}
	return s.get(ctx, req.GetId())
}

func (s *Service) ListServices(ctx context.Context, req *deploypb.ListServicesRequest) (*deploypb.ListServicesResponse, error) {
	if err := s.reader(ctx); err != nil {
		return nil, err
	}
	svs, err := s.store.Services(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &deploypb.ListServicesResponse{}
	for _, sv := range svs {
		out.Services = append(out.Services, serviceOut(sv))
	}
	return out, nil
}

func (s *Service) GetService(ctx context.Context, req *deploypb.GetServiceRequest) (*deploypb.Service, error) {
	if err := s.reader(ctx); err != nil {
		return nil, err
	}
	sv, err := s.store.GetService(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	limit := int(req.GetRecent())
	switch {
	case limit <= 0:
		limit = 20
	case limit > 200:
		limit = 200
	}
	recent, err := s.store.Recent(ctx, sv, limit)
	if err != nil {
		return nil, toStatus(err)
	}
	out := serviceOut(sv)
	for _, d := range recent {
		out.Recent = append(out.Recent, deploymentOut(d, nil))
	}
	return out, nil
}

// serviceOut is a service with its workload as the observer last read it (never the cluster
// itself): what runs, or why the cluster could not be read.
func serviceOut(sv *store.Service) *deploypb.Service {
	out := &deploypb.Service{Id: sv.ID, RepositoryId: sv.Repository, Repository: sv.RepoName, Deploy: sv.Deploy, Name: sv.Name,
		Workload: &deploypb.Workload{Namespace: sv.Workload.Namespace, Kind: string(sv.Workload.Kind), Name: sv.Workload.Name},
		Declared: sv.Declared, Group: sv.Group, Running: &deploypb.Running{}}
	if sv.Deployed != nil {
		out.Deployed = deploymentOut(sv.Deployed, nil)
	}
	if sv.Latest != nil {
		out.Latest = deploymentOut(sv.Latest, nil)
	}
	o := sv.Observation
	if o == nil {
		return out
	}
	r := out.Running
	if r.Error = o.Error; o.Error != "" {
		return out
	}
	r.Found, r.Live, r.Replicas, r.Ready, r.Images = o.Found, o.Live, o.Replicas, o.Ready, imagesOut(o.Images)
	for _, p := range o.Pods {
		r.Pods = append(r.Pods, &deploypb.Pod{Name: p.Name, Copy: p.Copy, Ready: p.Ready, Restarts: p.Restarts, Phase: p.Phase,
			Reason: p.Reason, Images: imagesOut(p.Images), Started: timeOut(p.Started), Terminating: p.Terminating})
	}
	return out
}

func imagesOut(m map[string]string) []*deploypb.ContainerImage {
	out := make([]*deploypb.ContainerImage, 0, len(m))
	for c, img := range m {
		out = append(out, &deploypb.ContainerImage{Container: c, Image: img})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Container < out[j].Container })
	return out
}

// ─── Conversions ───

func timeOut(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func deploymentOut(d *store.Deployment, evs []store.Event) *deploypb.Deployment {
	out := &deploypb.Deployment{
		Id: d.ID, RepositoryId: d.Repository, PlanId: d.PlanID, RetryOf: d.RetryOf, Deploy: d.Deploy, Name: d.Name,
		CommitId: d.Commit, FromSeq: d.FromSeq, ToSeq: d.ToSeq, Why: d.Why, Wave: int32(d.Wave), RequestedBy: d.RequestedBy,
		Status: deploypb.Deployment_Status(d.Status), Stage: deploypb.Deployment_Stage(d.Stage), Reason: d.Reason,
		Self: d.Self, HaltBy: d.HaltBy,
		CreatedAt: timeOut(d.CreatedAt), StartedAt: timeOut(d.StartedAt), FinishedAt: timeOut(d.FinishedAt),
	}
	if w := d.Workload; w != nil {
		out.Workload = &deploypb.Workload{Namespace: w.Namespace, Kind: string(w.Kind), Name: w.Name}
	}
	for _, im := range d.Images {
		out.Images = append(out.Images, &deploypb.Image{Container: im.Container, Target: im.Target, PushRun: im.RunID, Reference: im.Reference})
	}
	for _, m := range d.Migrations {
		out.Migrations = append(out.Migrations, &deploypb.Migration{Target: m.Target, Status: deploypb.Migration_Status(m.Status),
			Output: m.Output, Plan: planOut(m.Report), Refused: m.Refused, Allowed: m.Allowed, AllowedBy: m.AllowedBy})
	}
	if len(d.Roll) > 0 {
		var st roll.State
		if json.Unmarshal(d.Roll, &st) == nil {
			out.Roll = &deploypb.Roll{Stage: string(st.Stage), Outcome: string(st.Outcome), Reason: st.Reason, Live: st.Live, New: st.New,
				Replicas: st.Replicas, StageStarted: timeOut(st.Started)}
		}
	}
	for _, e := range evs {
		out.Events = append(out.Events, &deploypb.Event{At: timestamppb.New(e.At), Message: e.Message})
	}
	return out
}

// planOut is a schema_apply's report as the protocol carries it; nil for none.
func planOut(report []byte) *deploypb.MigrationPlan {
	if len(report) == 0 {
		return nil
	}
	var r struct {
		Database string `json:"database"`
		Server   string `json:"server"`
		Steps    []struct {
			Description string `json:"description"`
			SQL         string `json:"sql"`
			Hazards     []struct {
				Class  string `json:"class"`
				Detail string `json:"detail"`
				Gated  bool   `json:"gated"`
			} `json:"hazards"`
		} `json:"steps"`
		Executed bool   `json:"executed"`
		Error    string `json:"error"`
	}
	if json.Unmarshal(report, &r) != nil {
		return nil
	}
	out := &deploypb.MigrationPlan{Database: r.Database, Server: r.Server, Executed: r.Executed, Error: r.Error}
	for _, st := range r.Steps {
		step := &deploypb.MigrationPlan_Step{Description: st.Description, Sql: st.SQL}
		for _, h := range st.Hazards {
			step.Hazards = append(step.Hazards, &deploypb.MigrationPlan_Hazard{Class: h.Class, Detail: h.Detail, Gated: h.Gated})
		}
		out.Steps = append(out.Steps, step)
	}
	return out
}
