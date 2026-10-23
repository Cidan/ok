package service

import (
	"context"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/id/store"
)

// Quotas of CI (docs/hosted.md, "Hosted automation, as built"): administrators set an account's,
// and ok automation reads them for its admission.

// SetQuota sets an account's quota, or gives it the default again.
func (s *Server) SetQuota(ctx context.Context, req *identitypb.SetQuotaRequest) (*identitypb.Quota, error) {
	c, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	account, _, err := s.owner(ctx, req.GetAccount())
	if err != nil {
		return nil, grpcError(err)
	}
	if req.GetReset_() {
		err = s.cfg.Store.ResetQuota(ctx, account.ID)
	} else {
		err = s.cfg.Store.SetQuota(ctx, account.ID, store.Quota{Concurrent: int(req.GetConcurrent()), Minutes: req.GetMinutes(),
			Weight: int(req.GetWeight())})
	}
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, c.Principal, "account.quota", map[string]any{"account": account.Name, "concurrent": req.GetConcurrent(),
		"minutes": req.GetMinutes(), "weight": req.GetWeight(), "reset": req.GetReset_()})
	return s.quotaOut(ctx, req.GetAccount(), account.ID)
}

// GetQuota is an account's quota.
func (s *Server) GetQuota(ctx context.Context, req *identitypb.GetQuotaRequest) (*identitypb.Quota, error) {
	if _, err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}
	account, _, err := s.owner(ctx, req.GetAccount())
	if err != nil {
		return nil, grpcError(err)
	}
	return s.quotaOut(ctx, req.GetAccount(), account.ID)
}

func (s *Server) quotaOut(ctx context.Context, handle, account string) (*identitypb.Quota, error) {
	qs, err := s.cfg.Store.QuotasOf(ctx, []string{account})
	if err != nil {
		return nil, grpcError(err)
	}
	return quotaPB(account, handle, qs[account]), nil
}

func quotaPB(account, handle string, q store.Quota) *identitypb.Quota {
	return &identitypb.Quota{AccountId: account, Account: handle, Concurrent: int32(q.Concurrent), Minutes: q.Minutes, Weight: int32(q.Weight),
		Default: q.Default}
}

// GetQuotas are accounts' quotas, by ID, with their use in the cells but the one asking: ok
// automation's admission's.
func (s *Server) GetQuotas(ctx context.Context, req *identitypb.GetQuotasRequest) (*identitypb.GetQuotasResponse, error) {
	qs, err := s.cfg.Store.QuotasOf(ctx, req.GetAccountIds())
	if err != nil {
		return nil, grpcError(err)
	}
	used, err := s.cfg.Store.UsageElsewhere(ctx, req.GetAccountIds(), req.GetCell(), req.GetMonth())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.GetQuotasResponse{}
	for _, a := range req.GetAccountIds() {
		q := quotaPB(a, "", qs[a])
		q.RunningElsewhere, q.SecondsElsewhere = used[a].Running, used[a].Seconds
		out.Quotas = append(out.Quotas, q)
	}
	return out, nil
}

// ReportUsage records a cell's use this month by each account that ran or runs there.
func (s *Server) ReportUsage(ctx context.Context, req *identitypb.ReportUsageRequest) (*identitypb.ReportUsageResponse, error) {
	usage := make([]store.Usage, 0, len(req.GetUsage()))
	for _, u := range req.GetUsage() {
		usage = append(usage, store.Usage{Account: u.GetAccountId(), Seconds: u.GetSeconds(), Running: u.GetRunning()})
	}
	if err := s.cfg.Store.ReportUsage(ctx, req.GetCell(), req.GetMonth(), usage); err != nil {
		return nil, tenancyError(err)
	}
	return &identitypb.ReportUsageResponse{}, nil
}
