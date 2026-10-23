package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/deploy/store"
)

func (x queries) RepositoryOwner(ctx context.Context, id int64) (string, error) {
	var owner string
	err := x.Read(func(r spannerdata.Reader) error {
		_, err := spannerdata.First(ctx, r, `SELECT owner_id FROM repository WHERE repository_id = @id`, map[string]any{"id": id}, &owner)
		return err
	})
	return owner, err
}

var sessionColumns = []string{"account_id", "holder", "address", "lease_until", "connected_at"}

func (x queries) ClaimAgentSession(ctx context.Context, s store.AgentSession) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("agent_session", sessionColumns,
			[]any{s.Account, s.Holder, s.Address, s.LeaseUntil, s.ConnectedAt})})
	})
}

func (x queries) RenewAgentSession(ctx context.Context, account, holder string, until time.Time) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE agent_session SET lease_until = @until WHERE account_id = @account AND holder = @holder`,
		map[string]any{"account": account, "holder": holder, "until": until})
	return n > 0, err
}

func (x queries) ReleaseAgentSession(ctx context.Context, account, holder string) error {
	_, err := x.Exec(ctx, `DELETE FROM agent_session WHERE account_id = @account AND holder = @holder`,
		map[string]any{"account": account, "holder": holder})
	return err
}

func (x queries) AgentSession(ctx context.Context, account string) (store.AgentSession, bool, error) {
	var s store.AgentSession
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT account_id, holder, address, lease_until, connected_at FROM agent_session WHERE account_id = @account`,
			map[string]any{"account": account}, &s.Account, &s.Holder, &s.Address, &s.LeaseUntil, &s.ConnectedAt)
		return err
	})
	return s, ok, err
}
