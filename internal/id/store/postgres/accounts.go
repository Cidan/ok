package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) PutPassword(ctx context.Context, principal, hash string, at time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO passwords (principal_id, hash, updated_at) VALUES ($1, $2, $3)
		ON CONFLICT (principal_id) DO UPDATE SET hash = EXCLUDED.hash, updated_at = EXCLUDED.updated_at`, principal, hash, at)
	return err
}

func (x queries) Password(ctx context.Context, principal string) (string, error) {
	var h string
	err := x.q.QueryRow(ctx, `SELECT hash FROM passwords WHERE principal_id = $1`, principal).Scan(&h)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return h, err
}

func (x queries) InsertEmailToken(ctx context.Context, hash []byte, t store.EmailToken) error {
	var principal *string
	if t.Principal != "" {
		principal = &t.Principal
	}
	_, err := x.q.Exec(ctx, `INSERT INTO email_tokens (token_hash, purpose, email, principal_id, handle, hash, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, hash, t.Purpose, t.Email, principal, t.Handle, t.Hash, t.Expires)
	return err
}

func (x queries) TakeEmailToken(ctx context.Context, hash []byte, purpose string, now time.Time) (store.EmailToken, error) {
	var t store.EmailToken
	var principal *string
	err := x.q.QueryRow(ctx, `DELETE FROM email_tokens WHERE token_hash = $1 AND purpose = $2 AND expires_at > $3
		RETURNING purpose, email, principal_id, handle, hash, expires_at`, hash, purpose, now).
		Scan(&t.Purpose, &t.Email, &principal, &t.Handle, &t.Hash, &t.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.EmailToken{}, store.ErrNotFound
	}
	if principal != nil {
		t.Principal = *principal
	}
	return t, err
}

func (x queries) DeleteExpiredEmailTokens(ctx context.Context, now time.Time) error {
	_, err := x.q.Exec(ctx, `DELETE FROM email_tokens WHERE expires_at <= $1`, now)
	return err
}

func (x queries) InsertLoginLink(ctx context.Context, provider, subject, principal string, at time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO login_links (provider, subject, principal_id, created_at) VALUES ($1, $2, $3, $4)`, provider, subject, principal, at)
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) LinkedPrincipal(ctx context.Context, provider, subject string) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+prefixed("p")+` FROM login_links l JOIN principals p ON p.id = l.principal_id
		WHERE l.provider = $1 AND l.subject = $2`, provider, subject))
}

func (x queries) LoginLinks(ctx context.Context, principal string) ([]store.LoginLink, error) {
	rows, err := x.q.Query(ctx, `SELECT provider, subject, created_at FROM login_links WHERE principal_id = $1 ORDER BY provider`, principal)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.LoginLink, error) {
		var l store.LoginLink
		return l, row.Scan(&l.Provider, &l.Subject, &l.Created)
	})
}

func (x queries) DeleteLoginLink(ctx context.Context, principal, provider string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM login_links WHERE principal_id = $1 AND provider = $2`, principal, provider)
	return tag.RowsAffected() > 0, err
}

func (x queries) SignInFailures(ctx context.Context, key string, since time.Time) (int, error) {
	var n int
	err := x.q.QueryRow(ctx, `SELECT failures FROM sign_in_failures WHERE key = $1 AND since > $2`, key, since).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

func (x queries) NoteSignInFailure(ctx context.Context, key string, now, since time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO sign_in_failures (key, failures, since) VALUES ($1, 1, $2)
		ON CONFLICT (key) DO UPDATE SET
			failures = CASE WHEN sign_in_failures.since > $3 THEN sign_in_failures.failures + 1 ELSE 1 END,
			since    = CASE WHEN sign_in_failures.since > $3 THEN sign_in_failures.since ELSE $2 END`, key, now, since)
	return err
}

func (x queries) ClearSignInFailures(ctx context.Context, key string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM sign_in_failures WHERE key = $1`, key)
	return err
}
