package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// ─── Apps ───

const appColumns = `id, robot_id, organization_id, public, public_key, created_at`

func scanApp(row *spanner.Row) (store.App, error) {
	var a store.App
	err := row.Columns(&a.ID, &a.Robot, &a.Organization, &a.Public, &a.PublicKey, &a.Created)
	return a, err
}

func (x queries) apps(ctx context.Context, where string, params map[string]any) ([]store.App, error) {
	var out []store.App
	err := x.readAll(ctx, `SELECT `+appColumns+` FROM apps `+where, params, func() { out = nil }, func(row *spanner.Row) error {
		a, err := scanApp(row)
		if err != nil {
			return err
		}
		out = append(out, a)
		return nil
	})
	return out, err
}

func oneApp(as []store.App, err error) (store.App, error) {
	if err != nil {
		return store.App{}, err
	}
	if len(as) == 0 {
		return store.App{}, store.ErrNotFound
	}
	return as[0], nil
}

func (x queries) InsertApp(ctx context.Context, a store.App) error {
	_, err := x.Exec(ctx, `INSERT INTO apps (`+appColumns+`) VALUES (@id, @robot, @org, @public, @key, @created)`,
		map[string]any{"id": a.ID, "robot": a.Robot, "org": a.Organization, "public": a.Public, "key": a.PublicKey, "created": a.Created})
	return err
}

func (x queries) App(ctx context.Context, id string) (store.App, error) {
	return oneApp(x.apps(ctx, `WHERE id = @id`, map[string]any{"id": id}))
}

func (x queries) AppOfRobot(ctx context.Context, robot string) (store.App, error) {
	return oneApp(x.apps(ctx, `WHERE robot_id = @robot`, map[string]any{"robot": robot}))
}

func (x queries) Apps(ctx context.Context, org string) ([]store.App, error) {
	return x.apps(ctx, `WHERE organization_id = @org ORDER BY created_at, id`, map[string]any{"org": org})
}

func (x queries) SetAppKey(ctx context.Context, id string, key []byte) error {
	n, err := x.Exec(ctx, `UPDATE apps SET public_key = @key WHERE id = @id`, map[string]any{"id": id, "key": key})
	if err == nil && n == 0 {
		return store.ErrNotFound
	}
	return err
}

func (x queries) DeleteApp(ctx context.Context, id string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, sql := range []string{
			`DELETE FROM tokens WHERE installation_id IN (SELECT id FROM app_installations WHERE app_id = @id)`,
			`DELETE FROM app_installations WHERE app_id = @id`,
			`DELETE FROM apps WHERE id = @id`,
		} {
			if _, err := spannerdata.Update(ctx, tx, sql, map[string]any{"id": id}); err != nil {
				return err
			}
		}
		return nil
	})
}

const installationColumns = `id, app_id, organization_id, role, repositories, created_by, created_at`

func scanInstallation(row *spanner.Row) (store.Installation, error) {
	var i store.Installation
	var role string
	if err := row.Columns(&i.ID, &i.App, &i.Organization, &role, &i.Repositories, &i.CreatedBy, &i.Created); err != nil {
		return store.Installation{}, err
	}
	var err error
	i.Role, err = tenancy.ParseRole(role)
	i.Repositories = nonNil(i.Repositories)
	return i, err
}

func (x queries) installations(ctx context.Context, where string, params map[string]any) ([]store.Installation, error) {
	var out []store.Installation
	err := x.readAll(ctx, `SELECT `+installationColumns+` FROM app_installations `+where, params, func() { out = nil },
		func(row *spanner.Row) error {
			i, err := scanInstallation(row)
			if err != nil {
				return err
			}
			out = append(out, i)
			return nil
		})
	return out, err
}

func oneInstallation(is []store.Installation, err error) (store.Installation, error) {
	if err != nil {
		return store.Installation{}, err
	}
	if len(is) == 0 {
		return store.Installation{}, store.ErrNotFound
	}
	return is[0], nil
}

func (x queries) InsertInstallation(ctx context.Context, i store.Installation) error {
	_, err := x.Exec(ctx, `INSERT INTO app_installations (`+installationColumns+`) VALUES (@id, @app, @org, @role, @repos, @by, @created)`,
		map[string]any{"id": i.ID, "app": i.App, "org": i.Organization, "role": i.Role.String(), "repos": nonNil(i.Repositories),
			"by": i.CreatedBy, "created": i.Created})
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) UpdateInstallation(ctx context.Context, i store.Installation) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `UPDATE app_installations SET role = @role, repositories = @repos WHERE id = @id`,
			map[string]any{"id": i.ID, "role": i.Role.String(), "repos": nonNil(i.Repositories)}); err != nil {
			return err
		}
		// Its tokens hold what it was installed with: they go, and the app asks again.
		_, err := spannerdata.Update(ctx, tx, `DELETE FROM tokens WHERE installation_id = @id`, map[string]any{"id": i.ID})
		return err
	})
}

func (x queries) Installation(ctx context.Context, id string) (store.Installation, error) {
	return oneInstallation(x.installations(ctx, `WHERE id = @id`, map[string]any{"id": id}))
}

func (x queries) InstallationOf(ctx context.Context, app, org string) (store.Installation, error) {
	return oneInstallation(x.installations(ctx, `WHERE app_id = @app AND organization_id = @org`, map[string]any{"app": app, "org": org}))
}

func (x queries) Installations(ctx context.Context, org string) ([]store.Installation, error) {
	return x.installations(ctx, `WHERE organization_id = @org ORDER BY created_at, id`, map[string]any{"org": org})
}

func (x queries) InstallationsOfApp(ctx context.Context, app string) ([]store.Installation, error) {
	return x.installations(ctx, `WHERE app_id = @app ORDER BY created_at, id`, map[string]any{"app": app})
}

func (x queries) DeleteInstallation(ctx context.Context, id string) (bool, error) {
	var had bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `DELETE FROM tokens WHERE installation_id = @id`, map[string]any{"id": id}); err != nil {
			return err
		}
		n, err := spannerdata.Update(ctx, tx, `DELETE FROM app_installations WHERE id = @id`, map[string]any{"id": id})
		had = n == 1
		return err
	})
	return had, err
}

// ─── Accounts ───

func (x queries) PutPassword(ctx context.Context, principal, hash string, at time.Time) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO passwords (principal_id, password_hash, updated_at) VALUES (@principal, @hash, @at)`,
		map[string]any{"principal": principal, "hash": hash, "at": at})
	return err
}

func (x queries) Password(ctx context.Context, principal string) (string, error) {
	var h string
	err := x.readOne(ctx, `SELECT password_hash FROM passwords WHERE principal_id = @principal`, map[string]any{"principal": principal},
		func(row *spanner.Row) error { return row.Columns(&h) })
	return h, err
}

func (x queries) InsertEmailToken(ctx context.Context, hash []byte, t store.EmailToken) error {
	_, err := x.Exec(ctx, `INSERT INTO email_tokens (token_hash, purpose, email, principal_id, handle, password_hash, expires_at)
		VALUES (@token, @purpose, @email, @principal, @handle, @hash, @expires)`,
		map[string]any{"token": hash, "purpose": t.Purpose, "email": t.Email, "principal": nullString(t.Principal), "handle": t.Handle,
			"hash": t.Hash, "expires": t.Expires})
	return err
}

func (x queries) TakeEmailToken(ctx context.Context, hash []byte, purpose string, now time.Time) (store.EmailToken, error) {
	var t store.EmailToken
	found := false
	err := x.Returning(ctx, `DELETE FROM email_tokens WHERE token_hash = @token AND purpose = @purpose AND expires_at > @now
		THEN RETURN purpose, email, IFNULL(principal_id, ''), handle, password_hash, expires_at`,
		map[string]any{"token": hash, "purpose": purpose, "now": now}, func() { t, found = store.EmailToken{}, false },
		func(row *spanner.Row) error {
			found = true
			return row.Columns(&t.Purpose, &t.Email, &t.Principal, &t.Handle, &t.Hash, &t.Expires)
		})
	if err == nil && !found {
		return store.EmailToken{}, store.ErrNotFound
	}
	return t, err
}

func (x queries) DeleteExpiredEmailTokens(ctx context.Context, now time.Time) error {
	return x.sweep(ctx, `DELETE FROM email_tokens WHERE expires_at <= @now`, map[string]any{"now": now})
}

func (x queries) InsertLoginLink(ctx context.Context, provider, subject, principal string, at time.Time) error {
	_, err := x.Exec(ctx, `INSERT INTO login_links (provider, subject, principal_id, created_at) VALUES (@provider, @subject, @principal, @at)`,
		map[string]any{"provider": provider, "subject": subject, "principal": principal, "at": at})
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) LinkedPrincipal(ctx context.Context, provider, subject string) (store.Principal, error) {
	return x.principal(ctx, `SELECT `+prefixed("p")+` FROM login_links l JOIN principals p ON p.id = l.principal_id
		WHERE l.provider = @provider AND l.subject = @subject`, map[string]any{"provider": provider, "subject": subject})
}

func (x queries) LoginLinks(ctx context.Context, principal string) ([]store.LoginLink, error) {
	var out []store.LoginLink
	err := x.readAll(ctx, `SELECT provider, subject, created_at FROM login_links WHERE principal_id = @principal ORDER BY provider`,
		map[string]any{"principal": principal}, func() { out = nil }, func(row *spanner.Row) error {
			var l store.LoginLink
			if err := row.Columns(&l.Provider, &l.Subject, &l.Created); err != nil {
				return err
			}
			out = append(out, l)
			return nil
		})
	return out, err
}

func (x queries) DeleteLoginLink(ctx context.Context, principal, provider string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM login_links WHERE principal_id = @principal AND provider = @provider`,
		map[string]any{"principal": principal, "provider": provider})
	return n > 0, err
}

func (x queries) SignInFailures(ctx context.Context, key string, since time.Time) (int, error) {
	var n int64
	err := x.readOne(ctx, `SELECT failures FROM sign_in_failures WHERE key = @key AND since > @since`, map[string]any{"key": key, "since": since},
		func(row *spanner.Row) error { return row.Columns(&n) })
	if err == store.ErrNotFound {
		return 0, nil
	}
	return int(n), err
}

// NoteSignInFailure counts a failure in the key's window, or starts one at now when its window
// began before since.
func (x queries) NoteSignInFailure(ctx context.Context, key string, now, since time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"key": key, "now": now, "since": since}
		n, err := spannerdata.Update(ctx, tx, `UPDATE sign_in_failures SET
				failures = IF(since > @since, failures + 1, 1),
				since    = IF(since > @since, since, @now)
			WHERE key = @key`, p)
		if err != nil || n > 0 {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO sign_in_failures (key, failures, since) VALUES (@key, 1, @now)`, p)
		return err
	})
}

func (x queries) ClearSignInFailures(ctx context.Context, key string) error {
	_, err := x.Exec(ctx, `DELETE FROM sign_in_failures WHERE key = @key`, map[string]any{"key": key})
	return err
}
