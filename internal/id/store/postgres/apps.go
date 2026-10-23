package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

const appColumns = `id, robot_id, organization_id, public, public_key, created_at`

func scanApp(row pgx.Row) (store.App, error) {
	var a store.App
	err := row.Scan(&a.ID, &a.Robot, &a.Organization, &a.Public, &a.PublicKey, &a.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.App{}, store.ErrNotFound
	}
	return a, err
}

func (x queries) InsertApp(ctx context.Context, a store.App) error {
	_, err := x.q.Exec(ctx, `INSERT INTO apps (`+appColumns+`) VALUES ($1, $2, $3, $4, $5, $6)`,
		a.ID, a.Robot, a.Organization, a.Public, a.PublicKey, a.Created)
	return err
}

func (x queries) App(ctx context.Context, id string) (store.App, error) {
	return scanApp(x.q.QueryRow(ctx, `SELECT `+appColumns+` FROM apps WHERE id = $1`, id))
}

func (x queries) AppOfRobot(ctx context.Context, robot string) (store.App, error) {
	return scanApp(x.q.QueryRow(ctx, `SELECT `+appColumns+` FROM apps WHERE robot_id = $1`, robot))
}

func (x queries) Apps(ctx context.Context, org string) ([]store.App, error) {
	rows, err := x.q.Query(ctx, `SELECT `+appColumns+` FROM apps WHERE organization_id = $1 ORDER BY created_at, id`, org)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.App, error) { return scanApp(row) })
}

func (x queries) SetAppKey(ctx context.Context, id string, key []byte) error {
	tag, err := x.q.Exec(ctx, `UPDATE apps SET public_key = $2 WHERE id = $1`, id, key)
	if err == nil && tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}

func (x queries) DeleteApp(ctx context.Context, id string) error {
	if _, err := x.q.Exec(ctx, `DELETE FROM tokens WHERE installation_id IN (SELECT id FROM app_installations WHERE app_id = $1)`, id); err != nil {
		return err
	}
	if _, err := x.q.Exec(ctx, `DELETE FROM app_installations WHERE app_id = $1`, id); err != nil {
		return err
	}
	_, err := x.q.Exec(ctx, `DELETE FROM apps WHERE id = $1`, id)
	return err
}

const installationColumns = `id, app_id, organization_id, role, repositories, created_by, created_at`

func scanInstallation(row pgx.Row) (store.Installation, error) {
	var i store.Installation
	var role string
	err := row.Scan(&i.ID, &i.App, &i.Organization, &role, &i.Repositories, &i.CreatedBy, &i.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Installation{}, store.ErrNotFound
	}
	if err != nil {
		return store.Installation{}, err
	}
	i.Role, err = tenancy.ParseRole(role)
	return i, err
}

func (x queries) InsertInstallation(ctx context.Context, i store.Installation) error {
	repos := i.Repositories
	if repos == nil {
		repos = []int64{}
	}
	_, err := x.q.Exec(ctx, `INSERT INTO app_installations (`+installationColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		i.ID, i.App, i.Organization, i.Role.String(), repos, i.CreatedBy, i.Created)
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) UpdateInstallation(ctx context.Context, i store.Installation) error {
	repos := i.Repositories
	if repos == nil {
		repos = []int64{}
	}
	_, err := x.q.Exec(ctx, `UPDATE app_installations SET role = $2, repositories = $3 WHERE id = $1`, i.ID, i.Role.String(), repos)
	if err != nil {
		return err
	}
	// Its tokens hold what it was installed with: they go, and the app asks again.
	_, err = x.q.Exec(ctx, `DELETE FROM tokens WHERE installation_id = $1`, i.ID)
	return err
}

func (x queries) Installation(ctx context.Context, id string) (store.Installation, error) {
	return scanInstallation(x.q.QueryRow(ctx, `SELECT `+installationColumns+` FROM app_installations WHERE id = $1`, id))
}

func (x queries) InstallationOf(ctx context.Context, app, org string) (store.Installation, error) {
	return scanInstallation(x.q.QueryRow(ctx, `SELECT `+installationColumns+` FROM app_installations WHERE app_id = $1 AND organization_id = $2`, app, org))
}

func (x queries) installations(ctx context.Context, where string, arg string) ([]store.Installation, error) {
	rows, err := x.q.Query(ctx, `SELECT `+installationColumns+` FROM app_installations WHERE `+where+` = $1 ORDER BY created_at, id`, arg)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Installation, error) { return scanInstallation(row) })
}

func (x queries) Installations(ctx context.Context, org string) ([]store.Installation, error) {
	return x.installations(ctx, "organization_id", org)
}

func (x queries) InstallationsOfApp(ctx context.Context, app string) ([]store.Installation, error) {
	return x.installations(ctx, "app_id", app)
}

func (x queries) DeleteInstallation(ctx context.Context, id string) (bool, error) {
	if _, err := x.q.Exec(ctx, `DELETE FROM tokens WHERE installation_id = $1`, id); err != nil {
		return false, err
	}
	tag, err := x.q.Exec(ctx, `DELETE FROM app_installations WHERE id = $1`, id)
	return tag.RowsAffected() == 1, err
}
