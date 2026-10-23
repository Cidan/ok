package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/directory/scim"
)

// scimStore is what an identity provider provisioned, in the scim_users and scim_groups tables.
type scimStore struct{ pool *pgxpool.Pool }

func scimErr(err error) error {
	var pg *pgconn.PgError
	switch {
	case errors.As(err, &pg) && pg.Code == "23505":
		return scim.ErrConflict
	case errors.Is(err, pgx.ErrNoRows):
		return scim.ErrNotFound
	}
	return err
}

func scanUser(row scanner) (scim.User, error) {
	var u scim.User
	var raw []byte
	if err := row.Scan(&raw, &u.Created, &u.Modified); err != nil {
		return scim.User{}, scimErr(err)
	}
	created, modified := u.Created, u.Modified
	if err := json.Unmarshal(raw, &u); err != nil {
		return scim.User{}, err
	}
	u.Created, u.Modified = created, modified
	return u, nil
}

func (st scimStore) CreateUser(ctx context.Context, u scim.User) (scim.User, error) {
	raw, err := json.Marshal(u)
	if err != nil {
		return scim.User{}, err
	}
	return scanUser(st.pool.QueryRow(ctx, `INSERT INTO scim_users (id, user_name, data) VALUES ($1, $2, $3)
		RETURNING data, created_at, modified_at`, u.ID, u.UserName, raw))
}

func (st scimStore) ReplaceUser(ctx context.Context, u scim.User) (scim.User, error) {
	raw, err := json.Marshal(u)
	if err != nil {
		return scim.User{}, err
	}
	return scanUser(st.pool.QueryRow(ctx, `UPDATE scim_users SET user_name = $2, data = $3, modified_at = now() WHERE id = $1
		RETURNING data, created_at, modified_at`, u.ID, u.UserName, raw))
}

func (st scimStore) User(ctx context.Context, id string) (scim.User, error) {
	return scanUser(st.pool.QueryRow(ctx, `SELECT data, created_at, modified_at FROM scim_users WHERE id = $1`, id))
}

func (st scimStore) Users(ctx context.Context) ([]scim.User, error) {
	rows, err := st.pool.Query(ctx, `SELECT data, created_at, modified_at FROM scim_users ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scim.User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (st scimStore) DeleteUser(ctx context.Context, id string) error {
	return pgdata.Txn(ctx, st.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM scim_users WHERE id = $1`, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return scim.ErrNotFound
		}
		_, err = tx.Exec(ctx, `UPDATE scim_groups SET members = array_remove(members, $1), modified_at = now() WHERE $1 = ANY(members)`, id)
		return err
	})
}

func scanGroup(row scanner) (scim.Group, error) {
	var g scim.Group
	var raw []byte
	var members []string
	if err := row.Scan(&raw, &members, &g.Created, &g.Modified); err != nil {
		return scim.Group{}, scimErr(err)
	}
	created, modified := g.Created, g.Modified
	if err := json.Unmarshal(raw, &g); err != nil {
		return scim.Group{}, err
	}
	g.Members, g.Created, g.Modified = members, created, modified
	return g, nil
}

func (st scimStore) CreateGroup(ctx context.Context, g scim.Group) (scim.Group, error) {
	raw, err := json.Marshal(g)
	if err != nil {
		return scim.Group{}, err
	}
	members := g.Members
	if members == nil {
		members = []string{}
	}
	return scanGroup(st.pool.QueryRow(ctx, `INSERT INTO scim_groups (id, display_name, data, members) VALUES ($1, $2, $3, $4)
		RETURNING data, members, created_at, modified_at`, g.ID, g.DisplayName, raw, members))
}

func (st scimStore) ReplaceGroup(ctx context.Context, g scim.Group) (scim.Group, error) {
	raw, err := json.Marshal(g)
	if err != nil {
		return scim.Group{}, err
	}
	members := g.Members
	if members == nil {
		members = []string{}
	}
	return scanGroup(st.pool.QueryRow(ctx, `UPDATE scim_groups SET display_name = $2, data = $3, members = $4, modified_at = now() WHERE id = $1
		RETURNING data, members, created_at, modified_at`, g.ID, g.DisplayName, raw, members))
}

func (st scimStore) Group(ctx context.Context, id string) (scim.Group, error) {
	return scanGroup(st.pool.QueryRow(ctx, `SELECT data, members, created_at, modified_at FROM scim_groups WHERE id = $1`, id))
}

func (st scimStore) Groups(ctx context.Context) ([]scim.Group, error) {
	rows, err := st.pool.Query(ctx, `SELECT data, members, created_at, modified_at FROM scim_groups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scim.Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (st scimStore) DeleteGroup(ctx context.Context, id string) error {
	return pgdata.Txn(ctx, st.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM scim_groups WHERE id = $1`, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return scim.ErrNotFound
		}
		_, err = tx.Exec(ctx, `UPDATE scim_groups SET members = array_remove(members, $1), modified_at = now() WHERE $1 = ANY(members)`, id)
		return err
	})
}
