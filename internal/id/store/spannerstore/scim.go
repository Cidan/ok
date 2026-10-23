package spannerstore

import (
	"context"
	"encoding/json"
	"errors"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/directory/scim"
)

// scimStore is what an identity provider provisioned, in the scim_users and scim_groups tables:
// one scope's, the installation's ("") or an organization's, by ID.
type scimStore struct {
	c     *spanner.Client
	scope string
}

// errNoRow is a statement that found no row of the scope's.
var errNoRow = errors.New("no row")

func scimErr(err error) error {
	switch {
	case unique(err):
		return scim.ErrConflict
	case errors.Is(err, errNoRow):
		return scim.ErrNotFound
	}
	return err
}

func (st scimStore) write(ctx context.Context, fn func(tx *spanner.ReadWriteTransaction) error) error {
	_, err := spannertxn.ReadWrite(ctx, st.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error { return fn(tx) })
	return scimErr(err)
}

func scanUser(row *spanner.Row) (scim.User, error) {
	var u scim.User
	var raw []byte
	if err := row.Columns(&raw, &u.Created, &u.Modified); err != nil {
		return scim.User{}, err
	}
	created, modified := u.Created, u.Modified
	if err := json.Unmarshal(raw, &u); err != nil {
		return scim.User{}, err
	}
	u.Created, u.Modified = created, modified
	return u, nil
}

// user reads, in r, the scope's user id.
func (st scimStore) user(ctx context.Context, r spannerdata.Reader, id string) (scim.User, error) {
	var u scim.User
	found := false
	err := spannerdata.Each(ctx, r, `SELECT data, created_at, modified_at FROM scim_users WHERE id = @id AND scope = @scope`,
		map[string]any{"id": id, "scope": st.scope}, func(row *spanner.Row) error {
			var err error
			u, err = scanUser(row)
			found = true
			return err
		})
	if err == nil && !found {
		return scim.User{}, errNoRow
	}
	return u, err
}

func (st scimStore) CreateUser(ctx context.Context, u scim.User) (scim.User, error) {
	raw, err := json.Marshal(u)
	if err != nil {
		return scim.User{}, err
	}
	var out scim.User
	err = st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO scim_users (id, user_name, data, scope, created_at, modified_at)
			VALUES (@id, @name, @data, @scope, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			map[string]any{"id": u.ID, "name": u.UserName, "data": raw, "scope": st.scope}); err != nil {
			return err
		}
		var err error
		out, err = st.user(ctx, tx, u.ID)
		return err
	})
	return out, err
}

func (st scimStore) ReplaceUser(ctx context.Context, u scim.User) (scim.User, error) {
	raw, err := json.Marshal(u)
	if err != nil {
		return scim.User{}, err
	}
	var out scim.User
	err = st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `UPDATE scim_users SET user_name = @name, data = @data, modified_at = CURRENT_TIMESTAMP()
			WHERE id = @id AND scope = @scope`, map[string]any{"id": u.ID, "name": u.UserName, "data": raw, "scope": st.scope})
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoRow
		}
		out, err = st.user(ctx, tx, u.ID)
		return err
	})
	return out, err
}

func (st scimStore) User(ctx context.Context, id string) (scim.User, error) {
	u, err := st.user(ctx, st.c.Single(), id)
	return u, scimErr(err)
}

func (st scimStore) Users(ctx context.Context) ([]scim.User, error) {
	var out []scim.User
	err := spannerdata.Each(ctx, st.c.Single(), `SELECT data, created_at, modified_at FROM scim_users WHERE scope = @scope ORDER BY id`,
		map[string]any{"scope": st.scope}, func(row *spanner.Row) error {
			u, err := scanUser(row)
			if err != nil {
				return err
			}
			out = append(out, u)
			return nil
		})
	return out, err
}

// removeMember takes id out of the scope's groups' members, in tx.
func (st scimStore) removeMember(ctx context.Context, tx *spanner.ReadWriteTransaction, id string) error {
	_, err := spannerdata.Update(ctx, tx, `UPDATE scim_groups SET
			members = ARRAY(SELECT m FROM UNNEST(members) AS m WITH OFFSET o WHERE m != @member ORDER BY o), modified_at = CURRENT_TIMESTAMP()
		WHERE @member IN UNNEST(members) AND scope = @scope`, map[string]any{"member": id, "scope": st.scope})
	return err
}

func (st scimStore) DeleteUser(ctx context.Context, id string) error {
	return st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `DELETE FROM scim_users WHERE id = @id AND scope = @scope`, map[string]any{"id": id, "scope": st.scope})
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoRow
		}
		return st.removeMember(ctx, tx, id)
	})
}

func scanGroup(row *spanner.Row) (scim.Group, error) {
	var g scim.Group
	var raw []byte
	var members []string
	if err := row.Columns(&raw, &members, &g.Created, &g.Modified); err != nil {
		return scim.Group{}, err
	}
	created, modified := g.Created, g.Modified
	if err := json.Unmarshal(raw, &g); err != nil {
		return scim.Group{}, err
	}
	g.Members, g.Created, g.Modified = nonNil(members), created, modified
	return g, nil
}

// group reads, in r, the scope's group id.
func (st scimStore) group(ctx context.Context, r spannerdata.Reader, id string) (scim.Group, error) {
	var g scim.Group
	found := false
	err := spannerdata.Each(ctx, r, `SELECT data, members, created_at, modified_at FROM scim_groups WHERE id = @id AND scope = @scope`,
		map[string]any{"id": id, "scope": st.scope}, func(row *spanner.Row) error {
			var err error
			g, err = scanGroup(row)
			found = true
			return err
		})
	if err == nil && !found {
		return scim.Group{}, errNoRow
	}
	return g, err
}

func (st scimStore) CreateGroup(ctx context.Context, g scim.Group) (scim.Group, error) {
	raw, err := json.Marshal(g)
	if err != nil {
		return scim.Group{}, err
	}
	var out scim.Group
	err = st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO scim_groups (id, display_name, data, members, scope, created_at, modified_at)
			VALUES (@id, @name, @data, @members, @scope, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			map[string]any{"id": g.ID, "name": g.DisplayName, "data": raw, "members": nonNil(g.Members), "scope": st.scope}); err != nil {
			return err
		}
		var err error
		out, err = st.group(ctx, tx, g.ID)
		return err
	})
	return out, err
}

func (st scimStore) ReplaceGroup(ctx context.Context, g scim.Group) (scim.Group, error) {
	raw, err := json.Marshal(g)
	if err != nil {
		return scim.Group{}, err
	}
	var out scim.Group
	err = st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `UPDATE scim_groups SET display_name = @name, data = @data, members = @members,
				modified_at = CURRENT_TIMESTAMP()
			WHERE id = @id AND scope = @scope`,
			map[string]any{"id": g.ID, "name": g.DisplayName, "data": raw, "members": nonNil(g.Members), "scope": st.scope})
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoRow
		}
		out, err = st.group(ctx, tx, g.ID)
		return err
	})
	return out, err
}

func (st scimStore) Group(ctx context.Context, id string) (scim.Group, error) {
	g, err := st.group(ctx, st.c.Single(), id)
	return g, scimErr(err)
}

func (st scimStore) Groups(ctx context.Context) ([]scim.Group, error) {
	var out []scim.Group
	err := spannerdata.Each(ctx, st.c.Single(), `SELECT data, members, created_at, modified_at FROM scim_groups WHERE scope = @scope ORDER BY id`,
		map[string]any{"scope": st.scope}, func(row *spanner.Row) error {
			g, err := scanGroup(row)
			if err != nil {
				return err
			}
			out = append(out, g)
			return nil
		})
	return out, err
}

func (st scimStore) DeleteGroup(ctx context.Context, id string) error {
	return st.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `DELETE FROM scim_groups WHERE id = @id AND scope = @scope`, map[string]any{"id": id, "scope": st.scope})
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoRow
		}
		return st.removeMember(ctx, tx, id)
	})
}
