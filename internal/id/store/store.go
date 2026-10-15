// Package store is ok identity's directory in PostgreSQL: principals and retired names, the system
// of record's mirror (a directory.Store), the groups-and-ACLs tree, robots' bindings, credentials,
// sessions, device logins, passkeys, and sign-in ceremonies. Every change that must hold together
// is one transaction.
package store

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the store's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Errors the store answers with.
var (
	ErrNotFound = errors.New("not found")
	// ErrTaken is a name held, or once held, by another.
	ErrTaken = errors.New("the name is taken, or was, and a name is never given to another")
)

// Kind is what a principal is.
type Kind string

const (
	Human Kind = "human"
	Robot Kind = "robot"
)

// Principal is a principal as the store holds it.
type Principal struct {
	ID          string
	Name        string
	Kind        Kind
	Status      directory.Status
	ExternalID  string
	Email       string
	DisplayName string
	Created     time.Time
}

// Active reports whether the principal acts.
func (p Principal) Active() bool { return p.Status == directory.Active }

// Store is the directory's store.
type Store struct {
	pool  *pgxpool.Pool
	names identity.Names
	now   func() time.Time
}

// Open migrates the database pool reaches and returns its store.
func Open(ctx context.Context, pool *pgxpool.Pool, names identity.Names) (*Store, error) {
	if _, err := migrate.Apply(ctx, pool, "identity", Migrations); err != nil {
		return nil, err
	}
	return &Store{pool: pool, names: names, now: time.Now}, nil
}

// Names is the naming the store's principals and sets are under.
func (s *Store) Names() identity.Names { return s.names }

// Healthy reports whether the database answers.
func (s *Store) Healthy(ctx context.Context) bool { return s.pool.Ping(ctx) == nil }

// newID makes an ID: prefix and 26 random base32 characters.
func newID(prefix string) string {
	var b [16]byte
	rand.Read(b[:])
	return prefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

func statusText(s directory.Status) string {
	switch s {
	case directory.Suspended:
		return "suspended"
	case directory.Deleted:
		return "deleted"
	}
	return "active"
}

func parseStatus(s string) directory.Status {
	switch s {
	case "suspended":
		return directory.Suspended
	case "deleted":
		return directory.Deleted
	}
	return directory.Active
}

const principalColumns = `id, name, kind, status, COALESCE(external_id, ''), email, display_name, created_at`

type scanner interface{ Scan(...any) error }

func scanPrincipal(row scanner) (Principal, error) {
	var p Principal
	var kind, status string
	if err := row.Scan(&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Principal{}, ErrNotFound
		}
		return Principal{}, err
	}
	p.Kind, p.Status = Kind(kind), parseStatus(status)
	return p, nil
}

// Principal returns the principal with a name.
func (s *Store) Principal(ctx context.Context, name string) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = $1`, name))
}

// PrincipalByID returns the principal with an ID.
func (s *Store) PrincipalByID(ctx context.Context, id string) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE id = $1`, id))
}

// Filter selects principals to list.
type Filter struct {
	Kind            Kind   // "" for both
	Query           string // a substring of the name, email, or display name
	IncludeInactive bool
	After           string // list names after this one
	Limit           int
}

// Principals lists principals by name, from after f.After, at most f.Limit (all for 0).
func (s *Store) Principals(ctx context.Context, f Filter) ([]Principal, error) {
	q := `SELECT ` + principalColumns + ` FROM principals WHERE name > $1`
	args := []any{f.After}
	if f.Kind != "" {
		args = append(args, string(f.Kind))
		q += fmt.Sprintf(" AND kind = $%d", len(args))
	}
	if !f.IncludeInactive {
		q += " AND status = 'active'"
	}
	if f.Query != "" {
		args = append(args, "%"+likeEscape(strings.ToLower(f.Query))+"%")
		q += fmt.Sprintf(` AND (lower(name) LIKE $%[1]d OR lower(email) LIKE $%[1]d OR lower(display_name) LIKE $%[1]d)`, len(args))
	}
	q += " ORDER BY name"
	if f.Limit > 0 {
		args = append(args, f.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Principal
	for rows.Next() {
		p, err := scanPrincipal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// RetiredName is a name no one will have again.
type RetiredName struct {
	Name    string
	Retired time.Time
	Reason  string
}

// RetiredNames lists every retired name.
func (s *Store) RetiredNames(ctx context.Context) ([]RetiredName, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, retired_at, reason FROM retired_names ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetiredName
	for rows.Next() {
		var r RetiredName
		if err := rows.Scan(&r.Name, &r.Retired, &r.Reason); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// nameFree reports whether no principal or set holds a name, and none ever did.
func nameFree(ctx context.Context, tx pgx.Tx, name string) (bool, error) {
	var held bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM principals WHERE name = $1)
		OR EXISTS (SELECT 1 FROM retired_names WHERE name = $1)
		OR EXISTS (SELECT 1 FROM synced_groups WHERE name = $1)
		OR EXISTS (SELECT 1 FROM tree_sets WHERE name = $1)`, name).Scan(&held)
	return !held, err
}

func retire(ctx context.Context, tx pgx.Tx, name, reason string) error {
	_, err := tx.Exec(ctx, `INSERT INTO retired_names (name, reason) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`, name, reason)
	return err
}

// Audit records what an actor did.
func (s *Store) Audit(ctx context.Context, actor, action string, detail map[string]any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO audit (actor, action, detail) VALUES ($1, $2, $3)`, actor, action, detail)
	return err
}

// CreateRobot makes a robot: name is its full name, <name>.robot.<domain>.
func (s *Store) CreateRobot(ctx context.Context, name string) (Principal, error) {
	if k, ok := s.names.KindOf(name); !ok || k != identity.Robot {
		return Principal{}, fmt.Errorf("%q is not a robot's name under %s", name, s.names.Domain())
	}
	var p Principal
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		free, err := nameFree(ctx, tx, name)
		if err != nil {
			return err
		}
		if !free {
			return ErrTaken
		}
		p, err = scanPrincipal(tx.QueryRow(ctx, `INSERT INTO principals (id, name, kind, status) VALUES ($1, $2, 'robot', 'active')
			RETURNING `+principalColumns, newID("prn_"), name))
		return err
	})
	return p, err
}

// EnsureRobot makes a robot if there is none of the name, and returns it: how the installation's
// configured robots — its roles' — come to exist.
func (s *Store) EnsureRobot(ctx context.Context, name string) (Principal, error) {
	p, err := s.Principal(ctx, name)
	if err == nil {
		if p.Kind != Robot {
			return Principal{}, fmt.Errorf("%s is a person, not a robot", name)
		}
		if !p.Active() {
			return Principal{}, fmt.Errorf("the robot %s was deleted; its name is retired", name)
		}
		return p, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Principal{}, err
	}
	return s.CreateRobot(ctx, name)
}

// DeleteRobot deletes a robot: a tombstone, its name retired, its credentials revoked, its
// bindings gone.
func (s *Store) DeleteRobot(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		p, err := scanPrincipal(tx.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = $1 FOR UPDATE`, name))
		if err != nil {
			return err
		}
		if p.Kind != Robot {
			return fmt.Errorf("%s is a person: the system of record deletes people", name)
		}
		if p.Status == directory.Deleted {
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE principals SET status = 'deleted', updated_at = now() WHERE id = $1`, p.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE credentials SET revoked_at = now() WHERE principal_id = $1 AND revoked_at IS NULL`, p.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM robot_bindings WHERE principal_id = $1`, p.ID); err != nil {
			return err
		}
		return retire(ctx, tx, name, "the robot was deleted")
	})
}

// Binding names a robot by the tokens another issuer signs for it.
type Binding struct {
	Issuer, Subject string
}

// Bind lets the robot sign in with tokens for b. A binding names one robot.
func (s *Store) Bind(ctx context.Context, robot string, b Binding) error {
	if b.Issuer == "" || b.Subject == "" {
		return errors.New("a binding names an issuer and a subject")
	}
	p, err := s.Principal(ctx, robot)
	if err != nil {
		return err
	}
	if p.Kind != Robot || !p.Active() {
		return fmt.Errorf("%s is not an active robot", robot)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO robot_bindings (issuer, subject, principal_id) VALUES ($1, $2, $3)`, b.Issuer, b.Subject, p.ID)
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		return fmt.Errorf("issuer %s's subject %s already names a robot", b.Issuer, b.Subject)
	}
	return err
}

// Unbind removes a binding from a robot.
func (s *Store) Unbind(ctx context.Context, robot string, b Binding) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM robot_bindings rb USING principals p
		WHERE rb.principal_id = p.id AND p.name = $1 AND rb.issuer = $2 AND rb.subject = $3`, robot, b.Issuer, b.Subject)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// BoundRobot returns the robot a binding names.
func (s *Store) BoundRobot(ctx context.Context, b Binding) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, `SELECT `+prefixed("p")+` FROM robot_bindings rb JOIN principals p ON p.id = rb.principal_id
		WHERE rb.issuer = $1 AND rb.subject = $2`, b.Issuer, b.Subject))
}

// RobotWithBindings is a robot and its bindings.
type RobotWithBindings struct {
	Principal
	Bindings []Binding
}

// Robots lists the robots that are not deleted, with their bindings.
func (s *Store) Robots(ctx context.Context) ([]RobotWithBindings, error) {
	robots, err := s.Principals(ctx, Filter{Kind: Robot})
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT principal_id, issuer, subject FROM robot_bindings ORDER BY issuer, subject`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bindings := map[string][]Binding{}
	for rows.Next() {
		var id string
		var b Binding
		if err := rows.Scan(&id, &b.Issuer, &b.Subject); err != nil {
			return nil, err
		}
		bindings[id] = append(bindings[id], b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]RobotWithBindings, 0, len(robots))
	for _, r := range robots {
		out = append(out, RobotWithBindings{Principal: r, Bindings: bindings[r.ID]})
	}
	return out, nil
}

// prefixed is principalColumns qualified by a table alias.
func prefixed(alias string) string {
	return alias + `.id, ` + alias + `.name, ` + alias + `.kind, ` + alias + `.status, COALESCE(` + alias + `.external_id, ''), ` +
		alias + `.email, ` + alias + `.display_name, ` + alias + `.created_at`
}

// ByEmail returns the person with an email, compared case-insensitively. Two people with the
// email are no answer: an authenticator's email must name one.
func (s *Store) ByEmail(ctx context.Context, email string) (Principal, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE kind = 'human' AND email <> '' AND lower(email) = lower($1) LIMIT 2`, email)
	if err != nil {
		return Principal{}, err
	}
	defer rows.Close()
	var found []Principal
	for rows.Next() {
		p, err := scanPrincipal(rows)
		if err != nil {
			return Principal{}, err
		}
		found = append(found, p)
	}
	if err := rows.Err(); err != nil {
		return Principal{}, err
	}
	if len(found) != 1 {
		return Principal{}, ErrNotFound
	}
	return found[0], nil
}

// ByExternalID returns the person the system of record knows by an ID.
func (s *Store) ByExternalID(ctx context.Context, id string) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE external_id = $1`, id))
}

// Pool is the store's database, for what keeps its own tables in it: locks.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// EnsureBinding binds a robot to b unless it is bound already; b bound to another robot is an
// error.
func (s *Store) EnsureBinding(ctx context.Context, robot string, b Binding) error {
	p, err := s.BoundRobot(ctx, b)
	switch {
	case err == nil && p.Name == robot:
		return nil
	case err == nil:
		return fmt.Errorf("issuer %s's subject %s is bound to %s, not %s", b.Issuer, b.Subject, p.Name, robot)
	case !errors.Is(err, ErrNotFound):
		return err
	}
	return s.Bind(ctx, robot, b)
}
