// Package spannerstore is ok control's store (internal/control/db) on Spanner, in a database of
// its own (docs/hosted.md, "Spanner"). Every write is DML, so a transaction reads what it wrote, as
// on PostgreSQL; an operation outside a transaction is a transaction of its own (spannerdata.Ops).
// A stack is walked a level at a time, in one transaction, where PostgreSQL recurses.
package spannerstore

import (
	"context"
	"embed"
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok control's schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Store is ok control's store on Spanner.
type Store struct {
	queries
	moves *move.SpannerRows
}

var _ db.Store = (*Store)(nil)

// Open brings the database client reaches to ok control's schema, through admin, and returns its
// store.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Store, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Store{queries: queries{spannerdata.Ops{Client: client, Gate: move.SpannerGate}},
		moves: &move.SpannerRows{Client: client, Tables: moveTables, Where: moveWhere, Renames: moveRenames}}, nil
}

func (s *Store) Txn(ctx context.Context, fn func(db.Ops) error) error {
	return spannerdata.Txn(ctx, s.Client, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := move.SpannerGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{spannerdata.Ops{Client: s.Client, Tx: tx}})
	})
}

// moveTables are the tables of rows about repositories this cell holds, which a move copies
// (docs/hosted.md, "Cells, as built"): their CLs, their outboxes, their webhooks and deliveries, and
// where the SCM's feed of each was applied to.
// Not the repository table, nor favorites: every cell's ok control lists every repository, by the
// directory's events, for anyone's favorites.
var moveTables = []string{"changelist", "reviewer", "label_grant", "comment", "event", "outbox", "webhook", "webhook_delivery", "follow_cursor"}

// moveRenames are the columns this schema names otherwise than PostgreSQL's.
var moveRenames = spannerdata.Renames{
	"comment": {"id": "comment_id"},
	"event":   {"id": "event_id", "at": "event_at"},
}

// moveWhere is the condition a table's rows about repositories (@repos) meet, by Spanner's names.
func moveWhere(table string) string {
	if table == "follow_cursor" {
		return "topic = '" + scmfeed.Topic + "' AND key IN UNNEST(@repos)"
	}
	return "repository_id IN UNNEST(@repos)"
}

// Moves are the store's rows about repositories, for a move.
func (s *Store) Moves() move.Rows { return s.moves }

// queries are the store's operations, in a transaction or each in one of its own.
type queries struct{ spannerdata.Ops }

func nullString(s string) spanner.NullString { return spanner.NullString{StringVal: s, Valid: s != ""} }

func nullInt(n int64) spanner.NullInt64 { return spanner.NullInt64{Int64: n, Valid: n != 0} }

// ─── CLs ───

const clColumns = `repository_id, number, change_id, author, status, IFNULL(parent_number, 0), current_patchset,
	description, landed_commit, IFNULL(submitted_by, ''), approved, created_at, updated_at, rolls_back`

func scanCL(row *spanner.Row) (db.ChangeList, error) {
	var c db.ChangeList
	var status, patchset int64
	err := row.Columns(&c.Repo, &c.Number, &c.ChangeID, &c.Author, &status, &c.ParentNumber, &patchset,
		&c.Description, &c.LandedCommit, &c.SubmittedBy, &c.Approved, &c.CreatedAt, &c.UpdatedAt, &c.RollsBack)
	c.Status, c.CurrentPatchset = db.Status(status), int32(patchset)
	if c.RollsBack == nil {
		c.RollsBack = []int64{}
	}
	return c, err
}

// cls reads the CLs a query selects, in its order.
func cls(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]db.ChangeList, error) {
	var out []db.ChangeList
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		c, err := scanCL(row)
		if err != nil {
			return err
		}
		out = append(out, c)
		return nil
	})
	return out, err
}

func (x queries) readCLs(ctx context.Context, sql string, params map[string]any) ([]db.ChangeList, error) {
	var out []db.ChangeList
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = cls(ctx, r, sql, params)
		return err
	})
	return out, err
}

func (x queries) UpsertChangeList(ctx context.Context, c db.ChangeList) (bool, error) {
	if cur, ok, err := x.GetChangeList(ctx, c.Repo, c.Number); err != nil || ok && cur.Holds(c) {
		return false, err
	}
	rollsBack := c.RollsBack
	if rollsBack == nil {
		rollsBack = []int64{}
	}
	p := map[string]any{"repo": c.Repo, "number": c.Number, "change": c.ChangeID, "author": c.Author, "status": int64(c.Status),
		"parent": nullInt(c.ParentNumber), "patchset": int64(c.CurrentPatchset), "description": c.Description, "landed": c.LandedCommit,
		"submitted": nullString(c.SubmittedBy), "created": c.CreatedAt, "updated": c.UpdatedAt, "rollsBack": rollsBack}
	var wrote bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		wrote = false
		var updated time.Time
		ok, err := spannerdata.First(ctx, tx, `SELECT updated_at FROM changelist WHERE repository_id = @repo AND number = @number`, p, &updated)
		if err != nil {
			return err
		}
		switch {
		case !ok:
			_, err = spannerdata.Update(ctx, tx, `INSERT INTO changelist (repository_id, number, change_id, author, status, parent_number,
					current_patchset, description, landed_commit, submitted_by, created_at, updated_at, rolls_back)
				VALUES (@repo, @number, @change, @author, @status, @parent, @patchset, @description, @landed, @submitted, @created, @updated, @rollsBack)`, p)
		case !updated.After(c.UpdatedAt):
			// The approved projection is left as it is, and so are what the SCM's events never
			// change: the change, its author, when it was made, and what it rolls back.
			_, err = spannerdata.Update(ctx, tx, `UPDATE changelist SET status = @status, parent_number = @parent,
					current_patchset = @patchset, description = @description, landed_commit = @landed, submitted_by = @submitted,
					updated_at = @updated
				WHERE repository_id = @repo AND number = @number`, p)
		default:
			return nil // the projection holds a newer state
		}
		wrote = err == nil
		return err
	})
	return wrote, err
}

func (x queries) RollbacksOf(ctx context.Context, repo, number int64) ([]db.ChangeList, error) {
	return x.readCLs(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND @number IN UNNEST(rolls_back) ORDER BY number`,
		map[string]any{"repo": repo, "number": number})
}

func (x queries) oneCL(ctx context.Context, sql string, repo, number int64) (db.ChangeList, bool, error) {
	out, err := x.readCLs(ctx, sql, map[string]any{"repo": repo, "number": number})
	if err != nil || len(out) == 0 {
		return db.ChangeList{}, false, err
	}
	return out[0], true, nil
}

func (x queries) GetChangeList(ctx context.Context, repo, number int64) (db.ChangeList, bool, error) {
	return x.oneCL(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND number = @number`, repo, number)
}

func (x queries) LockChangeList(ctx context.Context, repo, number int64) (db.ChangeList, bool, error) {
	var c db.ChangeList
	var ok bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		out, err := cls(ctx, tx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND number = @number FOR UPDATE`,
			map[string]any{"repo": repo, "number": number})
		ok = len(out) > 0
		if ok {
			c = out[0]
		}
		return err
	})
	return c, ok, err
}

func (x queries) ListChangeLists(ctx context.Context, repo int64, f db.Filter, viewer string, limit int) ([]db.ChangeList, error) {
	where := `repository_id = @repo`
	switch f {
	case db.FilterOpen:
		where += ` AND (status = 2 OR (status = 1 AND author = @viewer))`
	case db.FilterDrafts:
		where += ` AND status = 1`
	case db.FilterNeedsReview:
		where += ` AND status = 2 AND EXISTS (SELECT 1 FROM reviewer r WHERE r.repository_id = changelist.repository_id
			AND r.number = changelist.number AND r.principal = @viewer)
			AND NOT EXISTS (SELECT 1 FROM label_grant g WHERE g.repository_id = changelist.repository_id
			AND g.number = changelist.number AND g.principal = @viewer AND g.granted)`
	case db.FilterMerged:
		where += ` AND status = 3`
	case db.FilterAbandoned:
		where += ` AND status = 4`
	}
	return x.readCLs(ctx, `SELECT `+clColumns+` FROM changelist WHERE `+where+` ORDER BY updated_at DESC, number DESC LIMIT @limit`,
		map[string]any{"repo": repo, "viewer": viewer, "limit": int64(limit)})
}

func (x queries) ActiveChangeLists(ctx context.Context, repo int64) ([]db.ChangeList, error) {
	return x.readCLs(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND status IN (1, 2) ORDER BY number`,
		map[string]any{"repo": repo})
}

// Stack walks the CL's ancestors a parent at a time and its descendants a level at a time, at one
// moment; a cycle the SCM's stacks never have ends the walk where it would repeat.
func (x queries) Stack(ctx context.Context, repo, number int64) ([]db.ChangeList, error) {
	var out []db.ChangeList
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		seen := map[int64]bool{}
		var anc []db.ChangeList
		for n := number; n != 0 && !seen[n]; {
			seen[n] = true
			got, err := cls(ctx, r, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND number = @number`,
				map[string]any{"repo": repo, "number": n})
			if err != nil {
				return err
			}
			if len(got) == 0 {
				break
			}
			anc = append(anc, got[0])
			n = got[0].ParentNumber
		}
		slices.Reverse(anc)
		out = anc
		for level := []int64{number}; len(level) > 0; {
			got, err := cls(ctx, r, `SELECT `+clColumns+` FROM changelist WHERE repository_id = @repo AND parent_number IN UNNEST(@parents)
				ORDER BY number`, map[string]any{"repo": repo, "parents": level})
			if err != nil {
				return err
			}
			level = nil
			for _, c := range got {
				if seen[c.Number] {
					continue
				}
				seen[c.Number] = true
				out = append(out, c)
				level = append(level, c.Number)
			}
		}
		return nil
	})
	return out, err
}

func (x queries) LandedBy(ctx context.Context, repo int64, commits [][]byte) (map[string]int64, error) {
	out := map[string]int64{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT landed_commit, number FROM changelist WHERE repository_id = @repo AND landed_commit IN UNNEST(@commits)`,
			map[string]any{"repo": repo, "commits": commits}, func(row *spanner.Row) error {
				var c []byte
				var n int64
				if err := row.Columns(&c, &n); err != nil {
					return err
				}
				out[string(c)] = n
				return nil
			})
	})
	return out, err
}

func (x queries) SetApproved(ctx context.Context, repo, number int64, approved bool) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE changelist SET approved = @approved WHERE repository_id = @repo AND number = @number AND approved != @approved`,
		map[string]any{"repo": repo, "number": number, "approved": approved})
	return n > 0, err
}

// ─── The feed ───

func (x queries) FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error) {
	var out map[int64]int64
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = spannerdata.Cursors(ctx, r, topic, keys)
		return err
	})
	return out, err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	var seq int64
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		seq, err = spannerdata.Cursor(ctx, r, topic, key)
		return err
	})
	return seq, err
}

func (x queries) LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	var seq int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		seq, err = spannerdata.LockCursor(ctx, tx, topic, key)
		return err
	})
	return seq, err
}

func (x queries) SetFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.SetCursor(ctx, tx, topic, key, seq) })
}

// ─── Repositories ───

// repositoryName reads, in tx, a repository's name: ok false when it is not recorded.
func repositoryName(ctx context.Context, tx *spanner.ReadWriteTransaction, id int64) (spanner.NullString, bool, error) {
	var name spanner.NullString
	ok, err := spannerdata.First(ctx, tx, `SELECT name FROM repository WHERE repository_id = @id`, map[string]any{"id": id}, &name)
	return name, ok, err
}

func (x queries) CreateRepository(ctx context.Context, r db.Repository) error {
	p := map[string]any{"id": r.ID, "name": nullString(r.Name), "by": r.CreatedBy, "created": r.CreatedAt, "head": r.HeadSeq}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		name, ok, err := repositoryName(ctx, tx, r.ID)
		switch {
		case err != nil:
			return err
		case !ok:
			_, err = spannerdata.Update(ctx, tx, `INSERT INTO repository (repository_id, name, created_by, created_at, head_seq, updated_at)
				VALUES (@id, @name, @by, @created, @head, @created)`, p)
		case !name.Valid && r.Name != "":
			_, err = spannerdata.Update(ctx, tx, `UPDATE repository SET name = @name WHERE repository_id = @id`, p)
		}
		return err
	})
}

func (x queries) NameRepository(ctx context.Context, r db.Repository) error {
	p := map[string]any{"id": r.ID, "name": r.Name, "deleted": r.Deleted, "by": r.CreatedBy, "created": r.CreatedAt}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, ok, err := repositoryName(ctx, tx, r.ID)
		if err != nil {
			return err
		}
		sql := `INSERT INTO repository (repository_id, name, deleted, created_by, created_at, updated_at)
			VALUES (@id, @name, @deleted, @by, @created, @created)`
		if ok {
			sql = `UPDATE repository SET name = @name, deleted = @deleted WHERE repository_id = @id`
		}
		_, err = spannerdata.Update(ctx, tx, sql, p)
		return err
	})
}

// DropRepository deletes a repository's rows by partitioned DML, each statement apart from any
// transaction: its CLs and all review keeps of them are too many rows for one transaction's
// mutations, and a purged repository is written no more, so a statement run again deletes nothing
// more. A CL's reviewers, labels, comments, and events are interleaved in it, and go with it.
func (x queries) DropRepository(ctx context.Context, id int64) error {
	for _, sql := range []string{`DELETE FROM changelist WHERE repository_id = @id`, `DELETE FROM favorite WHERE repository_id = @id`,
		`DELETE FROM repository WHERE repository_id = @id`} {
		if _, err := x.Client.PartitionedUpdate(ctx, spanner.Statement{SQL: sql, Params: map[string]any{"id": id}}); err != nil {
			return fmt.Errorf("dropping repository %d: %w", id, err)
		}
	}
	return nil
}

func (x queries) MoveHead(ctx context.Context, id, headSeq int64, at time.Time) error {
	_, err := x.Exec(ctx, `UPDATE repository SET head_seq = @seq, updated_at = @at WHERE repository_id = @id AND head_seq < @seq`,
		map[string]any{"id": id, "seq": headSeq, "at": at})
	return err
}

func (x queries) RepositoryByName(ctx context.Context, name string) (db.Repository, bool, error) {
	var r db.Repository
	var ok bool
	err := x.Read(func(rd spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, rd, `SELECT repository_id, name, deleted, created_by, created_at, head_seq, updated_at FROM repository
			WHERE name = @name ORDER BY deleted, repository_id LIMIT 1`, map[string]any{"name": name},
			&r.ID, &r.Name, &r.Deleted, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt)
		return err
	})
	return r, ok, err
}

func (x queries) RepositoriesByID(ctx context.Context, principal string, ids []int64) ([]db.Repository, error) {
	if ids == nil {
		ids = []int64{}
	}
	var out []db.Repository
	err := x.Read(func(rd spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, rd, `SELECT r.repository_id, IFNULL(r.name, ''), r.created_by, r.created_at, r.head_seq, r.updated_at,
				f.principal IS NOT NULL
			FROM repository r LEFT JOIN favorite f ON f.repository_id = r.repository_id AND f.principal = @principal
			WHERE r.repository_id IN UNNEST(@ids) AND NOT r.deleted`, map[string]any{"principal": principal, "ids": ids},
			func(row *spanner.Row) error {
				var r db.Repository
				if err := row.Columns(&r.ID, &r.Name, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt, &r.Favorite); err != nil {
					return err
				}
				out = append(out, r)
				return nil
			})
	})
	return out, err
}

func (x queries) ListRepositories(ctx context.Context, principal, keyword string, offset, limit int) ([]db.Repository, error) {
	// GoogleSQL's LIKE escapes with a backslash, as PostgreSQL's does.
	pattern := "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.ToLower(keyword)) + "%"
	var out []db.Repository
	err := x.Read(func(rd spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, rd, `SELECT r.repository_id, r.name, r.created_by, r.created_at, r.head_seq, r.updated_at, f.principal IS NOT NULL
			FROM repository r LEFT JOIN favorite f ON f.repository_id = r.repository_id AND f.principal = @principal
			WHERE r.name LIKE @pattern AND NOT r.deleted
			ORDER BY f.principal IS NOT NULL DESC, r.updated_at DESC, r.repository_id
			LIMIT @limit OFFSET @offset`, map[string]any{"principal": principal, "pattern": pattern, "offset": int64(offset), "limit": int64(limit)},
			func(row *spanner.Row) error {
				var r db.Repository
				if err := row.Columns(&r.ID, &r.Name, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt, &r.Favorite); err != nil {
					return err
				}
				out = append(out, r)
				return nil
			})
	})
	return out, err
}

// ─── Review ───

func (x queries) Reviewers(ctx context.Context, repo, number int64) ([]db.Reviewer, error) {
	var out []db.Reviewer
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT principal, added_by, added_at FROM reviewer WHERE repository_id = @repo AND number = @number
			ORDER BY added_at, principal`, map[string]any{"repo": repo, "number": number}, func(row *spanner.Row) error {
			var rv db.Reviewer
			if err := row.Columns(&rv.Principal, &rv.AddedBy, &rv.AddedAt); err != nil {
				return err
			}
			out = append(out, rv)
			return nil
		})
	})
	return out, err
}

func (x queries) AddReviewer(ctx context.Context, repo, number int64, principal, by string) (bool, error) {
	p := map[string]any{"repo": repo, "number": number, "principal": principal, "by": by}
	var added bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		added = false
		var one int64
		ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM reviewer WHERE repository_id = @repo AND number = @number AND principal = @principal`, p, &one)
		if err != nil || ok {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO reviewer (repository_id, number, principal, added_by, added_at)
			VALUES (@repo, @number, @principal, @by, CURRENT_TIMESTAMP())`, p)
		added = err == nil
		return err
	})
	return added, err
}

func (x queries) RemoveReviewer(ctx context.Context, repo, number int64, principal string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM reviewer WHERE repository_id = @repo AND number = @number AND principal = @principal`,
		map[string]any{"repo": repo, "number": number, "principal": principal})
	return n > 0, err
}

func (x queries) Grants(ctx context.Context, repo, number int64) ([]db.Grant, error) {
	return x.GrantsOf(ctx, repo, []int64{number})
}

func (x queries) GrantsOf(ctx context.Context, repo int64, numbers []int64) ([]db.Grant, error) {
	var out []db.Grant
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT number, principal, label, granted, patchset, updated_at FROM label_grant
			WHERE repository_id = @repo AND number IN UNNEST(@numbers) ORDER BY number, principal, label`,
			map[string]any{"repo": repo, "numbers": numbers}, func(row *spanner.Row) error {
				var g db.Grant
				var label, patchset int64
				if err := row.Columns(&g.Number, &g.Principal, &label, &g.Granted, &patchset, &g.UpdatedAt); err != nil {
					return err
				}
				g.Label, g.Patchset = db.Label(label), int32(patchset)
				out = append(out, g)
				return nil
			})
	})
	return out, err
}

func (x queries) SetGrant(ctx context.Context, repo, number int64, principal string, label db.Label, granted bool, patchset int32) (bool, error) {
	p := map[string]any{"repo": repo, "number": number, "principal": principal, "label": int64(label), "patchset": int64(patchset)}
	if !granted {
		n, err := x.Exec(ctx, `UPDATE label_grant SET granted = FALSE, patchset = @patchset, updated_at = CURRENT_TIMESTAMP()
			WHERE repository_id = @repo AND number = @number AND principal = @principal AND label = @label AND granted`, p)
		return n > 0, err
	}
	var changed bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		changed = false
		var was bool
		ok, err := spannerdata.First(ctx, tx, `SELECT granted FROM label_grant
			WHERE repository_id = @repo AND number = @number AND principal = @principal AND label = @label`, p, &was)
		switch {
		case err != nil || was:
			return err
		case ok:
			_, err = spannerdata.Update(ctx, tx, `UPDATE label_grant SET granted = TRUE, patchset = @patchset, updated_at = CURRENT_TIMESTAMP()
				WHERE repository_id = @repo AND number = @number AND principal = @principal AND label = @label`, p)
		default:
			_, err = spannerdata.Update(ctx, tx, `INSERT INTO label_grant (repository_id, number, principal, label, granted, patchset, updated_at)
				VALUES (@repo, @number, @principal, @label, TRUE, @patchset, CURRENT_TIMESTAMP())`, p)
		}
		changed = err == nil
		return err
	})
	return changed, err
}

const commentColumns = `comment_id, number, patchset, author, IFNULL(parent_id, 0), IFNULL(file_path, ''), IFNULL(line, 0),
	IFNULL(side, 0), body, resolved, created_at, updated_at`

func scanComment(row *spanner.Row) (db.Comment, error) {
	var c db.Comment
	var patchset, line, side int64
	err := row.Columns(&c.ID, &c.Number, &patchset, &c.Author, &c.ParentID, &c.FilePath, &line, &side, &c.Body, &c.Resolved, &c.CreatedAt, &c.UpdatedAt)
	c.Patchset, c.Line, c.Side = int32(patchset), int32(line), db.Side(side)
	return c, err
}

func (x queries) comments(ctx context.Context, sql string, params map[string]any) ([]db.Comment, error) {
	var out []db.Comment
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
			c, err := scanComment(row)
			if err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	})
	return out, err
}

func (x queries) Comments(ctx context.Context, repo, number int64) ([]db.Comment, error) {
	return x.comments(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = @repo AND number = @number ORDER BY created_at, comment_id`,
		map[string]any{"repo": repo, "number": number})
}

func (x queries) GetComment(ctx context.Context, repo, number, id int64) (db.Comment, bool, error) {
	out, err := x.comments(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = @repo AND number = @number AND comment_id = @id`,
		map[string]any{"repo": repo, "number": number, "id": id})
	if err != nil || len(out) == 0 {
		return db.Comment{}, false, err
	}
	return out[0], true, nil
}

func (x queries) CreateComment(ctx context.Context, repo int64, c db.Comment) (int64, error) {
	var line, side spanner.NullInt64
	if c.Line != 0 {
		line = spanner.NullInt64{Int64: int64(c.Line), Valid: true}
		side = spanner.NullInt64{Int64: int64(c.Side), Valid: true}
	} else if c.Side != db.SideNone && c.FilePath != "" {
		side = spanner.NullInt64{Int64: int64(c.Side), Valid: true}
	}
	id := data.NewID()
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO comment (repository_id, number, comment_id, patchset, author, parent_id, file_path, line,
				side, body, created_at, updated_at)
			VALUES (@repo, @number, @id, @patchset, @author, @parent, @file, @line, @side, @body, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			map[string]any{"repo": repo, "number": c.Number, "id": id, "patchset": int64(c.Patchset), "author": c.Author, "parent": nullInt(c.ParentID),
				"file": nullString(c.FilePath), "line": line, "side": side, "body": c.Body}); err != nil {
			return err
		}
		return queueOutbox(ctx, tx, repo, c.Number, 0, id)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (x queries) SetResolved(ctx context.Context, repo, number, rootID int64, resolved bool) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE comment SET resolved = @resolved, updated_at = CURRENT_TIMESTAMP()
		WHERE repository_id = @repo AND number = @number AND comment_id = @id AND parent_id IS NULL AND resolved != @resolved`,
		map[string]any{"repo": repo, "number": number, "id": rootID, "resolved": resolved})
	return n > 0, err
}

// DeleteComment deletes a comment and the replies under it, a level at a time.
func (x queries) DeleteComment(ctx context.Context, repo, number, id int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for level := []int64{id}; len(level) > 0; {
			var next []int64
			if err := spannerdata.Each(ctx, tx, `SELECT comment_id FROM comment WHERE repository_id = @repo AND number = @number AND parent_id IN UNNEST(@ids)`,
				map[string]any{"repo": repo, "number": number, "ids": level}, func(row *spanner.Row) error {
					var c int64
					if err := row.Columns(&c); err != nil {
						return err
					}
					next = append(next, c)
					return nil
				}); err != nil {
				return err
			}
			if _, err := spannerdata.Update(ctx, tx, `DELETE FROM comment WHERE repository_id = @repo AND number = @number AND comment_id IN UNNEST(@ids)`,
				map[string]any{"repo": repo, "number": number, "ids": level}); err != nil {
				return err
			}
			level = next
		}
		return nil
	})
}

func (x queries) UnresolvedThreads(ctx context.Context, repo, number int64) (int64, error) {
	var n int64
	err := x.Read(func(r spannerdata.Reader) error {
		_, err := spannerdata.First(ctx, r, `SELECT COUNT(*) FROM comment WHERE repository_id = @repo AND number = @number
			AND parent_id IS NULL AND file_path IS NOT NULL AND NOT resolved`, map[string]any{"repo": repo, "number": number}, &n)
		return err
	})
	return n, err
}

func (x queries) RecordEvent(ctx context.Context, repo, number int64, e db.Event) error {
	at := e.At
	if at.IsZero() {
		at = time.Now()
	}
	id := data.NewID()
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO event (repository_id, number, event_id, event_at, recorded_at, actor, kind, from_status,
				to_status, subject, label, patchset, commit_id, message, related)
			VALUES (@repo, @number, @id, @at, CURRENT_TIMESTAMP(), @actor, @kind, @from, @to, @subject, @label, @patchset, @commit, @message, @related)`,
			map[string]any{"repo": repo, "number": number, "id": id, "at": at, "actor": e.Actor, "kind": int64(e.Kind), "from": int64(e.From),
				"to": int64(e.To), "subject": e.Subject, "label": int64(e.Label), "patchset": int64(e.Patchset), "commit": e.Commit, "message": e.Message,
				"related": e.Related}); err != nil {
			return err
		}
		return queueOutbox(ctx, tx, repo, number, id, 0)
	})
}

func (x queries) Events(ctx context.Context, repo, number int64) ([]db.Event, error) {
	var out []db.Event
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT event_id, event_at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message, related
			FROM event WHERE repository_id = @repo AND number = @number ORDER BY event_at, recorded_at, event_id`,
			map[string]any{"repo": repo, "number": number}, func(row *spanner.Row) error {
				var e db.Event
				var kind, from, to, label, patchset int64
				if err := row.Columns(&e.ID, &e.At, &e.Actor, &kind, &from, &to, &e.Subject, &label, &patchset, &e.Commit, &e.Message, &e.Related); err != nil {
					return err
				}
				e.Kind, e.From, e.To, e.Label, e.Patchset = db.EventKind(kind), db.Status(from), db.Status(to), db.Label(label), int32(patchset)
				out = append(out, e)
				return nil
			})
	})
	return out, err
}

func (x queries) LastAbandon(ctx context.Context, repo, number int64) (db.Event, bool, error) {
	var e db.Event
	var from int64
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT event_id, from_status FROM event WHERE repository_id = @repo AND number = @number AND kind = @kind
			ORDER BY event_at DESC, recorded_at DESC, event_id DESC LIMIT 1`,
			map[string]any{"repo": repo, "number": number, "kind": int64(db.EventAbandoned)}, &e.ID, &from)
		return err
	})
	e.From = db.Status(from)
	return e, ok, err
}

func (x queries) Favorites(ctx context.Context, principal string) (map[int64]bool, error) {
	out := map[int64]bool{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT repository_id FROM favorite WHERE principal = @principal`, map[string]any{"principal": principal},
			func(row *spanner.Row) error {
				var id int64
				if err := row.Columns(&id); err != nil {
					return err
				}
				out[id] = true
				return nil
			})
	})
	return out, err
}

func (x queries) SetFavorite(ctx context.Context, principal string, repo int64, on bool) error {
	p := map[string]any{"principal": principal, "repo": repo}
	if !on {
		_, err := x.Exec(ctx, `DELETE FROM favorite WHERE principal = @principal AND repository_id = @repo`, p)
		return err
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var one int64
		ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM favorite WHERE principal = @principal AND repository_id = @repo`, p, &one)
		if err != nil || ok {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO favorite (principal, repository_id, added_at) VALUES (@principal, @repo, CURRENT_TIMESTAMP())`, p)
		return err
	})
}
