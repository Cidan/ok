// Package postgres migrates a PostgreSQL database to ordered SQL files, speaking ok's migrate
// protocol (package migrate): each file NNNN_name.sql at the root of its file system — 0001, 0002,
// … with no gap — is applied once, in order, in a transaction of its own, and recorded in the table
// <name>_migrations, under an advisory lock so two never apply at once.
//
// Each file is one step of the plan. Its hazards are those its statements show — dropping or
// emptying a table, column, sequence, schema, or extension, or deleting rows (DELETES_DATA, which
// the deployer gates); altering a table (ACCESS_EXCLUSIVE_LOCK); changing a column's type
// (TABLE_REWRITE); building an index (INDEX_BUILD); adding a constraint or NOT NULL that existing
// rows must meet (VALIDATION_SCAN, MAY_FAIL_ON_DATA) — and those it declares in its leading
// comments, one per line, for what the statements cannot show (a function that deletes when it
// runs):
//
//	-- hazard: DELETES_DATA the backfill below removes orphaned rows
//
// A file runs in a transaction, so it cannot hold a statement PostgreSQL refuses in one (CREATE
// INDEX CONCURRENTLY).
package postgres

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	imigrate "github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/migrate"
)

// Main is a migration program's main: it migrates the database whose URL the environment variable
// -url-env names (DATABASE_URL unless set), or the file -url-file names, to the files in fsys,
// recorded under -name (the table <name>_migrations; "app" unless set).
func Main(fsys fs.FS) {
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	urlEnv := flags.String("url-env", "DATABASE_URL", "the environment variable holding the database's URL")
	urlFile := flags.String("url-file", "", "a file holding the database's URL, instead of the environment")
	name := flags.String("name", "app", "what the migrations are recorded under: the table <name>_migrations")
	migrate.Main(flags, func(ctx context.Context) (migrate.Migrator, error) {
		url := os.Getenv(*urlEnv)
		if *urlFile != "" {
			data, err := os.ReadFile(*urlFile)
			if err != nil {
				return nil, err
			}
			url = strings.TrimSpace(string(data))
		}
		if url == "" {
			return nil, fmt.Errorf("no database URL: set %s, or name a file with -url-file", *urlEnv)
		}
		return New(ctx, url, fsys, *name)
	})
}

// Migrator migrates one database to its files.
type Migrator struct {
	pool             *pgxpool.Pool
	name             string
	migrations       []imigrate.Migration
	database, server string
}

var validName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// New makes a Migrator of the database at url to the files in fsys, recorded under name.
func New(ctx context.Context, url string, fsys fs.FS, name string) (*Migrator, error) {
	if !validName.MatchString(name) {
		return nil, fmt.Errorf("the migrations' name %q is not lowercase letters, digits, and _", name)
	}
	ms, err := imigrate.Load(fsys)
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("the database URL: %w", err)
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Migrator{pool: pool, name: name, migrations: ms, database: cfg.ConnConfig.Database,
		server: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port)))}, nil
}

// Close closes the Migrator's connections.
func (m *Migrator) Close() { m.pool.Close() }

// Plan is a step for each file the database has not applied.
func (m *Migrator) Plan(ctx context.Context) (migrate.Report, error) {
	r := migrate.Report{Database: m.database, Server: m.server, Steps: []migrate.Step{}}
	pending, err := imigrate.Pending(ctx, m.pool, m.name, m.migrations)
	if err != nil {
		return r, err
	}
	for _, p := range pending {
		r.Steps = append(r.Steps, migrate.Step{Description: fmt.Sprintf("%04d_%s", p.Version, p.Name), SQL: p.SQL, Hazards: Hazards(p.SQL)})
	}
	return r, nil
}

// Apply applies every file the database has not, the planned ones; one applied meanwhile by
// another is not applied again.
func (m *Migrator) Apply(ctx context.Context, plan migrate.Report) error {
	pending, err := imigrate.Pending(ctx, m.pool, m.name, m.migrations)
	if err != nil {
		return err
	}
	planned := map[string]bool{}
	for _, s := range plan.Steps {
		planned[s.Description] = true
	}
	for _, p := range pending {
		if !planned[fmt.Sprintf("%04d_%s", p.Version, p.Name)] {
			return errors.New("the database changed since the plan: plan again")
		}
	}
	_, err = imigrate.Apply(ctx, m.pool, m.name, m.migrations)
	return err
}

// Hazards are the hazards a file's SQL carries: those its statements show, and those its leading
// comments declare.
func Hazards(sql string) []migrate.Hazard {
	var out []migrate.Hazard
	seen := map[string]bool{}
	add := func(class, detail string) {
		if !seen[class+"\x00"+detail] {
			seen[class+"\x00"+detail] = true
			out = append(out, migrate.Hazard{Class: class, Detail: detail})
		}
	}
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		rest, ok := strings.CutPrefix(line, "--")
		if !ok {
			break
		}
		if decl, ok := strings.CutPrefix(strings.TrimSpace(rest), "hazard:"); ok {
			class, detail, _ := strings.Cut(strings.TrimSpace(decl), " ")
			add(class, strings.TrimSpace(detail))
		}
	}
	for _, stmt := range statements(sql) {
		u := strings.ToUpper(stmt)
		switch {
		case dropping.MatchString(u), strings.HasPrefix(u, "TRUNCATE "), strings.HasPrefix(u, "DELETE FROM "):
			add(migrate.DeletesData, stmt)
		case strings.HasPrefix(u, "ALTER TABLE "):
			add(migrate.AccessExclusiveLock, stmt)
			if dropsColumn(u) {
				add(migrate.DeletesData, stmt)
			}
			if changingType.MatchString(u) {
				add(migrate.TableRewrite, stmt)
			}
			if (addingConstraint.MatchString(u) && !strings.Contains(u, " NOT VALID")) || strings.Contains(u, " SET NOT NULL") {
				add(migrate.ValidationScan, stmt)
				add(migrate.MayFailOnData, stmt)
			}
		case indexing.MatchString(u):
			add(migrate.IndexBuild, stmt)
			if strings.HasPrefix(u, "CREATE UNIQUE ") {
				add(migrate.MayFailOnData, stmt)
			}
		}
	}
	return out
}

var (
	dropping         = regexp.MustCompile(`^DROP (TABLE|SEQUENCE|SCHEMA|EXTENSION|MATERIALIZED VIEW|DATABASE|TYPE) `)
	dropClause       = regexp.MustCompile(` DROP (COLUMN )?("[^"]*"|[A-Z_][A-Z0-9_$]*)`)
	changingType     = regexp.MustCompile(` ALTER (COLUMN )?("[^"]*"|[A-Z_][A-Z0-9_$]*) (SET DATA )?TYPE `)
	addingConstraint = regexp.MustCompile(` ADD (CONSTRAINT ("[^"]*"|[A-Z_][A-Z0-9_$]*) )?(CHECK|FOREIGN KEY|UNIQUE|PRIMARY KEY|EXCLUDE)`)
	indexing         = regexp.MustCompile(`^CREATE (UNIQUE )?INDEX `)
	// keepsDrop are what ALTER TABLE … DROP removes that holds no data.
	keepsDrop = regexp.MustCompile(`^(CONSTRAINT|DEFAULT|NOT|IDENTITY|EXPRESSION)$`)
)

// dropsColumn reports whether an ALTER TABLE drops a column: DROP COLUMN, or DROP of a name that
// is not one of what a DROP removes without data (a constraint, a default, NOT NULL, …).
func dropsColumn(u string) bool {
	for _, m := range dropClause.FindAllStringSubmatch(u, -1) {
		if m[1] != "" || !keepsDrop.MatchString(m[2]) {
			return true
		}
	}
	return false
}

// statements are sql's statements with comments, strings, and dollar-quoted bodies blanked and
// whitespace folded: what a statement does, not what its text or a function body holds.
func statements(sql string) []string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		switch {
		case strings.HasPrefix(sql[i:], "--"):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				i = len(sql)
			} else {
				i += end + 4
			}
			b.WriteByte(' ')
		case sql[i] == '\'':
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			b.WriteString("''")
			i = j + 1
		case sql[i] == '$':
			if tag := dollarTag.FindString(sql[i:]); tag != "" {
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					i = len(sql)
				} else {
					i += len(tag) + end + len(tag)
				}
				b.WriteString("''")
				continue
			}
			b.WriteByte(sql[i])
			i++
		default:
			b.WriteByte(sql[i])
			i++
		}
	}
	var out []string
	for _, s := range strings.Split(b.String(), ";") {
		if s = strings.Join(strings.Fields(s), " "); s != "" {
			out = append(out, s)
		}
	}
	return out
}

var dollarTag = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)?\$`)
