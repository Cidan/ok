// Package spannerdb opens a role's Spanner database, as pgdb opens its PostgreSQL schema: a
// database of its own in the configuration's instance (config.SpannerDatabase), made if it is not
// there, the role's backend bringing it to its schema.
package spannerdb

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/data/spannerconn"
)

// DB is a role's database: its client, and the admin client its migrations change its schema
// with.
type DB struct {
	Client *spanner.Client
	Admin  *database.DatabaseAdminClient
}

// Close closes both clients.
func (d *DB) Close() {
	d.Client.Close()
	d.Admin.Close()
}

// Connection is how the configuration reaches its instance.
func Connection(c *config.SpannerDatabase) (spannerconn.Connection, error) {
	o := c.Omni
	if o == nil {
		return spannerconn.Connection{Channels: c.Channels}, nil
	}
	conn := spannerconn.Connection{Omni: o.Address, PlainText: o.PlainText, CAFile: o.CAFile, Username: o.Username, Channels: c.Channels}
	if o.PasswordFile != "" {
		pw, err := config.Secret(o.PasswordFile)
		if err != nil {
			return conn, fmt.Errorf("database.spanner.omni.password_file: %w", err)
		}
		conn.Password = []byte(pw)
	}
	return conn, nil
}

// Open opens role's database in c's instance, making it if it is not there.
func Open(ctx context.Context, c *config.SpannerDatabase, role string) (*DB, error) {
	conn, err := Connection(c)
	if err != nil {
		return nil, err
	}
	name := c.DatabaseName(role)
	admin, err := conn.Admin(ctx)
	if err != nil {
		return nil, fmt.Errorf("spanner: the database admin: %w", err)
	}
	if err := ensure(ctx, admin, c.Instance, c.Prefix+"-"+role); err != nil {
		admin.Close()
		return nil, fmt.Errorf("spanner: the database %s: %w", name, err)
	}
	client, err := conn.Client(ctx, name)
	if err != nil {
		admin.Close()
		return nil, fmt.Errorf("spanner: %s: %w", name, err)
	}
	return &DB{Client: client, Admin: admin}, nil
}

// ensure makes the database id in instance if it is not there; replicas starting together make it
// once, the others finding it made.
func ensure(ctx context.Context, admin *database.DatabaseAdminClient, instance, id string) error {
	_, err := admin.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: instance + "/databases/" + id})
	if spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	op, err := admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: instance, CreateStatement: "CREATE DATABASE `" + id + "`"})
	if spanner.ErrCode(err) == codes.AlreadyExists {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	if spanner.ErrCode(err) == codes.AlreadyExists {
		return nil
	}
	return err
}
