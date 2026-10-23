// Package spannerconn says how to reach Spanner — Google's service or a Spanner Omni deployment —
// and opens its clients: a package of its own, importing none of ok's, so the test servers'
// package (internal/testdb) can reach Spanner as the roles do.
package spannerconn

import (
	"context"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
)

// Connection says how to reach Spanner: Google's service, with the environment's credentials (an
// emulator, too, when SPANNER_EMULATOR_HOST names one, as Spanner's own tools have it); or a
// Spanner Omni deployment at an address — the engine Spanner runs, on ok's own machines.
type Connection struct {
	// Omni is a Spanner Omni deployment's address, host:port; "" for Google's service.
	Omni string
	// PlainText reaches Omni without TLS: a single server on loopback.
	PlainText bool
	// CAFile, for Omni over TLS, is the CA its certificate is under; the system's roots when "".
	CAFile string
	// Username and Password sign in to Omni, when its deployment asks for a password.
	Username string
	Password []byte
	// Channels are the gRPC connections a client spreads its calls over, each carrying a bounded
	// number at once: DefaultChannels unless set.
	Channels int
}

// DefaultChannels are a client's connections to Spanner: a replica serving thousands of requests at
// once makes as many calls, which the client's four by default would queue.
const DefaultChannels = 16

func (c Connection) config() spanner.ClientConfig {
	channels := c.Channels
	if channels <= 0 {
		channels = DefaultChannels
	}
	if c.Omni == "" {
		return spanner.ClientConfig{NumChannels: channels}
	}
	return spanner.ClientConfig{Type: spanner.OMNI, UsePlainText: c.PlainText, CaCertificateFile: c.CAFile, Username: c.Username,
		Password: c.Password, NumChannels: channels}
}

func (c Connection) options() []option.ClientOption {
	if c.Omni == "" {
		return nil
	}
	return []option.ClientOption{option.WithEndpoint(c.Omni)}
}

// Client is a client of a database (projects/P/instances/I/databases/D; Omni's instance is
// projects/default/instances/default).
func (c Connection) Client(ctx context.Context, db string) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, db, c.config(), c.options()...)
}

// Admin is a database admin client: what makes databases, and what spannerdata.Migrate changes
// their schemas with.
func (c Connection) Admin(ctx context.Context) (*database.DatabaseAdminClient, error) {
	if c.Omni == "" {
		return database.NewDatabaseAdminClient(ctx)
	}
	return database.NewDatabaseAdminClientWithConfig(ctx, c.config(), c.options()...)
}
