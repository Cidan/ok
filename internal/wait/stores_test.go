package wait_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/wait"
)

// The waits stores keep each inbox's registrations until they lapse or it unregisters: Waiting
// finds the inboxes registered under any of the subjects it is asked of, each once, none lapsed.
func TestStores(t *testing.T) {
	for name, open := range map[string]func(t *testing.T) wait.Store{
		"postgres": func(t *testing.T) wait.Store {
			p := testdb.Pool(t)
			if _, err := p.Exec(context.Background(), pgdata.WorkSchema+pgdata.WaitsSchema); err != nil {
				t.Fatal(err)
			}
			return pgdata.Waits(p)
		},
		"spanner": func(t *testing.T) wait.Store {
			return spannerdata.Waits(testdb.SpannerClient(t, testdb.Spanner(t, append([]string{spannerdata.WorkDDL}, spannerdata.WaitsDDL...)...)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			now := time.Now()
			later := now.Add(time.Minute)
			for inbox, subjects := range map[string][]string{"a": {"x", "y"}, "b": {"y"}, "c": {"z"}} {
				if err := s.Register(ctx, inbox, subjects, later); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Register(ctx, "gone", []string{"x"}, now.Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				subjects, want []string
			}{
				{[]string{"x"}, []string{"a"}},
				{[]string{"x", "y"}, []string{"a", "b"}},
				{[]string{"y", "z", "w"}, []string{"a", "b", "c"}},
				{[]string{"w"}, nil},
			} {
				got, err := s.Waiting(ctx, tc.subjects, now)
				slices.Sort(got)
				if err != nil || !slices.Equal(got, tc.want) {
					t.Errorf("Waiting(%v) = %v, %v; want %v", tc.subjects, got, err, tc.want)
				}
			}
			if err := s.Unregister(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Waiting(ctx, []string{"x", "y"}, now); err != nil || !slices.Equal(got, []string{"b"}) {
				t.Errorf("after a unregistered: %v, %v", got, err)
			}
		})
	}
}
