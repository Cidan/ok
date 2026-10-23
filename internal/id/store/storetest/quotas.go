package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
)

// An account's quota of CI: the default until an administrator sets one, and again once reset;
// and its use in the cells but one, summed from each cell's reports — running runs only from
// reports of the last two minutes.
func testQuotas(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	s.SetClock(func() time.Time { return now })
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}})
	ada, err := s.Principal(ctx, "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	bea, err := s.Principal(ctx, "bea.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	s.SetDefaultQuota(store.Quota{Concurrent: 10, Minutes: 600})
	got, err := s.QuotasOf(ctx, []string{ada.ID, bea.ID})
	if err != nil || got[ada.ID] != (store.Quota{Concurrent: 10, Minutes: 600, Weight: 1, Default: true}) {
		t.Fatalf("ada's default quota: %+v, %v", got[ada.ID], err)
	}
	if err := s.SetQuota(ctx, ada.ID, store.Quota{Concurrent: -1}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a negative quota: %v", err)
	}
	if err := s.SetQuota(ctx, ada.ID, store.Quota{Concurrent: 50, Minutes: 0, Weight: 3}); err != nil {
		t.Fatal(err)
	}
	got, err = s.QuotasOf(ctx, []string{ada.ID, bea.ID})
	if err != nil || got[ada.ID] != (store.Quota{Concurrent: 50, Weight: 3}) || !got[bea.ID].Default {
		t.Errorf("the quotas once ada's is set: %+v, %v", got, err)
	}
	if err := s.ResetQuota(ctx, ada.ID); err != nil {
		t.Fatal(err)
	}
	if got, err = s.QuotasOf(ctx, []string{ada.ID}); err != nil || !got[ada.ID].Default {
		t.Errorf("ada's quota once reset: %+v, %v", got[ada.ID], err)
	}

	if err := s.ReportUsage(ctx, "c1", "nonsense", nil); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a report of no month: %v", err)
	}
	month := "2027-01"
	for cell, u := range map[string][]store.Usage{
		"c1": {{Account: ada.ID, Seconds: 100, Running: 2}, {Account: bea.ID, Seconds: 5}},
		"c2": {{Account: ada.ID, Seconds: 40, Running: 3}},
		"c3": {{Account: ada.ID, Seconds: 7, Running: 1}},
	} {
		if err := s.ReportUsage(ctx, cell, month, u); err != nil {
			t.Fatal(err)
		}
	}
	// c3's report grows stale: its seconds count still, its running runs no more.
	now = now.Add(3 * time.Minute)
	for _, cell := range []string{"c1", "c2"} {
		var u []store.Usage
		if cell == "c1" {
			u = []store.Usage{{Account: ada.ID, Seconds: 120, Running: 2}, {Account: bea.ID, Seconds: 5}}
		} else {
			u = []store.Usage{{Account: ada.ID, Seconds: 40, Running: 3}}
		}
		if err := s.ReportUsage(ctx, cell, month, u); err != nil {
			t.Fatal(err)
		}
	}
	elsewhere, err := s.UsageElsewhere(ctx, []string{ada.ID, bea.ID}, "c2", month)
	if err != nil {
		t.Fatal(err)
	}
	if u := elsewhere[ada.ID]; u.Seconds != 127 || u.Running != 2 {
		t.Errorf("ada's use elsewhere than c2: %+v, want 127 seconds and 2 running", u)
	}
	if u := elsewhere[bea.ID]; u.Seconds != 5 || u.Running != 0 {
		t.Errorf("bea's use elsewhere than c2: %+v", u)
	}
	if u, err := s.UsageElsewhere(ctx, []string{ada.ID}, "c1", "2027-02"); err != nil || len(u) != 0 {
		t.Errorf("another month's use: %+v, %v", u, err)
	}
}
