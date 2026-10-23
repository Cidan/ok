package store

import (
	"slices"
	"testing"
	"time"
)

// Admission orders accounts by running runs in the pool for their weight, then by their head's
// priority and age; an account at its concurrent runs or its month's seconds — here and elsewhere
// together — is not admitted.
func TestAdmit(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	head := func(p int16, age int) Head {
		return Head{Priority: p, Oldest: t0.Add(time.Duration(age) * time.Second)}
	}
	got := func(cs []Candidate) []string {
		var out []string
		for _, c := range Admit(cs) {
			out = append(out, c.Account)
		}
		return out
	}
	for _, tc := range []struct {
		name string
		cs   []Candidate
		want []string
	}{
		{"fewest running first", []Candidate{
			{Account: "a", Head: head(0, 0), Running: Running{InPool: 3, Total: 3}},
			{Account: "b", Head: head(0, 9), Running: Running{InPool: 1, Total: 1}},
		}, []string{"b", "a"}},
		{"for its weight", []Candidate{
			{Account: "a", Head: head(0, 0), Running: Running{InPool: 3, Total: 3}, Quota: Quota{Weight: 4}},
			{Account: "b", Head: head(0, 9), Running: Running{InPool: 1, Total: 1}},
		}, []string{"a", "b"}},
		{"ties to the higher priority, then the older", []Candidate{
			{Account: "a", Head: head(0, 0)},
			{Account: "b", Head: head(2, 9)},
			{Account: "c", Head: head(2, 5)},
		}, []string{"c", "b", "a"}},
		{"at its concurrent runs, with elsewhere's", []Candidate{
			{Account: "a", Head: head(0, 0), Running: Running{Total: 2}, Quota: Quota{Concurrent: 3, RunningElsewhere: 1}},
			{Account: "b", Head: head(0, 9), Running: Running{Total: 2}, Quota: Quota{Concurrent: 3}},
		}, []string{"b"}},
		{"at its month's seconds, with elsewhere's", []Candidate{
			{Account: "a", Head: head(0, 0), Seconds: 50, Quota: Quota{Seconds: 100, SecondsElsewhere: 50}},
			{Account: "b", Head: head(0, 9), Seconds: 99, Quota: Quota{Seconds: 100}},
		}, []string{"b"}},
		{"running in other pools counts against its quota, not its share", []Candidate{
			{Account: "a", Head: head(0, 0), Running: Running{InPool: 0, Total: 5}, Quota: Quota{Concurrent: 6}},
			{Account: "b", Head: head(0, 9), Running: Running{InPool: 1, Total: 1}},
		}, []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if g := got(tc.cs); !slices.Equal(g, tc.want) {
				t.Errorf("Admit = %v, want %v", g, tc.want)
			}
		})
	}
	if MonthOf(time.Date(2026, 10, 31, 23, 30, 0, 0, time.FixedZone("x", -3600))) != "2026-11" {
		t.Error("a month is UTC's")
	}
}
