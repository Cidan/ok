package review

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

func TestPickCover(t *testing.T) {
	// Users 1..5; rank = recent activity (lower first), names break ties.
	rank := map[string]int{"3": 0, "2": 1}
	order := func(p string) (int, string) {
		r, ok := rank[p]
		if !ok {
			r = len(rank)
		}
		return r, "u" + p
	}
	tests := []struct {
		name     string
		sets     [][]string
		assigned []string
		want     []string
	}{
		{name: "no sets", want: nil},
		{name: "one owner per set", sets: [][]string{{"1"}, {"2"}}, want: []string{"1", "2"}},
		{name: "one person covering every set wins", sets: [][]string{{"1", "4"}, {"2", "4"}, {"4", "5"}}, want: []string{"4"}},
		{name: "ties go to recent activity", sets: [][]string{{"1", "2", "3"}}, want: []string{"3"}},
		{name: "then to name", sets: [][]string{{"5", "4"}}, want: []string{"4"}},
		{name: "an assigned reviewer already covers their sets", sets: [][]string{{"1", "2"}, {"3"}}, assigned: []string{"2"}, want: []string{"3"}},
		{name: "everything covered by assignments picks nobody", sets: [][]string{{"1"}}, assigned: []string{"1"}, want: nil},
		{
			name: "greedy takes the widest cover first, then fills the rest",
			sets: [][]string{{"1", "2"}, {"2", "5"}, {"4"}}, want: []string{"2", "4"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assigned := map[string]bool{}
			for _, id := range tc.assigned {
				assigned[id] = true
			}
			got := slices.Sorted(maps.Keys(pickCover(tc.sets, assigned, order)))
			if !slices.Equal(got, tc.want) {
				t.Errorf("pickCover = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRecentPathspec(t *testing.T) {
	spec, whole := recentPrefixes([]string{"a/b/x.go", "a/b/y.go", "c/z.go"})
	if whole || !slices.Equal(spec, []string{"a/b", "c"}) {
		t.Errorf("got %v whole=%v", spec, whole)
	}
	if _, whole := recentPrefixes([]string{"a/x.go", "README.md"}); !whole {
		t.Error("a change at the repository root reads all of main's history")
	}
	if _, whole := recentPrefixes(nil); !whole {
		t.Error("no changed files reads all of main's history")
	}
	many := make([]string, recentMaxDirs+1)
	for i := range many {
		many[i] = fmt.Sprintf("d%d/f", i)
	}
	if _, whole := recentPrefixes(many); !whole {
		t.Error("too many directories reads all of main's history")
	}
}
