// Package load is ok's load generator (docs/hosted.md, "Proof at scale, as built"): it drives an
// installation through the services people's jj, ok control, and workers call, open-loop, at a
// stated fraction of one cell's share of the target, and reports what each operation took and what
// must hold however the run went.
package load

import (
	"fmt"
	"time"
)

// The target's rates, per account and repository (docs/hosted.md, "The target").
const (
	// PublishesPerDay are an account's jj transactions a day; PublishPeak is the peak over the
	// average (~12,000 a second against ~2,300).
	PublishesPerDay = 200
	PublishPeak     = 12000.0 / 2300.0
	// ActiveAtPeak are the accounts with a workspace live at peak, each checkpointing every
	// CheckpointEvery.
	ActiveAtPeak    = 0.1
	CheckpointEvery = 5 * time.Second
	// CIRunsPerDay are a repository's CI runs a day.
	CIRunsPerDay = 50
	// IngestPerDay are the bytes an account sends a day; a publish carries its share of them.
	IngestPerDay   = 1e9
	PublishBytes   = IngestPerDay / PublishesPerDay
	CheckpointFile = 4 << 10 // a checkpoint's changed file: a workspace's edit, a few KiB
	// CellAccounts are a cell's accounts: a million over a hundred cells.
	CellAccounts = 10_000
)

// Rates are operations a second.
type Rates struct {
	Publishes   float64 `json:"publishes"`
	Checkpoints float64 `json:"checkpoints"`
	CLs         float64 `json:"cls"`
	Browses     float64 `json:"browses"`
}

// Shape is how a run offers load: the repositories it drives, each at the target's rates a
// repository, times Speed — so N repositories at Speed S offer what N×S do — at peak or on average,
// with CLsPerDay a repository (the target names no CL rate).
type Shape struct {
	// Repositories are the units the target's rates are of: repositories, or people (People).
	Repositories int
	People       bool
	Speed        float64
	Peak         bool
	CLsPerDay    float64
	// BrowsesPerDay are a unit's reads of main's listing a day: the target names no rate.
	BrowsesPerDay float64
}

// Rates are the shape's rates.
func (s Shape) Rates() Rates {
	n := float64(s.Repositories) * s.Speed
	day := (24 * time.Hour).Seconds()
	r := Rates{Publishes: n * PublishesPerDay / day, CLs: n * s.CLsPerDay / day,
		Checkpoints: n * ActiveAtPeak / CheckpointEvery.Seconds(), Browses: n * s.BrowsesPerDay / day}
	if s.Peak {
		r.Publishes *= PublishPeak
	}
	return r
}

// CellFraction is the fraction of a cell's share of the target the shape offers.
func (s Shape) CellFraction() float64 {
	return float64(s.Repositories) * s.Speed / CellAccounts
}

func (s Shape) String() string {
	when := "on average"
	if s.Peak {
		when = "at peak"
	}
	unit := "repositories"
	if s.People {
		unit = "people"
	}
	return fmt.Sprintf("%d %s ×%g, %s: %.4g of a cell's share", s.Repositories, unit, s.Speed, when, s.CellFraction())
}
