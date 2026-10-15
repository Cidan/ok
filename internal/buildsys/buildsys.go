// Package buildsys is what automation asks of a repository's build system: what a change
// affects, what each target is, and the commands each job is. Its one implementation is Bazel
// (buildsys/bazel); the planner, the workers, the landing queue, and the deployer speak only
// these types, so they never name a build tool.
package buildsys

// Affected is what a change affects: every target whose result can differ, the tests among
// them, what they rest on, and the graph's problems.
type Affected struct {
	// Changed are the paths the change touches.
	Changed []string `json:"changed"`
	// Affected are the targets whose result can differ, by label.
	Affected []Target `json:"affected"`
	// Tests are the affected tests' labels.
	Tests []string `json:"tests"`
	// Upstream are the targets the affected ones depend on, transitively, that are not affected
	// themselves: their results stand.
	Upstream []string `json:"upstream"`
	// Global names the file that made every target affected, when one did (the module file, the
	// build tool's version, the repository's settings, a toolchain).
	Global string `json:"global,omitempty"`
	// Problems are why the graph could not be read at the change, each a sentence.
	Problems []string `json:"problems"`
	// LintTools are the targets the lint job's linters are, on which its findings depend.
	LintTools []string `json:"lint_tools"`
}

// Target is one affected target.
type Target struct {
	Label  string `json:"label"`
	Kind   string `json:"kind"`
	Test   bool   `json:"test"`
	Binary bool   `json:"binary"`
	// Direct targets are affected by the change itself — a file they read, or their own
	// declaration — rather than through a dependency.
	Direct bool `json:"direct"`
	// Root targets are affected and no affected target depends on them: building the roots builds
	// every affected target.
	Root bool `json:"root"`
	// Because is why the target is affected: a chain from it, through what it depends on, to a
	// directly affected target, the last naming the changed file when there is one.
	Because []Step `json:"because"`
}

// Step is one link of the chain that made a target affected.
type Step struct {
	Label string `json:"label"`
	Via   string `json:"via"`
	File  string `json:"file"`
}

// Described is what each named target is, and a problem for each that names nothing.
type Described struct {
	Targets  []Description `json:"targets"`
	Problems []string      `json:"problems"`
}

// Description is what one target is.
type Description struct {
	Label  string `json:"label"`
	Kind   string `json:"kind"`
	Test   bool   `json:"test"`
	Binary bool   `json:"binary"`
}
