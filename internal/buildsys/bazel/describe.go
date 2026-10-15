package bazel

import (
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/buildsys"
)

// Describe says what each target is at the commit whose graph g is — a rule's class, and whether
// it is a test or a binary — and names each target that does not exist there.
func Describe(g *Graph, targets []string) *buildsys.Described {
	out := &buildsys.Described{Targets: []buildsys.Description{}, Problems: []string{}}
	for _, t := range slices.Sorted(slices.Values(targets)) {
		nd := g.nodes[t]
		if nd == nil {
			out.Problems = append(out.Problems, "target "+t+" does not exist at the change")
			continue
		}
		d := buildsys.Description{Label: t, Kind: nd.kind}
		if nd.kind == "rule" {
			d.Kind = nd.class
			d.Test = isTest(nd.class)
			d.Binary = strings.HasSuffix(nd.class, "_binary")
		}
		out.Targets = append(out.Targets, d)
	}
	return out
}
