package store

import (
	"sort"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// table is where a referenced object lives.
type table int

const (
	tObject    table = iota // symlinks, trees, copy histories (kind distinguishes)
	tCommit                 // commits
	tFile                   // files (their contents are chunks)
	tChunk                  // chunks
	tOperation              // operations, in the caller's operation log
	tView                   // views, in the caller's operation log
)

// ref is a reference to a stored object. kind matters only for tObject.
type ref struct {
	table table
	kind  encoding.Kind
	id    encoding.ID
}

func (r ref) objectRef() ObjectRef {
	switch r.table {
	case tCommit:
		return ObjectRef{Kind: encoding.KindCommit, ID: r.id}
	case tFile:
		return ObjectRef{Kind: encoding.KindFile, ID: r.id}
	case tChunk:
		return ObjectRef{Kind: encoding.KindChunk, ID: r.id}
	case tOperation:
		return ObjectRef{Kind: encoding.KindOperation, ID: r.id}
	case tView:
		return ObjectRef{Kind: encoding.KindView, ID: r.id}
	}
	return ObjectRef{Kind: r.kind, ID: r.id}
}

// refOf is where an object of kind k with ID id lives.
func refOf(k encoding.Kind, id encoding.ID) ref {
	switch k {
	case encoding.KindCommit:
		return ref{table: tCommit, id: id}
	case encoding.KindFile:
		return ref{table: tFile, id: id}
	case encoding.KindChunk:
		return ref{table: tChunk, id: id}
	case encoding.KindOperation:
		return ref{table: tOperation, id: id}
	case encoding.KindView:
		return ref{table: tView, id: id}
	}
	return ref{table: tObject, kind: k, id: id}
}

// refSet collects references, dropping the virtual objects no store holds: the root
// commit, the empty tree, and the root operation and its view.
type refSet map[ref]bool

func (s refSet) commit(id encoding.ID) {
	if id != encoding.RootCommitID {
		s[ref{table: tCommit, id: id}] = true
	}
}

func (s refSet) tree(id encoding.ID) {
	if id != encoding.EmptyTreeID {
		s[ref{table: tObject, kind: encoding.KindTree, id: id}] = true
	}
}

func (s refSet) add(r ref) { s[r] = true }

func (s refSet) sorted() []ref {
	out := make([]ref, 0, len(s))
	for r := range s {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].table != out[j].table {
			return out[i].table < out[j].table
		}
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return string(out[i].id[:]) < string(out[j].id[:])
	})
	return out
}

func (s refSet) treeEntries(t *encoding.Tree) {
	for _, e := range t.Entries {
		v := e.Value
		switch v.Kind {
		case encoding.TreeValueFile:
			s.add(ref{table: tFile, id: v.ID})
			if v.CopyID != nil {
				s.add(ref{table: tObject, kind: encoding.KindCopyHistory, id: *v.CopyID})
			}
		case encoding.TreeValueSymlink:
			s.add(ref{table: tObject, kind: encoding.KindSymlink, id: v.ID})
		case encoding.TreeValueTree:
			s.tree(v.ID)
		}
	}
}

// closureRefs adds everything o references that a publish must resolve (docs/scm.md,
// "What the server verifies on every write"). A commit's predecessors are the one
// reference exempt: they are recorded but weak.
func closureRefs(s refSet, o encoding.Object) {
	switch o := o.(type) {
	case *encoding.Tree:
		s.treeEntries(o)
	case *encoding.Commit:
		for _, p := range o.Parents {
			s.commit(p)
		}
		for _, t := range o.RootTree {
			s.tree(t)
		}
	case *encoding.CopyHistory:
		for _, p := range o.Parents {
			s.add(ref{table: tObject, kind: encoding.KindCopyHistory, id: p})
		}
	case *encoding.Operation:
		if o.ViewID != encoding.RootViewID {
			s.add(ref{table: tView, id: o.ViewID})
		}
		for _, p := range o.Parents {
			if p != encoding.RootOperationID {
				s.add(ref{table: tOperation, id: p})
			}
		}
		for c, preds := range o.CommitPredecessors {
			s.commit(c)
			for _, p := range preds {
				s.commit(p)
			}
		}
	case *encoding.View:
		for _, c := range viewCommits(o) {
			s.commit(c)
		}
	}
}

// gcRefs adds the references freshness and garbage collection follow: a tree's entries,
// a commit's parents and root tree, a copy history's parents. Operations and views are
// not repository objects and are collected per operation log instead.
func gcRefs(s refSet, o encoding.Object) {
	switch o.(type) {
	case *encoding.Tree, *encoding.Commit, *encoding.CopyHistory:
		closureRefs(s, o)
	}
}

// viewCommits lists every commit a view references: its heads, every ref target's
// terms, and its working-copy commits.
func viewCommits(v *encoding.View) []encoding.ID {
	var out []encoding.ID
	out = append(out, v.HeadIDs...)
	target := func(t encoding.RefTarget) {
		for _, id := range t {
			if id != nil {
				out = append(out, *id)
			}
		}
	}
	for _, t := range v.LocalBookmarks {
		target(t)
	}
	for _, t := range v.LocalTags {
		target(t)
	}
	for _, rv := range v.RemoteViews {
		for _, r := range rv.Bookmarks {
			target(r.Target)
		}
		for _, r := range rv.Tags {
			target(r.Target)
		}
	}
	for _, t := range v.GitRefs {
		target(t)
	}
	for _, t := range v.GitHeads {
		target(t)
	}
	for _, c := range v.WCCommitIDs {
		out = append(out, c)
	}
	return out
}
