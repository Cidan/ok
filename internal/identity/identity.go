// Package identity is ok's model of principals' names and of the groups and ACLs: sets of
// principals, declared as a tree of folders in a repository, whose nesting is the ownership tree.
// It names things under an installation's domain (Names), parses and validates a tree (Load, New),
// derives each set's owner, expands membership, diffs two trees (Diff, Check), and decides which
// approvals a change needs (Authorize). The directory applies and serves a tree with it, and review
// approves changes to it, so both agree by construction.
package identity

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Names is an installation's naming: the domain every principal and set is named under —
// alice.human.example.com, eng.group.example.com.
type Names struct {
	domain string
}

// NewNames returns the naming under domain, which is lowercase DNS labels joined by dots.
func NewNames(domain string) (Names, error) {
	if domain == "" || len(domain) > 253 {
		return Names{}, fmt.Errorf("%q is not a domain", domain)
	}
	for _, l := range strings.Split(domain, ".") {
		if !ValidLabel(l) {
			return Names{}, fmt.Errorf("%q is not a domain: %q is not a lowercase DNS label", domain, l)
		}
	}
	return Names{domain: domain}, nil
}

// MustNames is NewNames for a domain known to be valid; it panics on another.
func MustNames(domain string) Names {
	n, err := NewNames(domain)
	if err != nil {
		panic(err)
	}
	return n
}

// Domain is the domain names are under.
func (n Names) Domain() string { return n.domain }

// Root is the name of the tree's root folder: it declares admins.acl.<domain>, the one ACL that
// owns itself, and optionally admins.group.<domain>.
const Root = "admins"

// Kind is the kind of a set.
type Kind int

const (
	// Group is group identity: who is in a team, the way a mailing list says.
	Group Kind = iota + 1
	// ACL is a set for authorization.
	ACL
)

// String is the kind as it appears in names and file names: "group" or "acl".
func (k Kind) String() string {
	switch k {
	case Group:
		return "group"
	case ACL:
		return "acl"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// File is the file in a folder that declares the folder's set of kind k.
func (k Kind) File() string { return k.String() + ".yaml" }

// MemberKind is the kind of anything a set may contain.
type MemberKind int

const (
	// Human is a person: <handle>.human.<domain>.
	Human MemberKind = iota + 1
	// Robot is a workload: <name>.robot.<domain>, or <serviceaccount>.<namespace>.robot.<domain>.
	Robot
	// GroupMember is a group.
	GroupMember
	// ACLMember is an ACL.
	ACLMember
)

func (k MemberKind) String() string {
	switch k {
	case Human:
		return "human"
	case Robot:
		return "robot"
	case GroupMember:
		return "group"
	case ACLMember:
		return "acl"
	}
	return fmt.Sprintf("MemberKind(%d)", int(k))
}

// labelRE is one lowercase DNS label; its length (at most 63) is checked apart.
var labelRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// ValidLabel reports whether s is a lowercase DNS label: what a set's (and its folder's) name is.
func ValidLabel(s string) bool { return len(s) <= 63 && labelRE.MatchString(s) }

// KindOf returns the kind of thing a name names: one label before .human, .group, or .acl, and
// one or two before .robot (a robot is named by itself, or by its Kubernetes ServiceAccount and
// namespace).
func (n Names) KindOf(name string) (MemberKind, bool) {
	rest, ok := strings.CutSuffix(name, "."+n.domain)
	if !ok {
		return 0, false
	}
	i := strings.LastIndexByte(rest, '.')
	if i < 0 {
		return 0, false
	}
	labels := strings.Split(rest[:i], ".")
	for _, l := range labels {
		if !ValidLabel(l) {
			return 0, false
		}
	}
	switch rest[i+1:] {
	case "human":
		return Human, len(labels) == 1
	case "robot":
		return Robot, len(labels) <= 2
	case "group":
		return GroupMember, len(labels) == 1
	case "acl":
		return ACLMember, len(labels) == 1
	}
	return 0, false
}

// Of is the name of the set of kind k named name: eng.acl.example.com.
func (n Names) Of(k Kind, name string) string { return name + "." + k.String() + "." + n.domain }

// OfSet is a set's name.
func (n Names) OfSet(s Set) string { return n.Of(s.Kind, s.Name) }

// Human is the name of the person with handle: alice.human.example.com.
func (n Names) Human(handle string) string { return handle + ".human." + n.domain }

// Robot is the name of the robot named name: ci.robot.example.com, or
// deploy.ok.robot.example.com for a ServiceAccount and its namespace.
func (n Names) Robot(name string) string { return name + ".robot." + n.domain }

// Set is one declared group or ACL.
type Set struct {
	Kind Kind
	// Name is the folder's name: "eng", or Root for the root folder.
	Name string
	// Path is the folder, relative to the tree's root and slash-separated: "eng/deploy", or ""
	// for the root.
	Path string
	// Members are full names, in their declared order.
	Members []string
}

func (s *Set) clone() *Set {
	c := *s
	c.Members = append([]string(nil), s.Members...)
	return &c
}

// Error is everything wrong with a tree or a change, each problem a sentence.
type Error struct {
	Problems []string
}

func (e *Error) Error() string {
	if len(e.Problems) == 1 {
		return e.Problems[0]
	}
	return fmt.Sprintf("%d problems:\n  %s", len(e.Problems), strings.Join(e.Problems, "\n  "))
}

// Tree is a valid tree of sets.
type Tree struct {
	naming Names
	sets   map[string]*Set   // by full name
	names  []string          // the full names, sorted
	owner  map[string]string // full name -> its owner's full name
	// synced are the groups the system of record holds, by full name, with their members: a tree
	// set may hold one, and expanding it goes through it.
	synced map[string][]string
}

// Option adds to what New knows.
type Option func(*Tree)

// WithSynced tells New the groups synced from the system of record, by full name, with their
// members: the tree's sets may hold them, and membership goes through them.
func WithSynced(groups map[string][]string) Option {
	return func(t *Tree) {
		for n, ms := range groups {
			t.synced[n] = append([]string(nil), ms...)
		}
	}
}

// members are a set's members: a tree set's, or a synced group's.
func (t *Tree) members(n string) ([]string, bool) {
	if s, ok := t.sets[n]; ok {
		return s.Members, true
	}
	ms, ok := t.synced[n]
	return ms, ok
}

// New validates sets as a tree named under names: each set's name and folder, the folders (every
// folder declares a set, and a folder is a name), the root (admins.acl.<domain>, with a member),
// and each set's
// members (well-formed names, each once, of kinds the set may contain, the sets among them
// declared, and no set containing itself through others). It derives each set's owner. The
// error is an *Error listing every problem.
func New(names Names, sets []Set, opts ...Option) (*Tree, error) {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	t := &Tree{naming: names, sets: map[string]*Set{}, owner: map[string]string{}, synced: map[string][]string{}}
	for _, o := range opts {
		o(t)
	}
	folders := map[string]bool{}  // every folder that declares a set
	byName := map[string]string{} // a folder's name -> its path
	for i := range sets {
		s := sets[i].clone()
		where := describe(s)
		if s.Kind != Group && s.Kind != ACL {
			add("%s: the kind %d is neither a group nor an ACL", where, int(s.Kind))
			continue
		}
		if !validPath(s.Path) {
			add("%s: %q is not a folder of lowercase DNS labels", where, s.Path)
			continue
		}
		want := Root
		if s.Path != "" {
			want = s.Path[strings.LastIndexByte(s.Path, '/')+1:]
		}
		if s.Name != want {
			add("%s: the set in folder %q is named %q", where, displayPath(s.Path), want)
			continue
		}
		if s.Path != "" && s.Name == Root {
			add("%s: only the tree's root is named %q", where, Root)
			continue
		}
		if p, ok := byName[s.Name]; ok && p != s.Path {
			add("%s: the name %q is also the folder %s; a folder is a name, so names are unique across the tree", where, s.Name, displayPath(p))
			continue
		}
		if _, ok := t.sets[t.naming.OfSet(*s)]; ok {
			add("%s: declared twice", where)
			continue
		}
		if _, ok := t.synced[t.naming.OfSet(*s)]; ok {
			add("%s: %s is a group synced from the system of record; the tree cannot declare it too", where, t.naming.OfSet(*s))
			continue
		}
		byName[s.Name] = s.Path
		folders[s.Path] = true
		t.sets[t.naming.OfSet(*s)] = s
	}
	for _, s := range t.sets {
		for p := parent(s.Path); p != ""; p = parent(p) {
			if !folders[p] {
				add("%s: the folder %s declares no group or ACL; every folder is a set's name", describe(s), displayPath(p))
				folders[p] = true
			}
		}
	}
	root, ok := t.sets[names.Of(ACL, Root)]
	switch {
	case !ok:
		add("the root declares no ACL: %s owns the whole tree, so the root's %s is required", names.Of(ACL, Root), ACL.File())
	case len(root.Members) == 0:
		add("%s has no members: it is the root of ownership, so someone must be in it", t.naming.OfSet(*root))
	}
	for _, name := range sortedKeys(t.sets) {
		s := t.sets[name]
		seen := map[string]bool{}
		for _, m := range s.Members {
			k, ok := names.KindOf(m)
			switch {
			case !ok:
				add("%s: %q is no name under %s (<handle>.human.%[3]s, <name>.robot.%[3]s, <serviceaccount>.<namespace>.robot.%[3]s, <name>.group.%[3]s, or <name>.acl.%[3]s)", describe(s), m, names.domain)
				continue
			case seen[m]:
				add("%s: %s is a member twice", describe(s), m)
				continue
			case m == name:
				add("%s: a set cannot contain itself", describe(s))
				continue
			case s.Kind == Group && k == ACLMember:
				add("%s: a group cannot contain an ACL (%s): group identity carries no authorization", describe(s), m)
				continue
			case k == ACLMember && t.sets[m] == nil, k == GroupMember && t.sets[m] == nil && t.synced[m] == nil:
				add("%s: %s is not declared in the tree, nor synced from the system of record", describe(s), m)
				continue
			}
			seen[m] = true
		}
	}
	if len(problems) == 0 {
		if cycle := t.cycle(); cycle != nil {
			add("membership is circular: %s", strings.Join(cycle, " contains "))
		}
	}
	if len(problems) > 0 {
		return nil, &Error{Problems: problems}
	}
	t.names = sortedKeys(t.sets)
	for _, name := range t.names {
		t.owner[name] = t.deriveOwner(t.sets[name])
	}
	return t, nil
}

// deriveOwner is the ACL that owns s: for an ACL, the nearest ACL in a folder strictly above its
// own (the root's ACL owns itself); for a group, the ACL in its own folder or, without one, the
// nearest above. The root's ACL always exists, so every set has an owner.
func (t *Tree) deriveOwner(s *Set) string {
	if s.Kind == ACL && s.Path == "" {
		return t.naming.OfSet(*s)
	}
	p := s.Path
	if s.Kind == ACL {
		p = parent(p)
	}
	for {
		if a := t.aclAt(p); a != "" {
			return a
		}
		p = parent(p)
	}
}

// aclAt is the full name of the ACL declared in folder p, or "".
func (t *Tree) aclAt(p string) string {
	name := Root
	if p != "" {
		name = p[strings.LastIndexByte(p, '/')+1:]
	}
	if s, ok := t.sets[t.naming.Of(ACL, name)]; ok && s.Path == p {
		return t.naming.OfSet(*s)
	}
	return ""
}

// cycle returns a membership cycle among the sets (its first set repeated last), or nil.
func (t *Tree) cycle() []string {
	const (
		unseen = iota
		open
		done
	)
	state := map[string]int{}
	var stack []string
	var found []string
	var visit func(n string) bool
	visit = func(n string) bool {
		state[n] = open
		stack = append(stack, n)
		for _, m := range t.sets[n].Members {
			if _, ok := t.sets[m]; !ok {
				continue
			}
			switch state[m] {
			case open:
				i := len(stack) - 1
				for stack[i] != m {
					i--
				}
				found = append(append([]string(nil), stack[i:]...), m)
				return true
			case unseen:
				if visit(m) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[n] = done
		return false
	}
	for _, n := range sortedKeys(t.sets) {
		if state[n] == unseen && visit(n) {
			return found
		}
	}
	return nil
}

// Names is the naming the tree's sets are under.
func (t *Tree) Names() Names { return t.naming }

// Sets returns copies of the tree's sets, ordered by full name.
func (t *Tree) Sets() []Set {
	out := make([]Set, 0, len(t.names))
	for _, n := range t.names {
		out = append(out, *t.sets[n].clone())
	}
	return out
}

// Set returns a copy of the set with the given full name.
func (t *Tree) Set(name string) (Set, bool) {
	s, ok := t.sets[name]
	if !ok {
		return Set{}, false
	}
	return *s.clone(), true
}

// Owner returns the full name of the ACL that owns the named set, and whether the set exists.
func (t *Tree) Owner(name string) (string, bool) {
	o, ok := t.owner[name]
	return o, ok
}

// Owned returns the sets the named ACL owns, sorted; the root's ACL is not among its own.
func (t *Tree) Owned(acl string) []string {
	var out []string
	for _, n := range t.names {
		if n != acl && t.owner[n] == acl {
			out = append(out, n)
		}
	}
	return out
}

// Expand returns every principal (human or robot) in the named set, through the sets it
// contains, sorted. It is empty for a set that does not exist.
func (t *Tree) Expand(name string) []string {
	return expand(t.members, name)
}

// Synced reports whether a name is a group synced from the system of record that the tree knows.
func (t *Tree) Synced(name string) bool {
	_, ok := t.synced[name]
	return ok
}

// Memberships returns every set that contains member — a principal or a set — directly or
// through others, sorted.
func (t *Tree) Memberships(member string) []string {
	in := map[string][]string{} // member -> the sets directly containing it
	for _, n := range t.names {
		for _, m := range t.sets[n].Members {
			in[m] = append(in[m], n)
		}
	}
	for _, n := range sortedKeys(t.synced) {
		for _, m := range t.synced[n] {
			in[m] = append(in[m], n)
		}
	}
	seen := map[string]bool{}
	queue := append([]string(nil), in[member]...)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if seen[n] {
			continue
		}
		seen[n] = true
		queue = append(queue, in[n]...)
	}
	return sortedKeys(seen)
}

// Contains reports whether member — a principal or a set — is in the named set, directly or
// through others, and returns the shortest chain that puts it there: the set, each set in
// between, and the member.
func (t *Tree) Contains(set, member string) ([]string, bool) {
	if _, ok := t.members(set); !ok {
		return nil, false
	}
	prev := map[string]string{set: ""}
	queue := []string{set}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		ms, _ := t.members(n)
		for _, m := range ms {
			if _, ok := prev[m]; ok {
				continue
			}
			prev[m] = n
			if m == member {
				chain := []string{m}
				for c := n; c != ""; c = prev[c] {
					chain = append(chain, c)
				}
				for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
					chain[i], chain[j] = chain[j], chain[i]
				}
				return chain, true
			}
			if _, ok := t.members(m); ok {
				queue = append(queue, m)
			}
		}
	}
	return nil, false
}

// expand is every principal reachable from name through members, which reports a set's members
// (false for a name that is not a set: a principal). It tolerates cycles.
func expand(members func(string) ([]string, bool), name string) []string {
	principals := map[string]bool{}
	seen := map[string]bool{}
	stack := []string{name}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		ms, ok := members(n)
		if !ok {
			if n != name {
				principals[n] = true
			}
			continue
		}
		stack = append(stack, ms...)
	}
	return sortedKeys(principals)
}

// validPath reports whether p is "" or slash-separated lowercase DNS labels.
func validPath(p string) bool {
	if p == "" {
		return true
	}
	for _, l := range strings.Split(p, "/") {
		if !ValidLabel(l) {
			return false
		}
	}
	return true
}

// parent is the folder above p: "" (the root) for a top-level folder.
func parent(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ""
	}
	return p[:i]
}

// under reports whether folder p is f or beneath it.
func under(p, f string) bool {
	return f == "" || p == f || strings.HasPrefix(p, f+"/")
}

func displayPath(p string) string {
	if p == "" {
		return "the root"
	}
	return p + "/"
}

func describe(s *Set) string {
	file := s.Kind.File()
	if s.Path != "" {
		file = s.Path + "/" + file
	}
	return file
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// AsError returns err's *Error, if it is one.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}
