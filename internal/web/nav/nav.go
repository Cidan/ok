// Package nav is a reusable navigation component (Go model + templ renderers + a
// small JS behavior file). It is deliberately decoupled from any router: the model
// carries the current request path as a plain string and does prefix-matching to
// decide the active item, so it works with net/http, chi, htmx-boosted nav, or
// anything else. A layout picks how to present the model — LeftRail (a collapsible
// drawer) or TopBar.
package nav

import (
	"strings"

	"github.com/a-h/templ"
)

// Item is one navigation entry. Icon is a named key resolved by the templ `icon`
// renderer. Match is the paths that mark this item active, space-separated (see
// Model.IsActive); when empty, Href is used. Unread renders the small green activity dot on a
// section row. Menu is a section row's actions, in a kebab dropdown beside it; a row with none
// has no kebab. Shortcut is the keys that go there (e.g. "g h"), which the top bar carries as
// data-shortcut for the app's own script to bind.
type Item struct {
	Label    string
	Href     string
	Icon     string
	Match    string
	Unread   bool
	Menu     []MenuAction
	Shortcut string
}

// MenuAction is one entry of a section row's menu: a <button> carrying the app's Attrs, the
// hook its own script acts on (the nav only opens and closes the menu). Danger tones it as
// destructive.
type MenuAction struct {
	Label  string
	Danger bool
	Attrs  templ.Attributes
}

// Section is a titled group of items — e.g. a list of recent conversations.
type Section struct {
	Title string
	Items []Item
}

// Model is the full navigation model for one render. Brand is the logo/toggle (its
// Icon is shown in the collapsed rail; it toggles the drawer, it does not navigate).
// Actions are page-level actions shown under the brand. Sections are dynamic titled
// lists. Footer sits at the bottom. Current is the current request path, used for
// active-state matching.
type Model struct {
	Brand    Item
	Actions  []Item
	Sections []Section
	Footer   []Item
	Current  string

	// HighlightActions opts the top Actions into active-state highlighting (the initial server
	// render + nav.js's client refresh mark the matching Action). Off by default so an app whose
	// Actions are commands rather than destinations (e.g. a New or Search button) is unchanged;
	// an app whose Actions are real destinations (e.g. ok's Repositories) sets it true.
	HighlightActions bool

	// Logo, when set, is drawn in place of Brand's icon: the app's own mark, sized by the app to
	// fit the rail's 64px column.
	Logo templ.Component

	// ThemeToggle adds the theme control — at the foot of the rail, at the end of the top bar — which
	// cycles the reader's choice of the system's theme, light, and dark, for an app whose shell sets
	// layout.Props.Themes and serves static/theme.js.
	ThemeToggle bool

	// Center and End are the app's own controls in the top bar (TopBar): Center in its middle (a
	// search box), End at its end (an account menu). The rail shows neither.
	Center templ.Component
	End    templ.Component
}

// IsActive reports whether item it is the active destination for the current path. Its
// patterns are its Match, space-separated, else its Href: "/" matches the root alone; a path
// ending in "$" matches itself alone; any other path matches itself and every path below it, by
// segment (so /chat/abc activates both a /chat item and a /chat/abc item); and "*" matches any
// path no other item of the model matches — the
// item that owns everything else (ok's Repositories, active inside every repository). This is
// the "nav router" — intentionally just string matching, no dependency on any router package.
// nav.js's updateActive mirrors it.
func (m Model) IsActive(it Item) bool {
	pats := it.patterns()
	for _, p := range pats {
		if p != "*" && matchPath(m.Current, p) {
			return true
		}
	}
	if !contains(pats, "*") {
		return false
	}
	for _, other := range m.items() {
		if other.Href == it.Href && other.Label == it.Label {
			continue
		}
		for _, p := range other.patterns() {
			if p != "*" && matchPath(m.Current, p) {
				return false
			}
		}
	}
	return true
}

// items is every navigable item of the model.
func (m Model) items() []Item {
	all := append(append([]Item{}, m.Actions...), m.Footer...)
	for _, s := range m.Sections {
		all = append(all, s.Items...)
	}
	return all
}

// matchPath reports whether one pattern (never "*") matches the path.
func matchPath(cur, p string) bool {
	if p == "" || p == "#" {
		return false
	}
	if p == "/" {
		return cur == "/"
	}
	if exact, ok := strings.CutSuffix(p, "$"); ok {
		return cur == exact
	}
	return cur == p || strings.HasPrefix(cur, p+"/")
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// pattern is the item's patterns as one string: what IsActive matches, and what its link
// carries (data-nav-match) for nav.js to match after each swap.
func (it Item) pattern() string {
	if it.Match != "" {
		return it.Match
	}
	return it.Href
}

func (it Item) patterns() []string { return strings.Fields(it.pattern()) }

// current marks the item the active destination (aria-current="page"), or nothing.
func (m Model) current(it Item) templ.Attributes {
	if m.IsActive(it) {
		return templ.Attributes{"aria-current": "page"}
	}
	return templ.Attributes{}
}

// actionCurrent is current for a top Action: only when the app opts its Actions in
// (HighlightActions), so an app whose Actions are commands renders none marked.
func (m Model) actionCurrent(it Item) templ.Attributes {
	if !m.HighlightActions {
		return templ.Attributes{}
	}
	return m.current(it)
}

// url wraps a href as a templ.SafeURL so it can be used in an href attribute.
func (it Item) url() templ.SafeURL { return templ.SafeURL(it.Href) }

// isAction reports whether this item is a no-op action rather than a destination — its Href
// is "#" (e.g. a Search the app's script opens). Such items render as a non-navigating
// <button> so clicking them never changes the URL or scroll.
func (it Item) isAction() bool { return it.Href == "#" }

// Class strings shared by the rail renderers, in the theme tokens (docs/web.md, "Theme
// tokens"). An active destination carries aria-current="page", which the aria-[current=page]
// variants style, so nav.js only moves the attribute. The rail rows' inset, radius, and active
// mark are the app-tunable custom properties LeftRail documents; unset, a row spans the rail.
const (
	railItemBase = "relative mx-[var(--nav-inset,0px)] items-center rounded-[var(--nav-radius,0px)] text-muted hover:text-fg hover:bg-hover transition-colors hidden md:flex group-data-[open=true]/nav:flex " +
		"aria-[current=page]:bg-selected aria-[current=page]:text-fg " +
		"before:absolute before:top-1/2 before:left-[calc(-1*var(--nav-inset,0px))] before:h-5 before:w-[3px] before:-translate-y-1/2 before:rounded-r-full before:bg-[var(--color-nav-mark,transparent)] before:opacity-0 before:transition-opacity aria-[current=page]:before:opacity-100"
	railIconBox     = "w-[calc(4rem_-_2*var(--nav-inset,0px))] flex items-center justify-center shrink-0"
	railLabelBase   = "whitespace-nowrap transition-opacity duration-300 text-sm font-medium opacity-0 group-data-[open=true]/nav:opacity-100"
	sectionRowClass = "w-full flex items-center px-4 py-2 text-muted hover:text-fg hover:bg-hover transition-colors text-left truncate aria-[current=page]:bg-selected aria-[current=page]:text-fg"
	menuAction      = "w-full text-left px-3 py-1.5 text-sm text-body hover:bg-hover-strong hover:text-fg"
	menuDanger      = "w-full text-left px-3 py-1.5 text-sm text-danger hover:bg-danger-soft"
	topItemClass    = "flex h-9 shrink-0 items-center gap-2 rounded-lg px-2.5 text-sm font-medium text-muted transition-colors hover:bg-hover hover:text-fg aria-[current=page]:bg-selected aria-[current=page]:text-fg"
)

// class returns a menu action's classes: destructive actions in the danger tone.
func (a MenuAction) class() string {
	if a.Danger {
		return menuDanger
	}
	return menuAction
}

// footerClass returns a footer link's classes: the rail item base (+h-16), and mt-auto for
// the first footer item (pushes the footer group to the bottom).
func footerClass(first bool) string {
	c := railItemBase + " h-16"
	if first {
		c += " mt-auto"
	}
	return c
}

// themeToggleClass returns the theme control's classes: a rail row at the foot of the rail,
// pushed to the bottom when no footer link is (first), with the rail's bottom margin below it.
func themeToggleClass(first bool) string {
	c := railItemBase + " h-12 mb-[var(--nav-inset,0px)] cursor-pointer"
	if first {
		c += " mt-auto"
	}
	return c
}
