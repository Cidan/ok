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
// renderer. Match is the path prefix that marks this item active; when empty, Href is
// used. Unread renders the small green activity dot on a section row. Menu is a section
// row's actions, in a kebab dropdown beside it; a row with none has no kebab.
type Item struct {
	Label  string
	Href   string
	Icon   string
	Match  string
	Unread bool
	Menu   []MenuAction
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
}

// IsActive reports whether item it is the active destination for the current path. It
// matches by prefix (Match, else Href): an exact equality, or a path segment prefix
// (so /chat/abc activates both a /chat item and a /chat/abc item). This is the "nav router" — intentionally just string matching, no
// dependency on any router package.
func (m Model) IsActive(it Item) bool {
	p := it.Match
	if p == "" {
		p = it.Href
	}
	if p == "" || p == "#" {
		return false
	}
	if p == "/" {
		return m.Current == "/"
	}
	return m.Current == p || strings.HasPrefix(m.Current, p+"/")
}

// url wraps a href as a templ.SafeURL so it can be used in an href attribute.
func (it Item) url() templ.SafeURL { return templ.SafeURL(it.Href) }

// isAction reports whether this item is a no-op action rather than a destination — its Href
// is "#" (e.g. a Search the app's script opens). Such items render as a non-navigating
// <button> so clicking them never changes the URL or scroll.
func (it Item) isAction() bool { return it.Href == "#" }

// Base class strings shared by the rail renderers. Kept in Go (not duplicated in the
// templ) so the two active/inactive variants stay in one place and are unit-testable.
const (
	railItemBase   = "w-full items-center text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 transition-colors hidden md:flex group-data-[open=true]/nav:flex"
	railLabelBase  = "whitespace-nowrap transition-opacity duration-300 text-sm font-medium opacity-0 group-data-[open=true]/nav:opacity-100"
	sectionRowBase = "w-full flex items-center px-4 py-2 text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 transition-colors text-left truncate"
	activeRow      = "bg-zinc-800 text-zinc-100"
	menuAction     = "w-full text-left px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-700 hover:text-zinc-100"
	menuDanger     = "w-full text-left px-3 py-1.5 text-sm text-red-400 hover:bg-red-500/10 hover:text-red-300"
)

// actionRowClass returns a rail Action's classes, adding the active highlight when
// HighlightActions is on and the item matches the current path (otherwise the plain base, so
// an app that doesn't opt in renders exactly as before).
func (m Model) actionRowClass(it Item) string {
	c := railItemBase + " h-12"
	if m.HighlightActions && m.IsActive(it) {
		c += " " + activeRow
	}
	return c
}

// sectionRowClass returns a section row's classes, adding the active highlight when the
// item matches the current path.
func (m Model) sectionRowClass(it Item) string {
	if m.IsActive(it) {
		return sectionRowBase + " " + activeRow
	}
	return sectionRowBase
}

// class returns a menu action's classes: destructive actions in the danger tone.
func (a MenuAction) class() string {
	if a.Danger {
		return menuDanger
	}
	return menuAction
}

// topItemClass returns a TopBar pill's classes, adding the active highlight on match.
func topItemClass(m Model, it Item) string {
	base := "flex items-center gap-1.5 px-3 h-10 rounded-lg text-sm font-medium text-zinc-400 hover:text-zinc-100 hover:bg-zinc-900 transition-colors"
	if m.IsActive(it) {
		return base + " bg-zinc-900 text-zinc-100"
	}
	return base
}

// footerClass returns a footer link's classes: the rail item base (+h-16), mt-auto for
// the first footer item (pushes the footer group to the bottom), and the active
// highlight when it matches the current path.
func (m Model) footerClass(it Item, first bool) string {
	c := railItemBase + " h-16"
	if first {
		c += " mt-auto"
	}
	if m.IsActive(it) {
		c += " " + activeRow
	}
	return c
}
