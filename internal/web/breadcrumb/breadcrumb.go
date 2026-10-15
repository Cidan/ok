// Package breadcrumb is a reusable, application-neutral path breadcrumb: a horizontal trail
// of segments separated by a divider, where each segment is either a link (a Crumb with an
// Href) or the plain, non-linked current location (a Crumb with an empty Href, typically the
// last). It makes no assumption about routing — the consumer supplies the labels and hrefs —
// so any app that shows a hierarchical path (a file tree, a settings section, a nav trail)
// uses it unchanged.
package breadcrumb

// Crumb is one segment of the trail. Href empty renders the segment as plain current-location
// text (not a link). Target, when set on a linked crumb, makes it an htmx navigation that swaps
// that CSS-selector region (e.g. "#repo-main") and pushes the URL, instead of a full-page load —
// so the app's persistent shell (nav rail, headers) never reloads. The consumer supplies the
// selector; the component stays routing-neutral. Href stays as the progressive-enhancement anchor.
// Swap is the htmx swap spec of such a navigation (e.g. one that scrolls its region back to the
// top); empty is the estate's default fade.
type Crumb struct {
	Label  string
	Href   string
	Target string
	Swap   string
}

// swap is the crumb's htmx swap spec.
func (c Crumb) swap() string {
	if c.Swap != "" {
		return c.Swap
	}
	return "innerHTML swap:70ms settle:1ms"
}
