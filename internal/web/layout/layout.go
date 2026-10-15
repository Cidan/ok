// Package layout is the reusable page shell: it owns the HTML document (head assets,
// fonts, the full-viewport dark body) and arranges a navigation presentation around a
// content slot. The nav is presentation-agnostic (package nav); the layout picks HOW
// it is shown via Variant, so "the nav adapts to the layout": LeftNav renders the
// collapsible left drawer, TopNav a horizontal bar, Navless a centered card (onboarding).
package layout

import "github.com/Cidan/ok/internal/web/nav"

// Variant selects the chrome arrangement.
type Variant int

const (
	LeftNav Variant = iota // collapsible left drawer + content
	TopNav                 // horizontal top bar + content
	Navless                // centered content, no nav (first-run/onboarding)
)

// Props configures one page render. Nav is ignored for the Navless variant.
//
// The head asset set is caller-supplied so the shell stays application-neutral: the
// consumer lists exactly the stylesheets and deferred scripts its pages need (served by
// the consumer's own /assets handler), in document order. The shell contributes only the
// application-agnostic document scaffolding — charset/viewport/title, the optional favicon,
// and the hydration reveal-gate style + font-preload guard.
type Props struct {
	Title   string
	Variant Variant
	Nav     nav.Model

	// Favicon is the href for the <link rel="icon"> tag; omitted when empty.
	Favicon string
	// Styles are stylesheet hrefs, emitted as <link rel="stylesheet"> in order.
	Styles []string
	// Scripts are script srcs, emitted as deferred <script defer> in order (defer keeps
	// document order, so the hydration runtime must be listed before any hydrator).
	Scripts []string
	// HistoryRestore makes Back and Forward swap <main id="content"> alone (hx-history-elt):
	// htmx 4 keeps no history cache, and restores a page by fetching it again and swapping its
	// history element — the whole <body>, rail and all, without one. Set it only when every
	// page serves the whole document to a request carrying HX-History-Restore-Request.
	HistoryRestore bool
}
