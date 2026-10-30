# ok's web UI

ok's pages — ok control's (`internal/control/pages`) and ok identity's (`internal/id/web`) — are
server-rendered [templ](https://templ.guide) components, styled with
[Tailwind CSS](https://tailwindcss.com) v4, made a single-page application by
[htmx](https://htmx.org). `go run ./cmd/ok-preview` serves ok control's pages on mock data, to work
on them without a backend.

## The parts

- **`internal/web`** — what every page set shares and no page set owns: `layout` (`Shell`, the
  document; its head assets are the caller's, `Props.Styles`, `Props.Scripts`, `Props.Favicon`),
  `nav` (the navigation rail's model and markup; a section row's kebab menu is the page set's,
  `Item.Menu`, which its own script acts on), `breadcrumb` (`Trail`; a `Crumb.Target` makes a
  linked crumb an htmx swap of that selector, `Crumb.Swap` its swap), `clipboard` (`Button` and
  `clipboard.js`: a delegated click copies the button's `data-copy` text), `infinitelist`
  (`Scroller`, a windowed infinite-scroll list, and `infinite.js`), and `static`: htmx, the
  hydration runtime (`hydrate.js`), the components' scripts, `livefilter.js` (an
  `input[data-filter]` that debounce-fetches a list's fragment and re-hydrates it), IBM Plex Sans,
  and the favicon. A component names no page set's routes, assets, or data.
- **A page set** owns its pages, its navigation model, its stylesheet and script set, and its
  `input.css`, whose `@source` lines name every file with Tailwind classes — the page set's, and the
  shared components it renders — and nothing is scanned that they do not name
  (`@import "tailwindcss" source(none)`). `scripts/css.sh` builds each `app.css`.

## Navigation & hydration: EVERY navigation link uses htmx (hard rule)

ok's pages are persistent htmx SPAs: the document shell (nav rail, head assets, and any
region outside the swapped boundary) is rendered once and **must never reload**. A full-page
`<a href>` navigation re-parses `<body>`, re-runs the shell fade + font-swap, and re-renders the
nav rail — a visible flicker. Therefore:

> **ALL navigation links MUST use htmx.** Every `<a>` that navigates within the app carries
> `hx-get` + `hx-target` + `hx-swap="innerHTML swap:70ms settle:1ms"` + `hx-push-url="true"`,
> and keeps its real `href` (progressive enhancement + correct link semantics). A plain
> `<a href>` with no `hx-*` is a bug for anything that navigates.

The **only** links that stay plain `<a>` (NON-nav) are: raw byte endpoints (`Raw`/`Download`),
external URLs, and same-document fragment anchors (`#Lnn` scroll targets).
Everything else is htmx. Form submissions that navigate use `hx-post` for the same reason.

**Hydration boundaries (`[data-hydrate]`) and the target you swap.** The reveal gate and the
target-aware runtime (`static/hydrate.js`) key off `[data-hydrate]` regions, which
**nest**. `layout.Shell` makes `<main id="content" data-hydrate>` the top-level boundary; an app
adds nested boundaries (e.g. ok control's `<div id="repo-main" data-hydrate>` below a repo's invariant
header) for finer swaps. On a swap the runtime hides/reveals **only the boundary being swapped**
(the trigger's `hx-target`, resolved to its nearest boundary) plus any nested inside it — so a
partial swap never touches the rail, the header, or sibling regions.

Pick the target as **the smallest boundary that fully reflects the navigation**:
- Leaving the current sub-context / moving between top-level pages → `hx-target="#content"`.
- Navigating within a context whose surrounding chrome is invariant (e.g. browsing files,
  switching branch, paging commits inside one repo — the repo header + rail don't change) →
  target the nested boundary (`hx-target="#repo-main"`), so only that region fades.

**The server must render the matching fragment.** A page's `Serve*` func renders three ways:
the **full document** on a deep link (no htmx headers), the **content-region fragment** on a
generic htmx nav (`HX-Request: true`), and a **nested-region fragment** when htmx targets a nested
boundary — return just that region's inner markup. Factor each boundary's inner markup into its
own templ (`FooContent` wraps the header + `<div id="foo-main" data-hydrate>{ FooMain }</div>`;
`FooMain` is what the nested-target branch returns).

> **GOTCHA — the `HX-Target` header is `<tag>#<id>`, NOT the bare id.** htmx (v4) serialises the
> swap target as ``${tagName}${id ? "#"+id : ""}`` — so a swap into `<div id="repo-main">` sends
> **`HX-Target: div#repo-main`**, and a naive `r.Header.Get("HX-Target") == "repo-main"` NEVER
> matches (the request silently falls through to the `HX-Request` branch and the header-bearing
> content fragment lands inside the nested region — a duplicated header). Match the id part after
> the `#`: `hxTargetIs(r, "repo-main")` → `strings.LastIndexByte(t, '#')`, compare the tail. Note
> the same serialisation is used for the `HX-Source` header (the *trigger's* `tag#id`). Verify
> partial-swap wiring with the REAL header value (`div#repo-main`), not the bare id — a curl with
> `-H "HX-Target: repo-main"` will pass against the buggy check and hide the defect.

**`hx-target` is a plain CSS selector (or `this`).** The runtime resolves a swap's region with
`document.querySelector(hx-target)`, so htmx's extended selectors (`find …`, `closest …`,
`next …`) would throw inside it. Give a region an id and target `#id`.

**Back and Forward (history restores).** htmx 4 keeps no history cache: on Back it fetches the
page again with `HX-History-Restore-Request: true` and swaps the history element — `<body>`,
rail and all, unless one is named. An app sets `layout.Props.HistoryRestore`, which makes
`<main id="content">` the history element (`hx-history-elt`), so a restore swaps only `#content`;
and then every page must render the **whole document** to a restore request (ok control's `hxNav`
treats it as a deep link), from which htmx takes `#content`.

**A region that is its own scroll container** keeps its scroll position across an `innerHTML`
swap: a navigation into it adds `scroll:top` to its swap spec (ok control's browser pane).

**Live regions (`[data-live]`).** A region that refreshes itself — a status that changes, a log
that grows, a row whose button rewrites the region it sits in — marks itself `data-live`, and so
does what the server swaps back in. A swap inside a live region hydrates in place with no
hide/reveal fade: a region the reader is watching must not blink each time it refreshes.

A region refreshes as what it shows changes, not on a timer (state.md, "Waiting"): it names its
page's stream of server-sent events (`data-live-events`: a CL's `…/cl/N/events`, a run's, a
deployment's, `/services/events`) and fetches itself again on htmx's `live` trigger
(`hx-trigger="live"`). `live.js` keeps one `EventSource` a stream, however many regions name it,
and on each `change` event fires `live` on each of them; a change that comes while they are being
fetched fetches them once more after their swap, and one that comes while the page is hidden, once
it is shown. The server's stream registers, with each service whose writes change what the page
shows, on what it shows — ok control's record of a CL, ok automation's checks and runs, the
deployer's deployments and services (`internal/wait`) — and says `change` as it opens, for what
changed between the page's render and the registration, then as each change commits. A region stops
by being swapped for one that names no stream, or by leaving the document; once no region names a
stream, it closes. A region on a timer (`hx-trigger="every …"`) is not sent while the page is
hidden (the runtime cancels it) and resumes on its next tick. A refresh that must update a region elsewhere on the page from the same reading
(a sub-nav's statuses beside the pane) carries it out of band (`hx-swap-oob`); htmx 4 fires one
`before:swap`/`after:swap` pair on the trigger for the whole response, so the out-of-band region
is swapped without a fade. Swap a scroll container's rows (`hx-swap-oob="innerHTML"`), not the
container, so its scroll position stays.

**Shared components** carry the htmx wiring through, app-neutrally: e.g. `breadcrumb.Crumb.Target`
makes a crumb an htmx swap of the caller-supplied selector instead of a full load. A shared
component never hardcodes an app's boundary id — the consumer supplies it.

## Visual design system (hard rules)

Every page of ok must look like **one designer** built it. Consistency comes from a small set of
primitives that every page composes, never from restyling each surface ad hoc. An app defines
its primitives once (ok control's live in `internal/control/pages/ui.templ` + the class helpers in `web.go`)
and pages use them; a page that hand-rolls a box, header, chip, or button is a bug.

- **One type scale.** UI text is a single size (`text-sm`): body, labels, headers, chips,
  buttons, table columns, metadata. Hierarchy comes from **weight and colour**, not size. The
  only exceptions are page-level titles (the page header, and a page's subject heading such as
  a change or commit subject) and **code surfaces** (diffs/source), which use one shared compact
  monospace size. Never introduce one-off sizes (`text-xs`, `text-[10px]`, `text-base`, …).
- **One case: Title Case.** Headers, column labels, chips, buttons, tabs, and nav labels are
  Title Case ("Change Info", "Submit Requirements", "Added"). **Never ALL CAPS** — no
  `uppercase`, no letter-spaced small-caps labels. Data (names, paths, commit subjects, user
  text) renders verbatim.
- **Every boxed surface is a panel, and every panel has a header.** A panel is one bordered,
  rounded, untinted container. Its first child is a header strip: fixed height, the header
  weight/colour, **a hairline rule across the full panel beneath it, and no background tint**.
  Content is either a title with optional trailing meta/actions, or a table's column labels.
  No panel without a header; no section heading floating outside its panel; no "sub-panel"
  created by a rule inside another panel (split it into two panels instead). Empty states live
  inside the panel, below its header.
- **One chip.** Status/labels/tags are a single chip shape and size, coloured by **tone** chosen
  from meaning — success, danger, info, warn, neutral — never by call site. A chip never nests
  inside another chip; secondary information inside a chip is tinted text. Where a chip on every
  row would crowd a list (a sub-nav, a group's services), a status is the chip's **dot**: the
  same tone, titled with what the chip would say.
- **One button geometry.** Every button (including `<summary>`-based disclosure triggers and
  button-styled links) shares one size, radius, and border box; only the variant colour differs
  (primary, success, secondary, inverse, disabled). Filled variants carry a transparent border
  so every variant is the same height.
- **Shape and spacing vocabulary.** Panels and floating popovers share one large radius;
  controls (buttons, inputs, selects) share one medium radius; chips one small radius. Panels
  in a column/grid share one gap; major page sections share one larger gap; list rows inside
  panels share one row padding.
- **Prose is never monospace.** Comments, messages, and labels use the UI font even when
  rendered inside a code table (reset the font on the row); monospace is for code, paths,
  hashes, and identifiers only.
- **Stable shape: never hide a field or a panel because it's empty.** A panel and its rows keep
  the same structure whatever the data: an empty field renders **`—`**, an empty panel renders
  its empty state below its header, and a row is sized for its tallest possible content (e.g.
  a key/value row is at least a chip tall), so a row showing `—` is exactly as tall as one
  showing chips. Information appearing and disappearing makes a reader re-scan and hides what a
  field *means*. Size only changes where content genuinely grows (a list, wrapped chips).
- **One home per piece of information.** Each dataset has exactly one surface (a panel or a
  tab); summaries elsewhere are a row in an existing panel, not a second panel. Stable shape
  governs how a surface looks when empty, never whether a duplicate surface should exist — a
  panel that only mirrors another surface is removed, not made permanent.
- **Name fields by what they mean, distinctly.** Two rows about related people (e.g. who *has*
  reviewed vs. who *still needs to* approve) get labels that can't be read as synonyms.
- **Popovers float; they are never clipped.** A popover (a dialog, a menu, a "who can approve"
  list) always renders whole, above the page. Panels clip their overflow, so a popover opened from
  inside a panel is positioned `fixed`, rendered outside the panel, placed at its trigger or the
  pointer and clamped to the viewport, and closed by an outside click or Escape. An in-flow
  `<details>` popover (absolutely positioned under its summary) is only for triggers that sit
  outside every panel, such as a page header's actions. A popover is headed like a panel.

When a new need doesn't fit a primitive, **extend the primitive** (a new tone, a new variant)
rather than styling around it — and verify by measuring the rendered DOM (computed font size,
case, header height/tint, and a panel's height with empty vs. populated data) across every
page, not just the one you touched. `go run ./cmd/ok-preview` serves every page on mock data, for a
headless browser to measure.
