# ok's web UI

ok's pages — ok control's (`internal/control/pages`) and ok identity's (`internal/id/web`) — are
server-rendered [templ](https://templ.guide) components, styled with
[Tailwind CSS](https://tailwindcss.com) v4, made a single-page application by
[htmx](https://htmx.org), in a light and a dark theme the reader chooses. `go run ./cmd/ok-preview`
serves ok control's pages on mock data, to work on them without a backend.

## The parts

- **`internal/web`** — what every page set shares and no page set owns: `layout` (`Shell`, the
  document; its head assets are the caller's, `Props.Styles`, `Props.Scripts`, `Props.Favicon`;
  `Props.Themes` makes the page set light and dark; `Props.Overlay` is what it draws once above
  every page — a command palette, a dialog — outside every swapped region), `nav` (the navigation's
  model and markup: the rail, `LeftRail`, and the top bar, `TopBar`, which adds the page set's name
  beside its mark, `Model.Logo`, its destinations as pills, two slots of its own — `Model.Center`,
  a search field, and `Model.End` — and the theme control, `Model.ThemeToggle`, and carries each
  destination's `Item.Shortcut` for the page set's script to bind; a section row's kebab menu is
  the page set's, `Item.Menu`, which its own script acts on), `breadcrumb` (`Trail`; a
  `Crumb.Target` makes a linked crumb an htmx swap of that selector, `Crumb.Swap` its swap),
  `clipboard` (`Button` and `clipboard.js`: a delegated click copies the button's `data-copy`
  text), `infinitelist` (`Scroller`, a windowed infinite-scroll list, and `infinite.js`), and
  `static`: htmx, the hydration runtime (`hydrate.js`), the loading feedback (`progress.js`), the
  theme's choice and switch (`theme-init.js`, `theme.js`), the components' scripts,
  `livefilter.js` (an `input[data-filter]` that debounce-fetches a list's fragment and re-hydrates
  it), the shell's stylesheet, IBM Plex Sans, Geist and Geist Mono, and the favicon. A component
  names no page set's routes, assets, or data.
- **A page set** owns its pages, its navigation model, its stylesheet and script set, and its
  `input.css`, whose `@source` lines name every file with Tailwind classes — the page set's, and the
  shared components it renders — and nothing is scanned that they do not name
  (`@import "tailwindcss" source(none)`). `scripts/css.sh` builds each `app.css`.
- **No inline script or style.** The pages' policy (`Content-Security-Policy: default-src 'self'`)
  refuses an inline `<script>`, `<style>`, or `style=""`: the shell's styles are `static/shell.css`,
  the theme's first-paint choice `static/theme-init.js`, and a script styles an element through
  CSSOM (`el.style.width = …`), never by writing a `style` attribute or a `<style>` element.

## Navigation & hydration: EVERY navigation link uses htmx (hard rule)

ok's pages are persistent htmx SPAs: the document shell (nav rail, head assets, and any
region outside the swapped boundary) is rendered once and **must never reload**. A full-page
`<a href>` navigation re-parses `<body>`, re-runs the shell fade + font-swap, and re-renders the
nav rail — a visible flicker. Therefore:

> **ALL navigation links MUST use htmx.** Every `<a>` that navigates within the app carries
> `hx-get` + `hx-target` + `hx-swap="innerHTML swap:70ms settle:1ms"` (with `scroll:top` when
> its target is a scroll container, so the new page starts at its top — ok control's `swapNav`) +
> `hx-push-url="true"`, and keeps its real `href` (progressive enhancement + correct link
> semantics). A plain `<a href>` with no `hx-*` is a bug for anything that navigates.

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

**The current destination is `aria-current="page"`.** A link that marks where the reader is —
a rail item, a section row, an app's tabs — carries `data-nav-link` and its paths,
`data-nav-match` (space-separated: `/` the root alone, a path ending in `$` itself alone, any
other path itself and everything below it, `*` any path no other link of its group matches). The
server marks the initial render (`nav.Model.IsActive`); `nav.js` re-marks every such link after
each swap, within its group (its nearest `[data-nav-group]`: the rail, the top bar, a set of
tabs). Style the current one with `aria-[current=page]:` variants, never a toggled class.

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

## Getting around

A page set's chrome is either the rail (`layout.LeftNav`) or the top bar (`layout.TopNav`, ok
control's: Home, Repositories, Services, Notifications). In the top bar, the search field
(`Model.Center`) opens the page set's **command palette** — its own, drawn in
`layout.Props.Overlay`: one field that finds anything it holds and any command it offers, moved
through with the arrow keys and opened with Enter, Ctrl+K (⌘K) and `/` opening it from anywhere.
**Keyboard shortcuts** go where the pointer does: `g` then a letter follows the link carrying it
(`data-shortcut`, which the top bar renders from `Item.Shortcut`), `?` shows them all, and none
fires while the reader is typing. ok control's palette, shortcuts, and their help are
`internal/control/pages/palette.templ` and `ok.js`; its home is a dashboard
(`dashboard.templ`) of the reader's inbox, notifications, repositories, and services.

## Loading feedback

A reader who clicks sees at once that something is happening, and never a blank or a flicker:

- **The progress bar and the busy region** (`static/progress.js`). While a request the
  reader is waiting on is in flight — a navigation, a form — a thin bar (`#page-progress`, the
  app's colours by its id) creeps across the top of the window and finishes when the answer is
  in, and the hydration boundary the answer will replace is `aria-busy="true"`, which the app
  dims. Both wait about a tenth of a second first, so a quick answer shows nothing. A request
  nobody is waiting on — inside a `[data-live]` region, or triggered by `intersect`, `load`,
  `revealed`, `every`, `live`, `input`, `keyup`, or `toggle` — shows nothing.
- **The control that asked** carries htmx's `htmx-request` class meanwhile (the element with the
  `hx-*` attributes: a `<form>`, or a button); a button shows a spinner in place of its label
  and takes no more clicks.
- **Content on its way** inside a page — a diff as its row opens, the next page of a list — is a
  placeholder of shimmering lines in the space it will fill, saying what loads for a screen
  reader, never a bare "Loading…".
- **A navigation starts the page at its top** (`scroll:top` in its swap spec: a region that is
  its own scroll container keeps its scroll position across a swap); a form's answer re-renders
  the page where the reader was.

Motion is short, and only says something: a region's reveal (a fade, and in an app that wants it
a rise of a few pixels), a busy region's dim, a popover's drop-in, a tab's underline drawn out.
All of it stops under `prefers-reduced-motion`.

## Theme tokens

Colours are **roles**, never palette shades: a page draws with `bg-surface`, `text-muted`,
`border-line`, `text-success`, `bg-accent-soft`, … — Tailwind utilities of `--color-*` theme
variables each app defines in its `input.css` `@theme` — and the app decides what each role is.
The shared components use only these:

| Token | Used for |
|---|---|
| `canvas` | the page behind everything (the shell's `<html>`/`<body>`, the rail, the top bar) |
| `raised`, `overlay` | the open drawer and the top bar's current item; a kebab menu |
| `hover`, `hover-strong`, `selected` | a row under the pointer, a control under it, the current row |
| `line`, `line-strong` | the top bar's rule; a menu's border |
| `fg`, `body`, `muted`, `subtle`, `faint` | text, strongest to weakest |
| `danger`, `danger-soft` | a destructive menu action |
| `--font-sans` | the UI font (the shell's `font-sans`) |

and these custom properties, optional, which leave the plain rail when unset: `--nav-inset` and
`--nav-radius` (a rail row's inset from the rail's edges and its corner radius), and
`--color-nav-mark` and `--color-rail-line` (the bar beside the current rail row, the rail's
right hairline). ok control defines a full set — surfaces, lines, text, its accent, each tone's
text, fill, and border, diff colours, syntax colours — in two themes
(`internal/control/pages/input.css`), and ok identity the same theme tokens
(`internal/id/web/input.css`).

**Light and dark.** An app with two themes sets `layout.Props.Themes`, serves
`static/theme.js` (and names `static/theme-init.js` first, which `layout.Shell` does), and turns on the
navigation's theme control (`nav.Model.ThemeToggle`). Its
stylesheet's `@theme` is one theme, and `:root[data-theme="light"]` (or `"dark"`) overrides the
same variables, with `color-scheme`, for the other. The shell picks the reader's theme before the
first paint — their choice (localStorage `theme`: `light`, `dark`, or unset for the system's), or
the system's — and marks `<html>` with `data-theme` (the theme shown) and `data-theme-pref` (the
choice); `theme.js` cycles the choice from the control (system, light, dark), follows the system
while the choice is the system's, applies a choice made in another tab, and keeps the browser
chrome (`meta theme-color`) on the canvas. A change of theme grows out of the control as a circle
where the browser has view transitions; elsewhere, and under reduced motion, it is instant.

## Visual design system (hard rules)

Every page of ok must look like **one designer** built it. Consistency comes from a small set of
primitives that every page composes, never from restyling each surface ad hoc. An app defines
its primitives once (ok control's live in `internal/control/pages/ui.templ` + the class helpers in `web.go`)
and pages use them; a page that hand-rolls a box, header, chip, or button is a bug.

- **One type scale, used for hierarchy.** Every piece of text takes a step of one scale, by
  what it is — never a one-off size (`text-[10px]`, `text-[15px]`):

  | Size | Is for |
  |---|---|
  | `text-xs` (12px) | metadata beside something larger: a row's second line, a count, a key (`kbd`), a kicker |
  | `text-sm` (14px) | the UI: body, labels, panel headers, chips, buttons, tabs, table columns |
  | `text-base` (16px) | lead text: a page's subtitle, a hero's summary, the palette's search field |
  | `text-lg` / `text-xl` (18/20px) | a context's name (a repository in its header), a page's subject (a CL's, a commit's) |
  | `text-2xl`–`text-4xl` (24–36px) | page titles and a hero's headline; big numbers in a stat |

  **Code surfaces** (diffs, source, logs, a commit message) use one compact monospace size
  (12.5px). Within a step, hierarchy is **weight and colour**.
- **One case: Title Case.** Headers, column labels, chips, buttons, tabs, and nav labels are
  Title Case ("Change Info", "Submit Requirements", "Added"). The one capitalized, letter-spaced
  label is a **kicker** — the small accent label over a hero's or a section's heading that says
  what it is part of ("Your Day on ok") — and it is never a heading itself. Data (names, paths,
  commit subjects, user text) renders verbatim.
- **Every boxed surface is a panel, and every panel has a header.** A panel is one bordered,
  rounded container: one surface, lifted off the canvas (its own colour, and in a themed app a
  shadow). Its first child is a header strip: fixed height, **a hairline rule across the full
  panel beneath it, and no background tint of its own**. Content is either a title (the
  strongest text, semibold) with optional trailing meta/actions, or a table's column labels
  (quieter). A page's **hero** — a dashboard's greeting, its counts, and its main actions — is
  the one boxed surface without a header strip: it is the page's own head.
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
- **Colour is a role, never a shade** ("Theme tokens" above): a page that names a palette shade
  (`text-zinc-400`, `bg-amber-500/10`) is a bug in a themed app — it is right in one theme at
  best. Status colour is a tone's (success, danger, info, warn, neutral), chosen by meaning; the
  accent is for the one primary action, links, the current place, and focus.
- **Layers.** The canvas is the bottom; panels sit on it; a header pinned while its page
  scrolls under it (a CL's) is frosted — the canvas, mostly opaque, blurred — so the page reads
  as passing beneath it; a popover or a dialog floats over everything with the deepest shadow,
  and a dialog over the whole page (a command palette) dims and blurs what is under it.
- **Shape and spacing vocabulary.** Panels and floating popovers share one large radius;
  controls (buttons, inputs, selects) share one medium radius; chips are one shape (ok control's: a
  pill). Panels
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
