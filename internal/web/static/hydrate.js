// hydrate.js — the client hydration runtime for the server-rendered web apps.
//
// Problem it solves: our content is server-rendered as inert markup that client libraries
// turn into the real thing — [data-md] → formatted markdown, `$…$` → KaTeX, <pre><code> →
// highlighted code, [data-figure-widget] → D3 charts. If that content is REVEALED before it
// is hydrated, the user watches it reflow and pop as each library runs (the "jumpy" fade).
// The fix is to hydrate while the content is hidden, then reveal the finished, laid-out
// result as one piece.
//
// This runtime is the shared machinery for that, decoupled from any component:
//
//   • Hydrators register a callback: Hydra.register(name, fn, priority). fn(root) enhances
//     everything inside `root` and MUST be idempotent (safe to run again over already-hydrated
//     content — our hydrators guard via data-md removal, data-hl, _figHydrated, and KaTeX
//     leaving no `$` delimiter behind). Lower priority runs first.
//
//   • Boundaries are regions whose reveal waits on hydration: they carry data-hydrate and
//     ship hidden (the `hydrating` class → opacity:0, see static/shell.css). Today that is
//     <main id="content">; any future swappable region opts in the same way.
//
// Race-free by construction: run() returns Promise.allSettled over the hydrators, and the
// reveal is chained off that promise — never a setTimeout/rAF guess. Sync hydrators settle on
// the microtask; a genuinely async one settles when it is actually done; a throwing one still
// settles, so a reveal can never hang. reveal() forces one synchronous reflow before dropping
// the gate so the opacity transition reliably plays (a style flush, not a timer).
//
// Lifecycle (owned here, so components never wire htmx themselves):
//   • initial / deep-link load: boundaries render with `hydrating`; on DOMContentLoaded we
//     run(document) then reveal. A <noscript> override reveals them when JS is off.
//   • htmx swap: target-aware. htmx:before:swap re-hides ONLY the boundary being swapped (and
//     any boundaries nested in it) so unhydrated swapped-in content never flashes; htmx:after:swap
//     hydrates that region then reveals it. resolveScope reads the swap target from the trigger's
//     hx-target (before:swap) or the swapped element itself (after:swap), so a partial swap
//     (e.g. #repo-main inside #content) never touches the nav rail, the header, or siblings; an
//     unresolvable scope degrades to the whole document.
//   • live swap: a swap within a [data-live] region (a polled status, a growing log) hydrates in
//     place, without hiding or revealing anything. A live region's poll (hx-trigger "every …")
//     does not go out while the page is hidden — no one is watching it — and resumes on its next
//     tick once it is shown.
//   • imperative (e.g. streamed chat content): call Hydra.run(el) directly.
(() => {
  var hydrators = [];
  var booted = false;

  // Minimum time (ms) a boundary stays hidden before revealing, measured from when the
  // hydration pass starts. This is a FLOOR, not a race: reveal still waits for hydration to
  // fully settle; the floor only extends a very fast pass so every page reveals with the same
  // graceful fade instead of snapping. Kept in step with the CSS opacity transition in the
  // shell (#content { transition: opacity … }).
  var MIN_REVEAL_MS = 130;

  function register(name, fn, priority) {
    hydrators.push({ name: name, fn: fn, priority: priority || 0 });
    hydrators.sort((a, b) => a.priority - b.priority);
    // A hydrator registered after the first hydration pass (unusual with defer-ordered
    // scripts, but robust to it) catches up over the whole document immediately.
    if (booted) invoke({ name: name, fn: fn }, document);
  }

  function invoke(h, root) {
    return new Promise((resolve) => {
      resolve(h.fn(root));
    }).catch((e) => {
      if (window.console) console.error('hydrate:' + h.name, e);
    });
  }

  // run every hydrator over root; the returned promise settles only when ALL have settled.
  function run(root) {
    return Promise.allSettled(hydrators.map((h) => invoke(h, root || document)));
  }

  function boundaries() {
    return document.querySelectorAll('[data-hydrate]');
  }

  // boundaryFor returns the nearest hydration boundary at or above el (el itself if it is one),
  // or null when el sits outside every boundary (e.g. the persistent nav rail).
  function boundaryFor(el) {
    if (!el?.closest) return null;
    return el.matches?.('[data-hydrate]') ? el : el.closest('[data-hydrate]');
  }

  // boundariesIn returns the boundaries a swap touches: within `scope` — the scope itself plus
  // any boundaries nested inside it — or, when scope is null, every boundary in the document
  // (the safe fallback and the initial-load case).
  function boundariesIn(scope) {
    if (!scope) return Array.prototype.slice.call(boundaries());
    var list = [];
    if (scope.matches?.('[data-hydrate]')) list.push(scope);
    if (scope.querySelectorAll)
      scope.querySelectorAll('[data-hydrate]').forEach((n) => {
        list.push(n);
      });
    return list;
  }

  // resolveScope finds the boundary a swap event acts on, so ONLY that region hides/reveals —
  // a partial swap (e.g. #repo-main) never flashes the surrounding page or the nav rail. htmx
  // (v4) dispatches after:swap ON the swapped element and before:swap on the triggering element,
  // so we resolve uniformly: prefer the trigger's declared hx-target selector, else the event
  // target itself, then walk up to the nearest boundary. null (nothing matched) makes callers
  // fall back to the whole document, so a region can never get stuck hidden.
  function resolveScope(evt) {
    var el = evt?.target;
    var host = el?.closest?.('[hx-target]');
    var sel = host?.getAttribute('hx-target');
    var target = sel ? document.querySelector(sel) : el;
    return boundaryFor(target);
  }

  // reveal drops the opacity gate. The forced reflow flushes the hidden (opacity:0) state so
  // removing `hydrating` transitions 0→1 rather than snapping — deterministic, no timer.
  function reveal(el) {
    if (!el?.classList.contains('hydrating')) return;
    void el.offsetWidth;
    el.classList.remove('hydrating');
  }

  function hide(el) {
    if (el) el.classList.add('hydrating');
  }

  // runAndReveal hydrates over root, then reveals the given boundaries (default: all) once
  // hydration settles AND at least MIN_REVEAL_MS has elapsed (the floor keeps the fade consistent
  // across fast and slow pages). The floor never shortcuts hydration — it only ever delays the
  // reveal. A scoped root + reveal list means only the swapped region participates.
  function now() {
    return window.performance && performance.now ? performance.now() : Date.now();
  }
  function runAndReveal(root, revealEls) {
    var start = now();
    return run(root).then(() => {
      var wait = MIN_REVEAL_MS - (now() - start);
      var doReveal = () => {
        (revealEls || boundariesIn(null)).forEach(reveal);
      };
      if (wait > 0)
        return new Promise((res) => {
          setTimeout(res, wait);
        }).then(doReveal);
      doReveal();
    });
  }

  window.Hydra = { register: register, run: run, reveal: reveal };

  function boot() {
    booted = true;
    runAndReveal(document);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }

  // isLive reports whether a swap refreshes a live region in place: its target — resolved as
  // resolveScope resolves it, hx-target="this" being the element itself — is inside a
  // [data-live] element: a status that polls, a log that grows, an action that rewrites the region
  // it sits in. A region the reader is watching must not blink each time it refreshes, so a live
  // swap hydrates what it swapped in without the hide/reveal fade. A link that navigates away
  // from inside a live region targets a region that is not live, and fades as any navigation.
  // The swapped-in content carries data-live too, so after:swap (which fires on it) sees the same.
  function isLive(evt) {
    var el = evt?.target;
    var host = el?.closest?.('[hx-target]');
    var sel = host?.getAttribute('hx-target');
    var target = el;
    if (sel === 'this') target = host;
    else if (sel) target = document.querySelector(sel);
    return !!target?.closest?.('[data-live]');
  }

  // htmx (v4) swap lifecycle — target-aware. Only the boundary being swapped (and any boundaries
  // nested inside it) hides before the swap and reveals after, so a partial swap leaves the rest
  // of the page — the nav rail, the repo header, sibling boundaries — completely untouched.
  // before:swap fires on the trigger, after:swap on the swapped element; resolveScope handles
  // both, and an unresolvable scope degrades to the whole document (the pre-scoping behaviour).
  document.addEventListener('htmx:before:swap', (evt) => {
    if (isLive(evt)) return;
    boundariesIn(resolveScope(evt)).forEach(hide);
  });
  // A poll is a live region's request on a timer: skipped while the page is hidden.
  document.addEventListener('htmx:config:request', (evt) => {
    var el = evt.target;
    if (!document.hidden || !el?.closest?.('[data-live]')) return;
    if (/(^|,)\s*every\s/.test(el.getAttribute('hx-trigger') || '')) evt.preventDefault();
  });
  document.addEventListener('htmx:after:swap', (evt) => {
    booted = true;
    if (isLive(evt)) {
      run(evt.target);
      return;
    }
    var scope = resolveScope(evt);
    runAndReveal(scope || document, boundariesIn(scope));
  });
})();
