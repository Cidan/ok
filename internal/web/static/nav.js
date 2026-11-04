// nav.js — the only client behavior the nav needs: toggling the drawer open/closed and
// swipe-to-close on touch. State lives as data-open on the [data-nav] shell; every
// collapse-reactive style is a group-data-[open]/nav Tailwind variant, so this file only
// flips the attribute. No framework, no build step — served as a static asset.
(() => {
  function shell(el) {
    return el.closest('[data-nav]');
  }
  function isOpen(s) {
    return !!s && s.dataset.open === 'true';
  }
  function setOpen(s, open) {
    if (s) s.dataset.open = open ? 'true' : 'false';
  }

  // Close every open section-row kebab menu and drop the sticky-visible state on its row.
  function closeMenus() {
    document.querySelectorAll('[data-nav-menu]').forEach((m) => {
      m.classList.add('hidden');
    });
    document.querySelectorAll('[data-nav-kebab-wrap]').forEach((w) => {
      w.classList.remove('opacity-100');
    });
  }

  document.addEventListener('click', (e) => {
    var toggle = e.target.closest('[data-nav-toggle]');
    if (toggle) {
      const s = shell(toggle);
      setOpen(s, !isOpen(s));
      return;
    }
    var backdrop = e.target.closest('[data-nav-backdrop]');
    if (backdrop) {
      setOpen(shell(backdrop), false);
      return;
    }

    // Kebab: toggle this row's dropdown, closing any other open one; the row stays visible
    // (opacity-100) while its menu is open.
    var kebab = e.target.closest('[data-nav-kebab]');
    if (kebab) {
      e.preventDefault();
      const wrap = kebab.closest('[data-nav-kebab-wrap]');
      const menu = wrap?.querySelector('[data-nav-menu]');
      const willOpen = menu?.classList.contains('hidden');
      closeMenus();
      if (willOpen) {
        menu.classList.remove('hidden');
        wrap.classList.add('opacity-100');
      }
      return;
    }

    // Selecting a destination in the rail (a section row, an action, or a footer link) closes
    // the drawer. The link still navigates (htmx handles its own click); we only drop the open
    // state. Fires for the current destination too.
    var navLink = e.target.closest('[data-nav-surface] a');
    if (navLink) {
      setOpen(shell(navLink), false);
      closeMenus();
      return;
    }

    // Any other click (including a menu action, which the app's own script handles) closes
    // open menus.
    closeMenus();
  });

  // Swipe-left to close: a mostly-horizontal swipe of more than 40px.
  var startX = 0,
    startY = 0;
  document.addEventListener(
    'touchstart',
    (e) => {
      if (!e.target.closest('[data-nav-surface]')) return;
      startX = e.touches[0].clientX;
      startY = e.touches[0].clientY;
    },
    { passive: true },
  );
  document.addEventListener(
    'touchend',
    (e) => {
      var surface = e.target.closest('[data-nav-surface]');
      if (!surface) return;
      var dx = startX - e.changedTouches[0].clientX;
      var dy = Math.abs(startY - e.changedTouches[0].clientY);
      if (dx > 40 && dx > dy) setOpen(shell(surface), false);
    },
    { passive: true },
  );

  // updateActive re-marks the active destination on every navigation. The rail is persistent
  // (never swapped — see nav.templ), so after an htmx swap the server can't re-mark the active
  // item; we do it here from the pushed URL, mirroring Model.IsActive: a link's patterns
  // (data-nav-match, space-separated) are "/" for the root alone, a path for itself and below, a
  // path ending in "$" for itself alone, and "*" for any path no other link matches. The active link carries aria-current="page",
  // which the nav's aria-[current=page] variants style. Only [data-nav-link] links (section
  // rows, footer, TopBar items — not the Actions, unless HighlightActions) are eligible.
  function matchPath(cur, p) {
    if (!p || p === '#') return false;
    if (p === '/') return cur === '/';
    if (p.endsWith('$')) return cur === p.slice(0, -1);
    return cur === p || cur.indexOf(p + '/') === 0;
  }
  function patterns(a) {
    return (
      a.getAttribute('data-nav-match') ||
      a.getAttribute('hx-get') ||
      a.getAttribute('href') ||
      ''
    )
      .split(/\s+/)
      .filter(Boolean);
  }
  // A "*" link is current when no other link of its group — its nearest [data-nav-group] (the
  // rail, the top bar, or an app's own set of links, such as tabs) — is.
  function updateActive() {
    var cur = location.pathname;
    var groups = new Map();
    document.querySelectorAll('a[data-nav-link]').forEach((a) => {
      var g = a.closest('[data-nav-group]') || document;
      if (!groups.has(g)) groups.set(g, []);
      groups.get(g).push(a);
    });
    groups.forEach((links) => {
      var matched = links.map((a) => patterns(a).some((p) => p !== '*' && matchPath(cur, p)));
      var any = matched.some(Boolean);
      links.forEach((a, i) => {
        var on = matched[i] || (!any && patterns(a).indexOf('*') >= 0);
        if (on) a.setAttribute('aria-current', 'page');
        else a.removeAttribute('aria-current');
      });
    });
  }
  // The rail persists across content swaps, so re-mark the active item after each htmx swap
  // (htmx 4 fires htmx:after:swap once per swap, after the URL is pushed).
  document.addEventListener('htmx:after:swap', updateActive);

  // Dev/preview aid: ?nav=open opens the drawer; ?nav=menu also opens the first section
  // row's kebab dropdown (for screenshotting it without a live hover/click).
  function preview() {
    var m = /[?#&]nav=([a-z]+)/.exec(location.search + location.hash);
    if (!m) return;
    var s = document.querySelector('[data-nav]');
    setOpen(s, true);
    if (m[1] === 'menu') {
      const wrap = document.querySelector('[data-nav-kebab-wrap]');
      const menu = wrap?.querySelector('[data-nav-menu]');
      if (menu) {
        menu.classList.remove('hidden');
        wrap.classList.add('opacity-100');
      }
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', preview);
  } else {
    preview();
  }
})();
