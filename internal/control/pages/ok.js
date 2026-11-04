// ok.js — ok control's pages' own: the command palette, the keyboard shortcuts, the diff view,
// and the dashboard's greeting. Delegated document-level listeners, so every page and every swap
// is covered with no re-init.
//
//   • The palette (palette.templ): Ctrl+K (⌘K on a Mac), /, or a [data-palette-open] control opens
//     it; what the reader types is sent to /repositories/palette — with the repository they are
//     in, which the page names ([data-repo-name]) — whose results replace the list; the arrow keys
//     and the pointer choose a result, Enter or a click opens it, Escape or a click outside closes.
//   • The shortcuts: g then h, r, s, n (and c, l, y, f in a repository) follow the link carrying that
//     data-shortcut; ? shows them all; on a list of changed files, j and k go to the next and the
//     previous file, o opens or closes it, and d switches the diff between unified and split.
//     None fire while the reader is typing.
//   • The diff view: unified or split (<html data-diff>), remembered (localStorage "diff"), and
//     switched from any [data-diff-view] control or with d.
//   • A CL's files' list ([data-file-jump]): a click brings the file up, open, on the Files tab;
//     the file at the top of the view is the one it marks as the page scrolls.
//   • The greeting ([data-greeting]): good morning, afternoon, or evening, by the reader's clock.
(() => {
  var root = document.documentElement;
  var MAC = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);

  function typing(e) {
    var t = e.target;
    return !!t?.closest?.('input, textarea, select, [contenteditable="true"]');
  }

  // --- The palette ----------------------------------------------------------------------------
  var DEBOUNCE = 110;
  var seq = 0;
  var timer = null;
  var aborter = null;
  var returnFocus = null;

  function palette() {
    return document.getElementById('palette');
  }
  function field() {
    return palette()?.querySelector('[data-palette-input]');
  }
  function list() {
    return palette()?.querySelector('[data-palette-results]');
  }
  function items() {
    return Array.prototype.slice.call(list()?.querySelectorAll('[data-palette-item]') || []);
  }
  function choose(el) {
    items().forEach((i) => {
      i.setAttribute('aria-selected', i === el ? 'true' : 'false');
    });
    el?.scrollIntoView({ block: 'nearest' });
  }
  function chosen() {
    return list()?.querySelector('[data-palette-item][aria-selected="true"]');
  }
  function move(by) {
    var all = items();
    if (!all.length) return;
    var i = all.indexOf(chosen());
    choose(all[(i + by + all.length) % all.length]);
  }

  function load(q) {
    var p = palette();
    if (!p) return;
    var mine = ++seq;
    if (aborter) aborter.abort();
    aborter = new AbortController();
    var busy = p.querySelector('[data-palette-busy]');
    busy?.classList.remove('hidden');
    var repo = document.querySelector('[data-repo-name]')?.getAttribute('data-repo-name') || '';
    fetch('/repositories/palette?' + new URLSearchParams({ q: q, repo: repo }), {
      signal: aborter.signal,
      credentials: 'same-origin',
    })
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error('palette: ' + r.status))))
      .then((html) => {
        if (mine !== seq) return;
        var box = list();
        box.innerHTML = html;
        if (window.htmx) window.htmx.process(box);
        modKeys(box);
        choose(items()[0]);
      })
      .catch(() => {})
      .finally(() => {
        if (mine === seq) busy?.classList.add('hidden');
      });
  }

  function openPalette() {
    var p = palette();
    if (!p?.hidden) return;
    closeShortcuts();
    returnFocus = document.activeElement;
    p.hidden = false;
    var f = field();
    f.value = '';
    load('');
    f.focus();
  }
  function closePalette() {
    var p = palette();
    if (!p || p.hidden) return;
    p.hidden = true;
    if (aborter) aborter.abort();
    if (returnFocus?.focus && document.contains(returnFocus)) returnFocus.focus();
    returnFocus = null;
  }

  document.addEventListener('input', (e) => {
    if (!e.target.matches?.('[data-palette-input]')) return;
    clearTimeout(timer);
    var q = e.target.value.trim();
    timer = setTimeout(() => {
      load(q);
    }, DEBOUNCE);
  });

  document.addEventListener('mousemove', (e) => {
    var it = e.target.closest?.('[data-palette-item]');
    if (it && it.getAttribute('aria-selected') !== 'true') choose(it);
  });

  // --- The shortcuts dialog ----------------------------------------------------------------------
  function shortcuts() {
    return document.getElementById('shortcuts');
  }
  function openShortcuts() {
    closePalette();
    var s = shortcuts();
    if (s) s.hidden = false;
  }
  function closeShortcuts() {
    var s = shortcuts();
    if (s) s.hidden = true;
  }

  // --- The diff view --------------------------------------------------------------------------
  function setDiff(view) {
    root.dataset.diff = view === 'split' ? 'split' : 'unified';
    try {
      localStorage.setItem('diff', root.dataset.diff);
    } catch {}
  }
  try {
    root.dataset.diff = localStorage.getItem('diff') === 'split' ? 'split' : 'unified';
  } catch {
    root.dataset.diff = 'unified';
  }

  // --- Files: j, k, o -------------------------------------------------------------------------
  // The changed files a reader can step through: the shown tab's, or a commit's.
  function files() {
    var scope = document.querySelector('[data-tab-panel="files"]:not([hidden])') || document;
    return Array.prototype.slice
      .call(scope.querySelectorAll('details[id^="diff-"]'))
      .filter((d) => d.getClientRects().length);
  }
  // stepFile moves to the next (by 1) or previous (by -1) file from the one at the top of the view.
  function stepFile(by) {
    var all = files();
    if (!all.length) return;
    // Where a file brought up comes to rest: its scroll container's top, less its scroll margin.
    // A file within a few pixels of it is the one the reader is at.
    var rest =
      (all[0].closest('[data-hydrate]')?.getBoundingClientRect().top || 0) +
      (parseFloat(getComputedStyle(all[0]).scrollMarginTop) || 0);
    var target;
    if (by > 0)
      target = all.find((d) => d.getBoundingClientRect().top > rest + 4) || all[all.length - 1];
    else
      target =
        all
          .slice()
          .reverse()
          .find((d) => d.getBoundingClientRect().top < rest - 4) || all[0];
    bringUp(target);
  }

  // bringUp scrolls a file to the top, marks it the reader's, and focuses it for o.
  function bringUp(d) {
    files().forEach((f) => {
      f.toggleAttribute('data-current', f === d);
    });
    d.scrollIntoView({ block: 'start', behavior: 'smooth' });
    d.querySelector('summary')?.focus({ preventScroll: true });
    markNav(d.id);
  }

  // jumpTo is a click in a CL's files' list: the file, on the Files tab, open.
  function jumpTo(id) {
    var d = document.getElementById(id);
    if (!d) return;
    if (d.closest('[data-tab-panel]')?.hidden)
      document.querySelector('[data-tab="files"]')?.click();
    d.open = true;
    bringUp(d);
  }

  // markNav marks the files' list's entry for the file the reader is at.
  function markNav(id) {
    document.querySelectorAll('[data-file-jump]').forEach((a) => {
      a.setAttribute('aria-current', a.getAttribute('data-file-jump') === id ? 'true' : 'false');
    });
  }

  // As the page scrolls, the file at the top of the view is the one the list marks.
  var spying = false;
  document.addEventListener(
    'scroll',
    () => {
      if (spying || !document.querySelector('[data-file-jump]')) return;
      spying = true;
      requestAnimationFrame(() => {
        spying = false;
        var all = files();
        if (!all.length) return;
        var line =
          (all[0].closest('[data-hydrate]')?.getBoundingClientRect().top || 0) +
          (parseFloat(getComputedStyle(all[0]).scrollMarginTop) || 0) +
          16;
        var at = all.filter((d) => d.getBoundingClientRect().top <= line).pop() || all[0];
        markNav(at.id);
      });
    },
    true,
  );
  function toggleFile() {
    var d =
      document.activeElement?.closest?.('details[id^="diff-"]') ||
      document.querySelector('details[id^="diff-"][data-current]');
    if (d) d.open = !d.open;
  }

  // --- Keys -----------------------------------------------------------------------------------
  var pendingG = 0;

  function follow(keys) {
    var link = Array.prototype.find.call(
      document.querySelectorAll('[data-shortcut="' + keys + '"]'),
      (a) => a.getClientRects().length,
    );
    if (link) link.click();
  }

  document.addEventListener('keydown', (e) => {
    var p = palette();
    var inPalette = p && !p.hidden;

    if ((e.metaKey || e.ctrlKey) && !e.altKey && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      if (inPalette) closePalette();
      else openPalette();
      return;
    }
    if (inPalette) {
      if (e.key === 'Escape') {
        e.preventDefault();
        closePalette();
      } else if (e.key === 'ArrowDown' || (e.ctrlKey && e.key === 'n')) {
        e.preventDefault();
        move(1);
      } else if (e.key === 'ArrowUp' || (e.ctrlKey && e.key === 'p')) {
        e.preventDefault();
        move(-1);
      } else if (e.key === 'Enter' && chosen()) {
        e.preventDefault();
        chosen().click();
      }
      return;
    }
    var s = shortcuts();
    if (s && !s.hidden) {
      if (e.key === 'Escape' || e.key === '?') {
        e.preventDefault();
        closeShortcuts();
      }
      return;
    }
    if (typing(e) || e.metaKey || e.ctrlKey || e.altKey || e.isComposing) return;

    if (pendingG && Date.now() - pendingG < 1200) {
      pendingG = 0;
      if (/^[a-z]$/.test(e.key)) {
        e.preventDefault();
        follow('g ' + e.key);
      }
      return;
    }
    switch (e.key) {
      case 'g':
        pendingG = Date.now();
        return;
      case '/':
        e.preventDefault();
        openPalette();
        return;
      case '?':
        e.preventDefault();
        openShortcuts();
        return;
      case 'j':
        stepFile(1);
        return;
      case 'k':
        stepFile(-1);
        return;
      case 'o':
        toggleFile();
        return;
      case 'd':
        if (files().length) setDiff(root.dataset.diff === 'split' ? 'unified' : 'split');
        return;
    }
  });

  document.addEventListener('click', (e) => {
    var t = e.target;
    if (t.closest('[data-palette-open]')) {
      e.preventDefault();
      openPalette();
      return;
    }
    if (t.closest('[data-shortcuts-open]')) {
      e.preventDefault();
      openShortcuts();
      return;
    }
    if (t.closest('[data-palette-close]')) {
      closePalette();
      return;
    }
    if (t.closest('[data-shortcuts-close]')) {
      closeShortcuts();
      return;
    }
    // A result was opened (a link navigates; a theme command is theme.js's): the palette's done.
    if (t.closest('[data-palette-item]')) {
      closePalette();
      return;
    }
    var jump = t.closest('[data-file-jump]');
    if (jump) {
      e.preventDefault();
      jumpTo(jump.getAttribute('data-file-jump'));
      return;
    }
    var view = t.closest('[data-diff-view]');
    if (view) {
      e.preventDefault();
      setDiff(view.getAttribute('data-diff-view'));
    }
  });

  // --- On every page and swap: the greeting, a Mac's keys ----------------------------------------
  function greet(scope) {
    scope.querySelectorAll?.('[data-greeting]').forEach((h) => {
      var hour = new Date().getHours();
      var part =
        hour < 5 || hour >= 18 ? 'Good evening' : hour < 12 ? 'Good morning' : 'Good afternoon';
      var name = h.getAttribute('data-name');
      h.textContent = name ? part + ', ' + name : part;
    });
  }
  function modKeys(scope) {
    if (!MAC) return;
    scope.querySelectorAll?.('kbd, [data-mod]').forEach((k) => {
      if (k.textContent.trim() === 'Ctrl') k.textContent = '⌘';
      else if (k.hasAttribute('data-mod')) k.textContent = k.textContent.replace('Ctrl', '⌘');
    });
  }
  function hydrate(scope) {
    greet(scope);
    modKeys(scope);
  }
  if (window.Hydra) window.Hydra.register('ok', hydrate, 50);
  else hydrate(document);
  // The overlay and the top bar are outside every swapped region: their keys once.
  if (document.readyState === 'loading')
    document.addEventListener('DOMContentLoaded', () => {
      modKeys(document);
    });
  else modKeys(document);

  // A navigation closes the palette and the shortcuts, wherever it came from.
  document.addEventListener('htmx:before:request', (e) => {
    if (!e.target?.closest?.('[data-live]')) {
      closePalette();
      closeShortcuts();
    }
  });
})();
