// clcomments.js — these pages' own: ChangeList review interactions.
//
// Five concerns, all handled by delegated document-level listeners so they survive htmx's
// #repo-main innerHTML swaps with no re-init (same approach as stars.js):
//
//   1. The Files / Comments / Checks / Deployments tab bar — clicking a [data-tab] button
//      activates it and shows the sibling [data-tab-panel] with the matching name, hiding the
//      others.
//   2. The inline diff comment composer — clicking the per-line "+" (`.cl-comment-add`) clones
//      the page's <template id="cl-composer-template"> into a full-width row directly below the
//      clicked diff line, stamps the file/line/side the "+" carries into the form's hidden
//      fields, and lets htmx POST it (the form's hx-post re-renders #repo-main with the new
//      thread anchored in place). Replies and resolve/unresolve are plain server-rendered htmx
//      forms and need no JS.
//   3. Pointer popovers — clicking a [data-popover-open="<id>"] trigger shows the [data-popover]
//      element with that id at the pointer (below the trigger when activated from the keyboard),
//      clamped inside the viewport. An outside click, Escape, or an htmx swap closes it.
//   4. The review dialog's reviewer picker — clicking a live-search result ([data-reviewer-pick])
//      checks that person: their existing checkbox if the dialog already offers them, otherwise a
//      new checked reviewer row in [data-reviewer-picked]. Checkboxes for the same person (offered
//      for several owner sets) toggle together, and Enter in the search box never submits.
//   5. The Checks tab's groups — each [data-check-group] <details> keeps whether the reader opened
//      or closed it across the tab's refreshes, which re-render it as the server would: a hydrator
//      (internal/web/static/hydrate.js) puts the reader's choice back before the refreshed tab is shown.
(() => {
  // --- Pointer popovers -------------------------------------------------------------------
  var MARGIN = 8;

  function closePopovers(except) {
    document.querySelectorAll('[data-popover]').forEach((p) => {
      if (p !== except) p.hidden = true;
    });
  }

  function openPopover(trigger, e) {
    var pop = document.getElementById(trigger.getAttribute('data-popover-open'));
    if (!pop) return;
    if (!pop.hidden) {
      pop.hidden = true;
      return;
    }
    closePopovers(pop);
    var x = e.clientX,
      y = e.clientY;
    if (e.detail === 0) {
      // Keyboard activation carries no pointer position: anchor under the trigger.
      const r = trigger.getBoundingClientRect();
      x = r.left;
      y = r.bottom;
    }
    pop.hidden = false;
    var w = pop.offsetWidth,
      h = pop.offsetHeight;
    var left = Math.min(x, window.innerWidth - w - MARGIN);
    var top = y + MARGIN;
    if (top + h > window.innerHeight - MARGIN) top = Math.max(MARGIN, y - h - MARGIN);
    pop.style.left = Math.max(MARGIN, left) + 'px';
    pop.style.top = top + 'px';
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closePopovers(null);
  });
  document.addEventListener('htmx:before:swap', () => {
    closePopovers(null);
  });

  // --- Check groups -----------------------------------------------------------------------
  // The reader's choices, by page and group: a page's groups are its own.
  var checkGroups = new Map();

  function checkGroupKey(el) {
    return location.pathname + '|' + el.getAttribute('data-check-group');
  }

  // toggle does not bubble: listen in the capture phase. A hydrator setting .open fires it too,
  // which records the same choice again.
  document.addEventListener(
    'toggle',
    (e) => {
      var el = e.target;
      if (el.matches?.('[data-check-group]')) checkGroups.set(checkGroupKey(el), el.open);
    },
    true,
  );

  window.Hydra.register('check-groups', (root) => {
    root.querySelectorAll?.('[data-check-group]').forEach((el) => {
      var open = checkGroups.get(checkGroupKey(el));
      if (open !== undefined && el.open !== open) el.open = open;
    });
  });

  // --- Tabs -------------------------------------------------------------------------------
  function activateTab(btn) {
    var bar = btn.closest('[data-tabs]');
    if (!bar) return;
    var name = btn.getAttribute('data-tab');
    bar.querySelectorAll('[data-tab]').forEach((b) => {
      b.classList.toggle('cl-tab-active', b === btn);
    });
    // Panels are siblings of the tab bar within the CL main container.
    var scope = bar.parentElement;
    scope.querySelectorAll('[data-tab-panel]').forEach((p) => {
      p.hidden = p.getAttribute('data-tab-panel') !== name;
    });
  }

  // --- Inline comment composer ------------------------------------------------------------
  function openComposer(btn) {
    var row = btn.closest('tr');
    if (!row) return;

    // If a composer is already open right below this line, just focus it.
    var next = row.nextElementSibling;
    if (next?.classList.contains('cl-composer-row')) {
      const existing = next.querySelector('textarea');
      if (existing) existing.focus();
      return;
    }

    var tmpl = document.getElementById('cl-composer-template');
    if (!tmpl?.content) return;
    // The template wraps the row in a <table> so the <tr> survives HTML parsing; clone the row.
    var tr = tmpl.content.querySelector('tr.cl-composer-row');
    if (!tr) return;
    var node = tr.cloneNode(true);
    setField(node, 'file', btn.getAttribute('data-file'));
    setField(node, 'line', btn.getAttribute('data-line'));
    setField(node, 'side', btn.getAttribute('data-side'));

    row.after(node);
    if (window.htmx) window.htmx.process(node);
    var ta = node.querySelector('textarea');
    if (ta) ta.focus();
  }

  function setField(root, name, value) {
    var el = root.querySelector('[name="' + name + '"]');
    if (el) el.value = value == null ? '' : value;
  }

  // --- Reviewer picker ---------------------------------------------------------------------
  function reviewerBoxes(form, id) {
    return form.querySelectorAll('input[data-reviewer-id="' + CSS.escape(id) + '"]');
  }

  function pickReviewer(btn) {
    var form = btn.closest('form');
    if (!form) return;
    var id = btn.getAttribute('data-id');
    var boxes = reviewerBoxes(form, id);
    if (boxes.length) {
      boxes.forEach((b) => {
        b.checked = true;
      });
    } else {
      const picked = form.querySelector('[data-reviewer-picked]');
      if (!picked) return;
      const label = document.createElement('label');
      label.className =
        'flex cursor-pointer items-center gap-2.5 rounded-lg px-2 py-1.5 text-sm text-zinc-300 transition-colors hover:bg-zinc-900';
      const box = document.createElement('input');
      box.type = 'checkbox';
      box.name = 'reviewer';
      box.value = id;
      box.checked = true;
      box.setAttribute('data-reviewer-id', id);
      box.className = 'h-4 w-4 cursor-pointer accent-sky-500';
      const name = document.createElement('span');
      name.textContent = btn.getAttribute('data-name');
      label.append(box, name);
      picked.append(label);
    }
    var search = form.querySelector('[data-reviewer-search]');
    if (search) {
      search.value = '';
      search.focus();
    }
    var results = form.querySelector('[data-reviewer-results]');
    if (results) results.replaceChildren();
  }

  document.addEventListener('change', (e) => {
    var box = e.target.closest('input[data-reviewer-id]');
    if (!box) return;
    var form = box.closest('form');
    if (!form) return;
    reviewerBoxes(form, box.getAttribute('data-reviewer-id')).forEach((b) => {
      b.checked = box.checked;
    });
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && e.target.closest('[data-reviewer-search]')) e.preventDefault();
  });

  document.addEventListener('click', (e) => {
    var pick = e.target.closest('[data-reviewer-pick]');
    if (pick) {
      e.preventDefault();
      pickReviewer(pick);
      return;
    }
    var popTrigger = e.target.closest('[data-popover-open]');
    if (popTrigger) {
      e.preventDefault();
      openPopover(popTrigger, e);
      return;
    }
    if (!e.target.closest('[data-popover]')) closePopovers(null);

    var tab = e.target.closest('[data-tab]');
    if (tab) {
      e.preventDefault();
      activateTab(tab);
      return;
    }
    var add = e.target.closest('.cl-comment-add');
    if (add) {
      e.preventDefault();
      openComposer(add);
      return;
    }
    var cancel = e.target.closest('.cl-composer-cancel');
    if (cancel) {
      e.preventDefault();
      const row = cancel.closest('.cl-composer-row');
      if (row) row.remove();
    }
  });
})();
