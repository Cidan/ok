// infinite.js — the client controller for the windowed infinite-scroll container
// (internal/web/infinitelist). It keeps a BOUNDED window of item batches in the DOM:
//
//   • Scrolling toward the bottom loads the next page and appends it; scrolling toward the
//     top (after batches were unloaded) loads the previous page and prepends it.
//   • When the loaded window exceeds MAX_PAGES, the batch furthest from the viewport is
//     removed from the DOM and its measured height is added to a spacer <div> on that side.
//     The spacer keeps scrollHeight (and the scrollbar) honest and the viewport stable, and
//     scrolling back re-reveals the sentinel, which reloads that page and shrinks the spacer.
//
// So the DOM never grows past ~MAX_PAGES * pageSize rows in either direction. The row markup
// and the ?page=N fragment endpoint are supplied by the consumer; this file is app-neutral.
(() => {
  var MAX_PAGES = 4;

  function initAll(root) {
    (root || document).querySelectorAll('[data-infinite]').forEach(init);
  }

  function init(el) {
    if (el._infiniteReady) return;
    el._infiniteReady = true;

    var url = el.getAttribute('data-infinite-url');
    var pageSize = parseInt(el.getAttribute('data-infinite-page-size'), 10) || 20;
    var rows = el.querySelector('[data-infinite-rows]');
    var topSpacer = el.querySelector('[data-infinite-top-spacer]');
    var botSpacer = el.querySelector('[data-infinite-bottom-spacer]');
    var topSentinel = el.querySelector('[data-infinite-sentinel="top"]');
    var botSentinel = el.querySelector('[data-infinite-sentinel="bottom"]');
    if (!rows || !topSpacer || !botSpacer || !topSentinel || !botSentinel) return;

    var first = 1,
      last = 1;
    var hasMoreDown = el.getAttribute('data-infinite-has-more') === 'true';
    var loading = false;

    function heightOf(nodes) {
      var h = 0;
      for (let i = 0; i < nodes.length; i++) h += nodes[i].offsetHeight;
      return h;
    }
    function childRows() {
      return Array.prototype.slice.call(rows.children);
    }

    function parseRows(html) {
      var tpl = document.createElement('template');
      tpl.innerHTML = (html || '').trim();
      return Array.prototype.slice.call(tpl.content.children);
    }
    function fetchPage(n) {
      var u = url + (url.indexOf('?') >= 0 ? '&' : '?') + 'page=' + n;
      return fetch(u, { headers: { 'X-Infinite': '1' }, credentials: 'same-origin' })
        .then((r) => (r.ok ? r.text() : ''))
        .then(parseRows);
    }

    // Drop the top batch (furthest above the viewport) into the top spacer.
    function trimTop() {
      while (last - first + 1 > MAX_PAGES) {
        const kids = childRows().slice(0, pageSize);
        if (!kids.length) break;
        topSpacer.style.height = topSpacer.offsetHeight + heightOf(kids) + 'px';
        kids.forEach((n) => {
          n.remove();
        });
        first++;
      }
    }
    // Drop the bottom batch (furthest below the viewport) into the bottom spacer.
    function trimBottom() {
      while (last - first + 1 > MAX_PAGES) {
        const all = childRows();
        const kids = all.slice(Math.max(0, all.length - pageSize));
        if (!kids.length) break;
        botSpacer.style.height = botSpacer.offsetHeight + heightOf(kids) + 'px';
        kids.forEach((n) => {
          n.remove();
        });
        last--;
        hasMoreDown = true;
      }
    }

    function loadNext() {
      if (loading || !hasMoreDown) return;
      loading = true;
      fetchPage(last + 1)
        .then((newRows) => {
          if (newRows.length) {
            newRows.forEach((n) => {
              rows.appendChild(n);
            });
            last++;
            hasMoreDown = newRows.length >= pageSize;
            trimTop(); // appended below the viewport; trimming above keeps position stable
          } else {
            hasMoreDown = false;
          }
          loading = false;
        })
        .catch(() => {
          loading = false;
        });
    }

    function loadPrev() {
      if (loading || first <= 1) return;
      loading = true;
      var prev = first - 1;
      fetchPage(prev)
        .then((newRows) => {
          if (newRows.length) {
            const frag = document.createDocumentFragment();
            newRows.forEach((n) => {
              frag.appendChild(n);
            });
            rows.insertBefore(frag, rows.firstChild);
            // Prepending shifts content down; absorb it by shrinking the top spacer, and if the
            // spacer can't cover it all, nudge scrollTop so the viewport doesn't jump.
            const addedH = heightOf(newRows);
            const reduce = Math.min(topSpacer.offsetHeight, addedH);
            topSpacer.style.height = topSpacer.offsetHeight - reduce + 'px';
            if (addedH > reduce) el.scrollTop += addedH - reduce;
            first = prev;
            trimBottom();
          }
          loading = false;
        })
        .catch(() => {
          loading = false;
        });
    }

    var io = new IntersectionObserver(
      (entries) => {
        entries.forEach((e) => {
          if (!e.isIntersecting) return;
          if (e.target === botSentinel) loadNext();
          else if (e.target === topSentinel) loadPrev();
        });
      },
      { root: el, rootMargin: '400px 0px' },
    );
    io.observe(botSentinel);
    io.observe(topSentinel);
  }

  if (window.Hydra) {
    window.Hydra.register('infinite', initAll, 40);
  } else if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initAll(document);
    });
  } else {
    initAll(document);
  }
})();
