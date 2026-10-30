// live.js — live regions refreshed as what they show changes, not on a timer (docs/web.md, "Live
// regions"; docs/state.md, "Waiting"). An element with data-live-events names a stream of
// server-sent events: its page's — a CL's, a run's, a deployment's, the services'. Each "change"
// event dispatches a "live" event on every element that names the stream, whose hx-trigger="live"
// fetches it again as a timer did. One EventSource per stream, however many regions name it,
// closed once none does. A change that comes while the stream's regions are being fetched fetches
// them once more after; one that comes while the page is hidden, once the page is shown.
(() => {
  // A fetch that never ends frees its stream after this long.
  var STUCK = 15000;
  var streams = {}; // url → { es, busy, again, timer }
  var hidden = {}; // urls changed while the page was hidden

  function regions(url) {
    return document.querySelectorAll('[data-live-events="' + CSS.escape(url) + '"]');
  }

  function refresh(url) {
    var s = streams[url];
    if (!s) return;
    if (document.hidden) {
      hidden[url] = true;
      return;
    }
    if (s.busy) {
      s.again = true;
      return;
    }
    var els = regions(url);
    if (els.length === 0) {
      close(url);
      return;
    }
    s.busy = true;
    s.timer = setTimeout(() => {
      done(url);
    }, STUCK);
    els.forEach((el) => {
      el.dispatchEvent(new CustomEvent('live'));
    });
  }

  function done(url) {
    var s = streams[url];
    if (!s?.busy) return;
    clearTimeout(s.timer);
    s.busy = false;
    if (s.again) {
      s.again = false;
      refresh(url);
    }
  }

  function open(url) {
    if (streams[url]) return;
    var s = { es: new EventSource(url), busy: false, again: false, timer: null };
    streams[url] = s;
    s.es.addEventListener('change', () => {
      refresh(url);
    });
  }

  function close(url) {
    var s = streams[url];
    if (!s) return;
    clearTimeout(s.timer);
    s.es.close();
    delete streams[url];
  }

  function initAll(root) {
    var scope = root || document;
    if (scope.matches?.('[data-live-events]')) open(scope.getAttribute('data-live-events'));
    scope.querySelectorAll('[data-live-events]').forEach((el) => {
      open(el.getAttribute('data-live-events'));
    });
    Object.keys(streams).forEach((url) => {
      if (regions(url).length === 0) close(url);
    });
  }

  // A region's fetch is done once its answer is swapped in: htmx (v4) fires after:swap on the
  // element swapped in. Not after:request, which comes before the swap — a fetch begun then would
  // go from the element about to be replaced, and be dropped with it. A region swapped for one no
  // longer live leaves its stream to close.
  document.addEventListener('htmx:after:swap', (evt) => {
    var url = evt.target?.getAttribute?.('data-live-events');
    if (url) done(url);
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    var urls = Object.keys(hidden);
    hidden = {};
    urls.forEach(refresh);
  });

  if (window.Hydra) {
    window.Hydra.register('live', initAll, 40);
  } else if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initAll(document);
    });
  } else {
    initAll(document);
  }
})();
