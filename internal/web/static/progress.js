// progress.js — loading feedback for htmx requests the reader is waiting on.
//
// While a navigation or an action is in flight, a thin bar (#page-progress) creeps across the top
// of the window and finishes when the answer is in; and the hydration boundary the answer will
// replace (hydrate.js) is marked aria-busy="true", which the app's stylesheet may
// dim. The app styles the bar by its id (its colour, a glow); unstyled, it is the text colour.
//
// Nothing shows for a request that answers within DELAY, so a quick page never flickers; nor for
// a request the reader didn't ask for and isn't watching: a live region's refresh, a lazy load as
// something scrolls into view, a search as the reader types (anything inside [data-live], or
// triggered by intersect, load, revealed, every, live, input, keyup, or toggle). The element that
// issued a request carries htmx's own htmx-request class meanwhile, for the app to style (a
// button's spinner).
(() => {
  var DELAY = 120;
  var QUIET = /(^|[\s,])(intersect|load|revealed|every|live|input|keyup|toggle)\b/;
  var pending = new Map(); // request context → the region it marked busy
  var shown = false;
  var timer = null;
  var trickle = null;
  var hideTimer = null;
  var width = 0;
  var bar = null;

  function barEl() {
    if (bar) return bar;
    bar = document.createElement('div');
    bar.id = 'page-progress';
    bar.setAttribute('aria-hidden', 'true');
    bar.style.cssText =
      'position:fixed;top:0;left:0;height:2px;width:0;z-index:2147483000;pointer-events:none;opacity:0;' +
      'background:currentColor;transition:width 240ms ease-out,opacity 280ms ease;';
    document.body.appendChild(bar);
    return bar;
  }

  function setWidth(w) {
    width = w;
    barEl().style.width = w + '%';
  }

  function show() {
    timer = null;
    clearTimeout(hideTimer);
    shown = true;
    var b = barEl();
    b.style.transition = 'none';
    setWidth(0);
    b.style.opacity = '1';
    void b.offsetWidth;
    b.style.transition = 'width 240ms ease-out,opacity 280ms ease';
    setWidth(18);
    trickle = setInterval(() => {
      setWidth(width + (92 - width) * 0.08);
    }, 220);
  }

  function finish() {
    clearTimeout(timer);
    timer = null;
    if (!shown) return;
    shown = false;
    clearInterval(trickle);
    setWidth(100);
    hideTimer = setTimeout(() => {
      barEl().style.opacity = '0';
    }, 220);
  }

  function watched(el) {
    if (!el?.closest) return true;
    if (el.closest('[data-live]')) return false;
    return !QUIET.test(el.getAttribute('hx-trigger') || '');
  }

  // region is the hydration boundary a request's answer replaces: its target's nearest.
  function region(ctx, el) {
    var t = ctx?.target;
    if (typeof t === 'string') t = document.querySelector(t);
    if (!t?.closest) {
      const host = el?.closest?.('[hx-target]');
      const sel = host?.getAttribute('hx-target');
      t = sel && sel !== 'this' ? document.querySelector(sel) : null;
    }
    return t?.closest ? t.closest('[data-hydrate]') : null;
  }

  document.addEventListener('htmx:before:request', (evt) => {
    var ctx = evt.detail?.ctx;
    if (!ctx || !watched(evt.target)) return;
    var r = region(ctx, evt.target);
    pending.set(ctx, r);
    if (r) r.setAttribute('aria-busy', 'true');
    if (!shown && !timer) timer = setTimeout(show, DELAY);
  });

  document.addEventListener('htmx:finally:request', (evt) => {
    var ctx = evt.detail?.ctx;
    if (!ctx || !pending.has(ctx)) return;
    var r = pending.get(ctx);
    pending.delete(ctx);
    if (r && ![...pending.values()].includes(r)) r.removeAttribute('aria-busy');
    if (pending.size === 0) finish();
  });
})();
