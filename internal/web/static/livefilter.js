// livefilter.js — debounced live filtering of a server-rendered list region, app-neutral and
// paired with infinitelist. An <input data-filter> with data-filter-url and data-filter-target
// debounces on input, fetches the URL with the input's name=value appended (raw fetch, NOT
// htmx — so it never triggers the shell's hydration fade or flickers the input you're typing
// in), replaces the target region's innerHTML with the response, and re-hydrates the target via
// window.Hydra.run so a swapped-in infinite-scroll list re-initialises. The consumer supplies
// the input, the fragment endpoint (returning the list region markup), and the target selector.
(() => {
  var DELAY = 250;

  function wire(input) {
    if (input._filterReady) return;
    input._filterReady = true;
    var timer = null;
    input.addEventListener('input', () => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(run, DELAY);
    });
    function run() {
      var base = input.getAttribute('data-filter-url');
      var sel = input.getAttribute('data-filter-target');
      var target = sel && document.querySelector(sel);
      if (!base || !target) return;
      var name = input.getAttribute('name') || 'q';
      var u =
        base +
        (base.indexOf('?') >= 0 ? '&' : '?') +
        encodeURIComponent(name) +
        '=' +
        encodeURIComponent(input.value);
      fetch(u, { headers: { 'X-Filter': '1' }, credentials: 'same-origin' })
        .then((r) => (r.ok ? r.text() : ''))
        .then((html) => {
          target.innerHTML = html;
          if (window.Hydra) window.Hydra.run(target);
        })
        .catch(() => {});
    }
  }

  function initAll(root) {
    (root || document).querySelectorAll('input[data-filter]').forEach(wire);
  }

  if (window.Hydra) {
    window.Hydra.register('livefilter', initAll, 30);
  } else if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initAll(document);
    });
  } else {
    initAll(document);
  }
})();
