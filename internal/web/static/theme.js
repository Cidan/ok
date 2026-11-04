// theme.js — switches a themed page set between its light and dark themes (layout.Props.Themes;
// docs/web.md, "Theme tokens").
//
// The shell picked the theme before the first paint (theme-init.js), from the reader's choice — localStorage
// "theme": light, dark, or system (unset) — and marked <html> with it: data-theme, the theme
// shown, and data-theme-pref, the choice. This keeps them right afterwards:
//
//   • a [data-theme-toggle] control (the nav's theme control) cycles the choice — system, light,
//     dark — and a [data-theme-set="<choice>"] control (a palette's command) makes one; either
//     remembers it and applies it, and a change of theme grows out of the control as a circle,
//     where the browser has view transitions and the reader has not asked for less motion;
//   • while the choice is the system's, the theme follows the system as it changes;
//   • a choice made in another tab applies here too;
//   • the browser chrome (meta theme-color) takes the theme's canvas, --color-canvas.
(() => {
  var root = document.documentElement;
  var KEY = 'theme';
  var ORDER = ['system', 'light', 'dark'];
  var NAMES = { system: 'System Theme', light: 'Light Theme', dark: 'Dark Theme' };
  var light = window.matchMedia('(prefers-color-scheme: light)');
  var reduced = window.matchMedia('(prefers-reduced-motion: reduce)');

  function pref() {
    var p = root.dataset.themePref;
    return ORDER.indexOf(p) >= 0 ? p : 'system';
  }

  function resolve(p) {
    if (p === 'system') return light.matches ? 'light' : 'dark';
    return p;
  }

  function sync() {
    var c = getComputedStyle(root).getPropertyValue('--color-canvas').trim();
    var m = document.querySelector('meta[name="theme-color"]');
    if (c && m) m.setAttribute('content', c);
    var p = pref();
    document.querySelectorAll('[data-theme-toggle]').forEach((b) => {
      b.title = NAMES[p] + ' — switch to ' + NAMES[ORDER[(ORDER.indexOf(p) + 1) % ORDER.length]];
    });
  }

  function apply(p) {
    root.dataset.themePref = p;
    root.dataset.theme = resolve(p);
    sync();
  }

  // The circle reveal replaces the view transition's default cross-fade (shell.css turns the
  // default off): the new theme is drawn over the old one, growing from the control.
  function choose(p, x, y) {
    try {
      if (p === 'system') localStorage.removeItem(KEY);
      else localStorage.setItem(KEY, p);
    } catch {}
    if (resolve(p) === root.dataset.theme || !document.startViewTransition || reduced.matches) {
      apply(p);
      return;
    }
    var r = Math.hypot(Math.max(x, innerWidth - x), Math.max(y, innerHeight - y));
    var t = document.startViewTransition(() => {
      apply(p);
    });
    t.ready
      .then(() => {
        root.animate(
          {
            clipPath: [
              'circle(0px at ' + x + 'px ' + y + 'px)',
              'circle(' + r + 'px at ' + x + 'px ' + y + 'px)',
            ],
          },
          {
            duration: 520,
            easing: 'cubic-bezier(0.22, 1, 0.36, 1)',
            pseudoElement: '::view-transition-new(root)',
          },
        );
      })
      .catch(() => {});
  }

  document.addEventListener('click', (e) => {
    var b = e.target.closest('[data-theme-toggle], [data-theme-set]');
    if (!b) return;
    e.preventDefault();
    var x = e.clientX,
      y = e.clientY;
    if (e.detail === 0) {
      // Keyboard activation carries no pointer position: grow from the control's centre.
      const r = b.getBoundingClientRect();
      x = r.left + r.width / 2;
      y = r.top + r.height / 2;
    }
    var set = b.getAttribute('data-theme-set');
    var next = ORDER.indexOf(set) >= 0 ? set : ORDER[(ORDER.indexOf(pref()) + 1) % ORDER.length];
    choose(next, x, y);
  });

  light.addEventListener('change', () => {
    if (pref() === 'system') apply('system');
  });

  window.addEventListener('storage', (e) => {
    if (e.key === KEY) apply(e.newValue || 'system');
  });

  if (!root.dataset.theme) apply(pref());
  else sync();
})();
