// Picks a themed page set's theme before the first paint (layout.Props.Themes; docs/web.md, "Theme
// tokens"), so a page never flashes in the other one: the reader's choice — localStorage "theme":
// light, dark, or system (unset) — or, for system, the system's. It marks <html> with data-theme,
// the theme shown, and data-theme-pref, the choice, and gives the browser chrome (meta
// theme-color) the theme's canvas. The shell loads it synchronously after the stylesheets, which
// is what reading the canvas waits on; theme.js keeps the theme right afterwards.
(() => {
  var d = document.documentElement;
  var pref = 'system';
  try {
    pref = localStorage.getItem('theme') || 'system';
  } catch {}
  var dark =
    pref === 'dark' || (pref !== 'light' && !matchMedia('(prefers-color-scheme: light)').matches);
  d.dataset.themePref = pref;
  d.dataset.theme = dark ? 'dark' : 'light';
  var canvas = getComputedStyle(d).getPropertyValue('--color-canvas').trim();
  var meta = document.querySelector('meta[name="theme-color"]');
  if (canvas && meta) meta.setAttribute('content', canvas);
})();
