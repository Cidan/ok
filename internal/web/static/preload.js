// Suppresses CSS transitions until webfonts are ready, so the async font swap's reflow cannot
// animate transition-* elements (the nav rail, main, the composer) as a page loads. Loaded
// synchronously at the start of <body>, before what it guards renders.
(() => {
  function go() {
    if (document.body) document.body.classList.remove('preload');
  }
  (document.fonts?.ready ?? Promise.resolve()).then(go);
  setTimeout(go, 1500);
})();
