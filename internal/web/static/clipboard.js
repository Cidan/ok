// clipboard.js — the only client behavior the clipboard component needs: copy a button's
// data-copy text to the clipboard and briefly reflect success. Like nav.js, this is one
// delegated document-level click listener (no per-element wiring, no Hydra registration), so
// it works for buttons that arrive via an htmx swap with no re-init. No framework, no build
// step — served as a static asset.
(() => {
  var RESET_MS = 1400;

  // copyText writes text to the clipboard, preferring the async Clipboard API and falling back
  // to a hidden textarea + execCommand for non-secure contexts. Returns a Promise<boolean>.
  function copyText(text) {
    if (navigator.clipboard?.writeText) {
      return navigator.clipboard.writeText(text).then(
        () => true,
        () => legacyCopy(text),
      );
    }
    return Promise.resolve(legacyCopy(text));
  }

  function legacyCopy(text) {
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.top = '-1000px';
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }

  // reflect toggles the button's copied state: data-copied="true" plus, if the consumer marked
  // an idle/done child pair, swap which is shown. A per-button timer resets it after RESET_MS.
  function reflect(btn) {
    if (btn._copyTimer) clearTimeout(btn._copyTimer);
    btn.dataset.copied = 'true';
    var idle = btn.querySelector('[data-copy-idle]');
    var done = btn.querySelector('[data-copy-done]');
    if (idle) idle.classList.add('hidden');
    if (done) done.classList.remove('hidden');
    btn._copyTimer = setTimeout(() => {
      btn.dataset.copied = 'false';
      if (idle) idle.classList.remove('hidden');
      if (done) done.classList.add('hidden');
    }, RESET_MS);
  }

  document.addEventListener('click', (e) => {
    var btn = e.target.closest('[data-copy]');
    if (!btn) return;
    e.preventDefault();
    copyText(btn.getAttribute('data-copy')).then((ok) => {
      if (ok) reflect(btn);
    });
  });
})();
