// stars.js — these pages' own: the favorite (star) toggle on the repo-finder list, with a smooth
// FLIP reorder. Clicking a row's star button optimistically flips its state, re-sorts the
// currently-rendered rows (favorites first, then most-recently-updated), animates every row
// from its old position to its new one (First-Last-Invert-Play), and POSTs the toggle to
// Gitea's star endpoint to persist it (reverting on failure). One delegated document-level
// click listener, so it survives the search box's list swaps with no re-init.
(() => {
  var DURATION = 320;
  var EASE = 'cubic-bezier(0.2, 0, 0, 1)';

  function rows(container) {
    return Array.prototype.slice.call(container.querySelectorAll(':scope > [data-repo-row]'));
  }

  // compare: starred first, then most-recently-updated first.
  function cmp(a, b) {
    var sa = a.getAttribute('data-starred') === 'true';
    var sb = b.getAttribute('data-starred') === 'true';
    if (sa !== sb) return sa ? -1 : 1;
    return (
      (parseInt(b.getAttribute('data-updated'), 10) || 0) -
      (parseInt(a.getAttribute('data-updated'), 10) || 0)
    );
  }

  // reorder re-sorts the container's rows and animates each from its previous box to its new one.
  function reorder(container) {
    var current = rows(container);
    var firstTop = {};
    current.forEach((r) => {
      firstTop[rowKey(r)] = r.getBoundingClientRect().top;
    });

    var sorted = current.slice().sort(cmp);
    var changed = sorted.some((r, i) => r !== current[i]);
    if (!changed) return;
    sorted.forEach((r) => {
      container.appendChild(r);
    });

    sorted.forEach((r) => {
      var dy = firstTop[rowKey(r)] - r.getBoundingClientRect().top;
      if (!dy) return;
      r.style.transition = 'none';
      r.style.transform = 'translateY(' + dy + 'px)';
      r.getBoundingClientRect(); // force reflow so the invert is committed before playing
      r.style.transition = 'transform ' + DURATION + 'ms ' + EASE;
      r.style.transform = '';
      window.setTimeout(() => {
        r.style.transition = '';
      }, DURATION + 30);
    });
  }

  var keySeq = 0;
  function rowKey(r) {
    if (!r._starKey) r._starKey = 'r' + ++keySeq;
    return r._starKey;
  }

  function setStarUI(btn, on) {
    btn.setAttribute('aria-pressed', on ? 'true' : 'false');
    var yes = btn.querySelector('[data-star-yes]');
    var no = btn.querySelector('[data-star-no]');
    if (yes) yes.classList.toggle('hidden', !on);
    if (no) no.classList.toggle('hidden', on);
  }

  function toggle(btn) {
    var row = btn.closest('[data-repo-row]');
    if (!row) return;
    var container = row.parentElement;
    var was = row.getAttribute('data-starred') === 'true';
    var now = !was;

    setStarUI(btn, now);
    row.setAttribute('data-starred', now ? 'true' : 'false');
    reorder(container);

    var link = row.getAttribute('data-repo-link');
    if (!link) return;
    fetch(link + '/action/' + (now ? 'star' : 'unstar'), {
      method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'same-origin',
    })
      .then((r) => {
        if (!r.ok) revert();
      })
      .catch(revert);

    function revert() {
      setStarUI(btn, was);
      row.setAttribute('data-starred', was ? 'true' : 'false');
      reorder(container);
    }
  }

  document.addEventListener('click', (e) => {
    var btn = e.target.closest('[data-star]');
    if (!btn) return;
    e.preventDefault();
    toggle(btn);
  });
})();
