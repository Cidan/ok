// tree.js — the repository browser's file tree (browse.templ).
//
// htmx does the fetching: a folder the server did not render open carries hx-get with
// hx-trigger "toggle once", so opening it — by its chevron, or from here — fetches its rows
// into its list, inside the tree's [data-live] region (no fade). This script only keeps the tree
// in step with the pane, as a hydrator (internal/web/static/hydrate.js), so it runs before a region
// is revealed:
//
//   • after the pane (#browse-main) swaps, it selects the path the pane shows
//     ([data-browse-path]): it opens every folder above it (and the folder itself), each opening
//     fetching that folder's rows if they have not arrived, and resumes as each folder's rows
//     land, until the path's row is marked aria-current and scrolled into view;
//   • it applies the Files panel's shown/hidden state ([data-browser][data-tree]), kept in
//     localStorage, so a hidden panel stays hidden across pages and loads — set before the
//     region is revealed, so it never flashes;
//   • a click on a [data-tree-toggle] shows or hides the panel.
(() => {
  var KEY = 'ok.files';
  var pending = null; // the selection waiting on a folder's rows: { path, dir }

  function hidden() {
    try {
      return localStorage.getItem(KEY) === 'closed';
    } catch (_) {
      return false;
    }
  }

  function frames(root) {
    var list = [];
    if (root.matches?.('[data-browser]')) list.push(root);
    root.querySelectorAll?.('[data-browser]').forEach((f) => {
      list.push(f);
    });
    return list;
  }

  function tree() {
    return document.querySelector('[data-tree]');
  }

  function quoted(s) {
    return `"${CSS.escape(s)}"`;
  }

  // loaded reports whether a folder's rows are in its list.
  function loaded(details) {
    var list = details.querySelector(':scope > ul');
    return !!list && list.children.length > 0;
  }

  // select marks path's row in the tree, opening the folders down to it (and path itself, a
  // folder). A folder whose rows have not arrived stops it; it resumes when they land.
  function select(path, dir) {
    var t = tree();
    if (!t || !path) return;
    pending = { path: path, dir: dir };
    var parts = path.split('/');
    var depth = dir ? parts.length : parts.length - 1;
    for (let i = 1; i <= depth; i++) {
      const folder = t.querySelector(
        `details[data-tree-dir=${quoted(parts.slice(0, i).join('/'))}]`,
      );
      if (!folder) return;
      if (!folder.open) folder.open = true;
      if (!loaded(folder)) return;
    }
    pending = null;
    t.querySelectorAll('[data-tree-row][aria-current]').forEach((r) => {
      r.removeAttribute('aria-current');
    });
    var row = t.querySelector(`[data-tree-row][data-path=${quoted(path)}]`);
    if (!row) return;
    row.setAttribute('aria-current', 'page');
    var rows = row.getBoundingClientRect();
    var view = t.getBoundingClientRect();
    if (rows.top < view.top || rows.bottom > view.bottom) row.scrollIntoView({ block: 'nearest' });
  }

  // hydrate runs over each region the runtime hydrates: a whole page, the pane after a swap, or
  // a folder's rows as they land.
  function hydrate(root) {
    var closed = hidden();
    frames(root).forEach((f) => {
      f.dataset.tree = closed ? 'closed' : 'open';
    });
    var pane = root.matches?.('[data-browse-path]')
      ? root
      : root.querySelector?.('[data-browse-path]');
    if (pane) {
      select(pane.dataset.browsePath, pane.dataset.browseKind === 'dir');
      return;
    }
    if (pending && tree()?.contains(root)) select(pending.path, pending.dir);
  }

  window.Hydra.register('tree', hydrate);

  document.addEventListener('click', (evt) => {
    var button = evt.target.closest?.('[data-tree-toggle]');
    var frame = button?.closest('[data-browser]');
    if (!frame) return;
    var closed = frame.dataset.tree !== 'closed';
    frame.dataset.tree = closed ? 'closed' : 'open';
    try {
      localStorage.setItem(KEY, closed ? 'closed' : 'open');
    } catch (_) {}
  });
})();
