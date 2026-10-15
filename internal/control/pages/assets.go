package pages

import _ "embed"

// app.css is the Tailwind output over the pages and the shared components (scripts/css.sh). The
// scripts are the pages' own: the favorite toggle and reorder of the repository list (stars.js),
// a ChangeList's review tabs and inline comment composer (clcomments.js), and the repository
// browser's file tree (tree.js). Everything else under /assets/ is internal/web/static's.

//go:embed app.css
var appCSS []byte

//go:embed stars.js
var starsJS []byte

//go:embed clcomments.js
var clcommentsJS []byte

//go:embed tree.js
var treeJS []byte
