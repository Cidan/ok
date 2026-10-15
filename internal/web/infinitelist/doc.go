// Package infinitelist is a reusable, windowed infinite-scroll container: the Scroller templ
// (see infinitelist.templ) plus the client controller (infinite.js). The server renders the
// first page of rows; the controller lazily loads more on scroll in both directions and
// unloads batches scrolled far off-screen into spacers, keeping the DOM bounded.
package infinitelist
