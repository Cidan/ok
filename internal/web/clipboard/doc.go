// Package clipboard is a reusable, application-neutral copy-to-clipboard control: the Button
// templ (see clipboard.templ) plus the client controller (clipboard.js). The button carries
// the text to copy; the controller — one delegated document-level click listener — writes it to
// the clipboard and briefly reflects success, swapping an optional idle/done child pair.
package clipboard
