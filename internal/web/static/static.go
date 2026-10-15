// Package static is the assets every page of ok's web UI shares, served under /assets/: the
// hydration runtime, the navigation rail's and components' scripts, the fonts' stylesheet, and the
// favicon — and, from third_party (upstream's files, byte-identical, with their licenses; see
// third_party/UPSTREAM), htmx and IBM Plex Sans.
package static

import (
	"embed"
	"errors"
	"io/fs"
	"net/http"
)

//go:embed *.js *.css *.svg third_party
var files embed.FS

// assets is the one tree the pages see: the package's own files, then third_party's beside them.
type assets struct{}

func (assets) Open(name string) (fs.File, error) {
	f, err := files.Open(name)
	if errors.Is(err, fs.ErrNotExist) && name != "." {
		return files.Open("third_party/" + name)
	}
	return f, err
}

// Handler serves the assets, for mounting at /assets/ with the prefix stripped. Their URLs do not
// change from one build to the next, so a browser revalidates them each time (no-cache).
func Handler() http.Handler {
	fs := http.FileServerFS(assets{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fs.ServeHTTP(w, r)
	})
}
