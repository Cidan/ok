package web

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/pages"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// codeSearchLimit bounds the lines a code search page lists.
const codeSearchLimit = 200

// codeSearch is a repository's code search (docs/hosted.md, "ok control at scale, as built"):
// main's files, as the SCM's latest index of them says.
func (s *Server) codeSearch(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	q := r.URL.Query()
	v := pages.SearchView{Chrome: chrome(repo, "main", repo.GetHead(), ""), Action: repoLink(repo.GetName()) + "/search",
		Query: strings.TrimSpace(q.Get("q")), Path: strings.TrimSpace(q.Get("path")), Regexp: q.Get("regexp") != "", CaseSensitive: q.Get("case") != ""}
	if v.Query == "" {
		pages.ServeSearch(w, r, v)
		return
	}
	resp, err := s.scm.Search.Code(r.Context(), &scmpb.CodeSearchRequest{RepositoryId: repo.GetId(), Pattern: v.Query, Regexp: v.Regexp,
		CaseSensitive: v.CaseSensitive, Path: v.Path, Limit: codeSearchLimit})
	switch status.Code(err) {
	case codes.OK:
	case codes.InvalidArgument:
		v.Error = status.Convert(err).Message()
		pages.ServeSearch(w, r, v)
		return
	case codes.Unavailable:
		v.Notice = "main's code is being indexed: search again in a minute."
		pages.ServeSearch(w, r, v)
		return
	default:
		fail(w, r, err)
		return
	}
	rev := hex.EncodeToString(resp.GetCommitId())
	if resp.GetStale() {
		v.Notice = "main has moved since its code was last indexed, at " + rev[:12] + ": these are what that index finds; the next is being built."
	}
	byPath := map[string]int{}
	for _, m := range resp.GetMatches() {
		i, ok := byPath[m.GetPath()]
		if !ok {
			i = len(v.Files)
			byPath[m.GetPath()] = i
			v.Files = append(v.Files, pages.SearchFile{Path: m.GetPath(), Href: treeHref(repo.GetName(), rev, m.GetPath())})
		}
		text := m.GetText()
		start, end := int(m.GetStart()), int(m.GetEnd())
		if start < 0 || end > len(text) || start > end {
			start, end = 0, 0
		}
		v.Files[i].Lines = append(v.Files[i].Lines, pages.SearchLine{Number: int(m.GetLine()),
			Href: treeHref(repo.GetName(), rev, m.GetPath()) + "#L" + strconv.Itoa(int(m.GetLine())), Before: text[:start], Match: text[start:end],
			After: text[end:]})
	}
	v.Truncated, v.Shown = resp.GetTruncated(), len(resp.GetMatches())
	pages.ServeSearch(w, r, v)
}
