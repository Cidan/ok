package review

import (
	"context"
	"strconv"

	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/wait"
)

// FeedSubject is what the change feed applying a page of a repository's events changes
// (internal/wait): what an action's WaitForFeed waits on.
func FeedSubject(repo int64) string { return "feed/" + strconv.FormatInt(repo, 10) }

// WaitForFeed waits until the change feed has applied a repository's event seq (its place in the
// repository's log), so a page rendered after an action shows it: registered on the repository's
// feed, it reads the feed's cursor, again as the feed applies each page of the repository's events;
// after feedWait it gives up, and the page renders from the projection the action already updated.
func (s *Service) WaitForFeed(ctx context.Context, repo, seq int64) error {
	if seq == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.feedWait)
	defer cancel()
	w, err := wait.Open(ctx, s.db.Waits().Jobs().Queue, wait.Reg{Store: s.db.Waits(), Subjects: []string{FeedSubject(repo)}})
	if err != nil {
		return nil // the page renders from the projection
	}
	defer w.Close()
	for {
		cur, err := s.db.FollowCursor(ctx, scmfeed.Topic, repo)
		if err != nil || cur >= seq {
			return err
		}
		if _, err := w.Next(ctx); err != nil {
			return nil
		}
	}
}
