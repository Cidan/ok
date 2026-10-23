package load

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Setup is the run's repositories, owner/prefix-00000 on (prefix-00000, of the installation's
// organization, with no owner), making each that is not there, parallel at a time.
func Setup(ctx context.Context, repos scmpb.RepositoriesClient, owner, prefix string, n, parallel int) ([]Repo, error) {
	out := make([]Repo, n)
	errs := make([]error, n)
	sem := make(chan struct{}, max(parallel, 1))
	var wg sync.WaitGroup
	for i := range n {
		full := fmt.Sprintf("%s-%05d", prefix, i)
		if owner != "" {
			full = owner + "/" + full
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r, err := repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: full})
			if status.Code(err) == codes.AlreadyExists {
				// Made by another of the run's pods: its name is taken before the repository is
				// there to read, for as long as that pod's creation takes.
				for range 30 {
					r, err = repos.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: full}})
					if status.Code(err) != codes.NotFound {
						break
					}
					select {
					case <-ctx.Done():
						errs[i] = ctx.Err()
						return
					case <-time.After(time.Second):
					}
				}
			}
			if err != nil {
				errs[i] = fmt.Errorf("%s: %w", full, err)
				return
			}
			out[i] = Repo{ID: r.GetId(), Name: r.GetName()}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
