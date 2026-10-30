package web

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Cidan/ok/internal/control/api"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/wait"
)

// A page's live regions (live.js) are refreshed as the server says what they show changed: each
// names a stream of server-sent events, which registers on what the page shows with each service
// whose writes change it (internal/wait), and says "change" once as it opens — whatever changed
// between the page's render and the registration — then each time any of it changes.

// keepAlive is how long a stream goes without an event before it sends a comment, so a proxy does
// not take it for idle.
const keepAlive = 25 * time.Second

// liveEvents streams change events for regs until the page goes, or api.MaxWatch passes and the
// browser connects again. more, when set, is asked after each change for registrations the change
// calls for — a CL that landed, on its commit's deployments — each sent a change of its own once
// registered.
func (s *Server) liveEvents(w http.ResponseWriter, r *http.Request, regs []wait.Reg, more func(context.Context) ([]wait.Reg, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), api.MaxWatch)
	defer cancel()
	wt, err := wait.Open(ctx, s.db.Waits().Jobs().Queue, regs...)
	if err != nil {
		slog.Warn("ok control: opening a page's change events", "err", err)
		http.Error(w, "The page's changes cannot be followed now.", http.StatusServiceUnavailable)
		return
	}
	defer wt.Close()
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	send := func(text string) bool {
		if _, err := fmt.Fprint(w, text); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send("retry: 2000\n\nevent: change\ndata:\n\n") {
		return
	}
	for {
		next, stop := context.WithTimeout(ctx, keepAlive)
		_, err := wt.Next(next)
		stop()
		switch {
		case ctx.Err() != nil:
			return
		case errors.Is(err, context.DeadlineExceeded):
			if !send(": still here\n\n") {
				return
			}
			continue
		case err != nil:
			return // its registrations could not be renewed: the browser connects again
		}
		if more != nil {
			add, err := more(ctx)
			if err != nil {
				return
			}
			for _, r := range add {
				if err := wt.Add(ctx, r); err != nil {
					return
				}
			}
		}
		if !send("event: change\ndata:\n\n") {
			return
		}
	}
}

// clEvents are a CL page's change events: of the CL, as ok control records it, and as ok
// automation's checks of it go — and, once it lands, its commit's deployments.
func (s *Server) clEvents(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	repo, number := t.repo.GetId(), t.cl.Number
	subject := wait.CLSubject(repo, number)
	regs := []wait.Reg{{Store: s.db.Waits(), Subjects: []string{subject}}}
	if s.waits.Automation != nil {
		regs = append(regs, wait.Reg{Store: s.waits.Automation, Subjects: []string{subject}})
	}
	deploys := func(cl db.ChangeList) []wait.Reg {
		if s.waits.Deploy == nil || cl.Status != db.StatusSubmitted || len(cl.LandedCommit) == 0 {
			return nil
		}
		return []wait.Reg{{Store: s.waits.Deploy, Subjects: []string{wait.CommitSubject(repo, cl.LandedCommit)}}}
	}
	regs = append(regs, deploys(t.cl)...)
	watching := deploys(t.cl) != nil
	s.liveEvents(w, r, regs, func(ctx context.Context) ([]wait.Reg, error) {
		if watching {
			return nil, nil
		}
		cl, ok, err := s.db.GetChangeList(ctx, repo, number)
		if err != nil || !ok {
			return nil, err
		}
		add := deploys(cl)
		watching = add != nil
		return add, nil
	})
}

// runEvents are a run page's change events: the run's, as it is queued, runs, logs, and ends.
func (s *Server) runEvents(w http.ResponseWriter, r *http.Request) {
	_, run, err := s.loadRun(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	if s.waits.Automation == nil {
		http.Error(w, "ok automation's changes cannot be followed.", http.StatusServiceUnavailable)
		return
	}
	s.liveEvents(w, r, []wait.Reg{{Store: s.waits.Automation, Subjects: []string{wait.RunSubject(run.GetId())}}}, nil)
}

// deploymentEvents are a deployment page's change events: the deployment's.
func (s *Server) deploymentEvents(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadDeployment(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	if s.waits.Deploy == nil {
		http.Error(w, "The deployer's changes cannot be followed.", http.StatusServiceUnavailable)
		return
	}
	s.liveEvents(w, r, []wait.Reg{{Store: s.waits.Deploy, Subjects: []string{wait.DeploymentSubject(t.d.GetId())}}}, nil)
}

// servicesEvents are the Services pages' change events: the services', as they are deployed and as
// what the cluster runs of them changes.
func (s *Server) servicesEvents(w http.ResponseWriter, r *http.Request) {
	if s.waits.Deploy == nil {
		http.Error(w, "The deployer's changes cannot be followed.", http.StatusServiceUnavailable)
		return
	}
	s.liveEvents(w, r, []wait.Reg{{Store: s.waits.Deploy, Subjects: []string{wait.ServicesSubject}}}, nil)
}
