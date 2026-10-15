package roll

import (
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Timing is how a stage is watched (docs/deploy.md, "Watch"): a stage's new pods must be
// Ready within ReadyWithin of starting, and the stage passes Watch after they started with no
// failure. The deployer's are the design's, 3 minutes and 30 seconds; tests may change them.
type Timing struct {
	ReadyWithin time.Duration
	Watch       time.Duration
}

// DefaultTiming is the design's.
var DefaultTiming = Timing{ReadyWithin: 3 * time.Minute, Watch: 30 * time.Second}

// Pod is what the watch reads of one pod.
type Pod struct {
	Name     string
	Started  time.Time // its creation
	Ready    bool
	Restarts int32 // across every container, init containers included
	// Stuck is a container's waiting reason that cannot resolve itself: a crash loop, an image
	// that cannot be pulled, a container that cannot be created. "" when none.
	Stuck  string
	Failed string // why the pod failed or was evicted; "" while it has not
}

// stuckReasons are the waiting reasons that fail a stage at once.
var stuckReasons = map[string]bool{
	"CrashLoopBackOff":           true,
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"InvalidImageName":           true,
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"RunContainerError":          true,
}

// podOf reads what the watch needs of a pod.
func podOf(p *corev1.Pod) Pod {
	out := Pod{Name: p.Name, Started: p.CreationTimestamp.Time}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			out.Ready = true
		}
	}
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.InitContainerStatuses, p.Status.ContainerStatuses} {
		for _, cs := range statuses {
			out.Restarts += cs.RestartCount
			if w := cs.State.Waiting; w != nil && stuckReasons[w.Reason] && out.Stuck == "" {
				out.Stuck = fmt.Sprintf("container %s: %s", cs.Name, w.Reason)
			}
		}
	}
	switch {
	case p.Status.Phase == corev1.PodFailed && p.Status.Reason != "":
		out.Failed = p.Status.Reason
	case p.Status.Phase == corev1.PodFailed:
		out.Failed = "the pod failed"
	}
	return out
}

// Verdict is a watch's judgment of a stage.
type Verdict int

const (
	Waiting Verdict = iota // the stage is not over
	Passed
	Failed
)

// Judgment is a verdict, why, and the pods seen Ready so far, which the caller keeps: a pod
// seen Ready that is not Ready any more has failed.
type Judgment struct {
	Verdict Verdict
	Reason  string
	Seen    []string
}

// Evaluate judges a stage at now: it began at started, wants want pods, and has pods (its new
// pods, those running the images it rolls out); seen are the pods it has seen Ready before.
func Evaluate(t Timing, now, started time.Time, want int, pods []Pod, seen []string) Judgment {
	seenReady := map[string]bool{}
	for _, s := range seen {
		seenReady[s] = true
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	j := Judgment{Verdict: Waiting}
	settled := 0
	for _, p := range pods {
		switch {
		case p.Stuck != "":
			return Judgment{Verdict: Failed, Reason: fmt.Sprintf("pod %s: %s", p.Name, p.Stuck), Seen: seen}
		case p.Failed != "":
			return Judgment{Verdict: Failed, Reason: fmt.Sprintf("pod %s: %s", p.Name, p.Failed), Seen: seen}
		case p.Restarts > 0:
			return Judgment{Verdict: Failed, Reason: fmt.Sprintf("pod %s restarted %d time(s)", p.Name, p.Restarts), Seen: seen}
		case seenReady[p.Name] && !p.Ready:
			return Judgment{Verdict: Failed, Reason: fmt.Sprintf("pod %s was Ready and is not any more", p.Name), Seen: seen}
		case !p.Ready && now.Sub(p.Started) > t.ReadyWithin:
			return Judgment{Verdict: Failed, Reason: fmt.Sprintf("pod %s was not Ready within %s", p.Name, t.ReadyWithin), Seen: seen}
		}
		if p.Ready {
			seenReady[p.Name] = true
			if now.Sub(p.Started) >= t.Watch {
				settled++
			}
		}
	}
	if len(pods) < want && now.Sub(started) > t.ReadyWithin {
		return Judgment{Verdict: Failed, Reason: fmt.Sprintf("%d of %d pods started within %s", len(pods), want, t.ReadyWithin), Seen: seen}
	}
	for name := range seenReady {
		j.Seen = append(j.Seen, name)
	}
	sort.Strings(j.Seen)
	if settled >= want {
		j.Verdict = Passed
	}
	return j
}
