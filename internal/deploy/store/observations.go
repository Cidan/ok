package store

import (
	"maps"
	"time"
)

// Observation is what the cluster runs of a service's workload, as the observer last read it.
type Observation struct {
	Error    string            // the cluster could not be read: the rest is empty
	Found    bool              // the workload exists
	Live     string            // a Deployment's live copy, blue or green; "" for a StatefulSet
	Replicas int32             // the live copy's wanted replicas
	Ready    int32             // its pods Ready and running its template's images
	Images   map[string]string // the live copy's template: container → image
	Pods     []ObservedPod
}

// ObservedPod is one of a workload's pods.
type ObservedPod struct {
	Name        string            `json:"name"`
	Copy        string            `json:"copy,omitempty"` // blue or green; "" for a StatefulSet's
	Ready       bool              `json:"ready,omitempty"`
	Restarts    int32             `json:"restarts,omitempty"`
	Phase       string            `json:"phase,omitempty"`
	Reason      string            `json:"reason,omitempty"` // why it is not running
	Images      map[string]string `json:"images,omitempty"` // container → the image it runs
	Started     time.Time         `json:"started"`
	Terminating bool              `json:"terminating,omitempty"`
}

// sameObservation reports whether two readings of a workload agree.
func sameObservation(a, b Observation) bool {
	if a.Error != b.Error || a.Found != b.Found || a.Live != b.Live || a.Replicas != b.Replicas || a.Ready != b.Ready ||
		!maps.Equal(a.Images, b.Images) || len(a.Pods) != len(b.Pods) {
		return false
	}
	for i, p := range a.Pods {
		q := b.Pods[i]
		if p.Name != q.Name || p.Copy != q.Copy || p.Ready != q.Ready || p.Restarts != q.Restarts || p.Phase != q.Phase ||
			p.Reason != q.Reason || !maps.Equal(p.Images, q.Images) || !p.Started.Equal(q.Started) || p.Terminating != q.Terminating {
			return false
		}
	}
	return true
}
