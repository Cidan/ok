package store

import (
	"context"
	"encoding/json"
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

// RecordObservation records what the cluster runs of a service, replacing what was read before.
func (s *Store) RecordObservation(ctx context.Context, service int64, o Observation) error {
	images, err := json.Marshal(nonNilMap(o.Images))
	if err != nil {
		return err
	}
	pods := o.Pods
	if pods == nil {
		pods = []ObservedPod{}
	}
	podsJSON, err := json.Marshal(pods)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO service_observation (service_id, error, found, live, replicas, ready, images, pods)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (service_id) DO UPDATE SET error = EXCLUDED.error, found = EXCLUDED.found,
			live = EXCLUDED.live, replicas = EXCLUDED.replicas, ready = EXCLUDED.ready, images = EXCLUDED.images, pods = EXCLUDED.pods`,
		service, o.Error, o.Found, o.Live, o.Replicas, o.Ready, images, podsJSON)
	return err
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// observations reads the observations of services, by service.
func (s *Store) observations(ctx context.Context, ids []int64) (map[int64]*Observation, error) {
	rows, err := s.pool.Query(ctx, `SELECT service_id, error, found, live, replicas, ready, images, pods
		FROM service_observation WHERE service_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]*Observation{}
	for rows.Next() {
		var id int64
		var o Observation
		var images, pods []byte
		if err := rows.Scan(&id, &o.Error, &o.Found, &o.Live, &o.Replicas, &o.Ready, &images, &pods); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(images, &o.Images); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(pods, &o.Pods); err != nil {
			return nil, err
		}
		out[id] = &o
	}
	return out, rows.Err()
}
