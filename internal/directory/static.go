package directory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Static is a system of record kept as a YAML file, for an installation with no directory of its
// own:
//
//	people:
//	  - id: "1001"                 # never reused, never changed
//	    email: ada@example.com     # the handle defaults to the email's local part
//	    name: Ada Lovelace
//	  - id: "1002"
//	    handle: grace
//	    email: grace@example.com
//	    active: false              # suspended
//	groups:
//	  - id: eng
//	    name: eng
//	    members: ["1001", "1002", "eng-infra"]   # people's and groups' ids
//
// Its cursor is the file's SHA-256; it says no deltas, so every sync reads the whole file.
type Static struct {
	Path string
}

type staticFile struct {
	People []staticPerson `yaml:"people"`
	Groups []staticGroup  `yaml:"groups"`
}

type staticPerson struct {
	ID     string `yaml:"id"`
	Handle string `yaml:"handle"`
	Email  string `yaml:"email"`
	Name   string `yaml:"name"`
	Active *bool  `yaml:"active"`
}

type staticGroup struct {
	ID      string   `yaml:"id"`
	Name    string   `yaml:"name"`
	Members []string `yaml:"members"`
}

// Snapshot reads the file.
func (s Static) Snapshot(context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		return nil, err
	}
	return parseStatic(data)
}

// Changes says no deltas: the file keeps no history.
func (s Static) Changes(context.Context, string) (*Delta, error) { return nil, ErrNoDelta }

func parseStatic(data []byte) (*Snapshot, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var f staticFile
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("the static system of record: %w", err)
	}
	sum := sha256.Sum256(data)
	snap := &Snapshot{Cursor: hex.EncodeToString(sum[:])}
	kind := map[string]string{} // id -> "person" or "group"
	var problems []string
	for _, p := range f.People {
		handle := p.Handle
		if handle == "" {
			handle, _, _ = strings.Cut(p.Email, "@")
		}
		active := p.Active == nil || *p.Active
		snap.People = append(snap.People, Person{ExternalID: p.ID, Handle: handle, Email: p.Email, Name: p.Name, Active: active})
		if kind[p.ID] != "" {
			problems = append(problems, fmt.Sprintf("the id %q is given twice", p.ID))
		}
		kind[p.ID] = "person"
	}
	for _, g := range f.Groups {
		if kind[g.ID] != "" {
			problems = append(problems, fmt.Sprintf("the id %q is given twice", g.ID))
		}
		kind[g.ID] = "group"
	}
	for _, g := range f.Groups {
		group := Group{ExternalID: g.ID, Name: g.Name}
		for _, m := range g.Members {
			switch kind[m] {
			case "person":
				group.Members = append(group.Members, Member{ExternalID: m})
			case "group":
				group.Members = append(group.Members, Member{ExternalID: m, Group: true})
			default:
				problems = append(problems, fmt.Sprintf("the group %q holds %q, which is no person's or group's id", g.ID, m))
			}
		}
		snap.Groups = append(snap.Groups, group)
	}
	if len(problems) > 0 {
		return nil, &Error{Problems: problems}
	}
	return snap, snap.check()
}
