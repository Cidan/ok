// Package google reads a Google Workspace directory as ok's system of record, through the Admin
// SDK's Directory API: its users, its groups, and their members. ok signs in as a service account
// with domain-wide delegation, acting as an administrator of the Workspace, with read-only scopes.
package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/oauth2/jwt"

	"github.com/Cidan/ok/internal/directory"
)

// Scopes are the read-only scopes the service account is delegated.
var Scopes = []string{
	"https://www.googleapis.com/auth/admin.directory.user.readonly",
	"https://www.googleapis.com/auth/admin.directory.group.readonly",
}

// Config is a Workspace to read.
type Config struct {
	// KeyFile is the service account's JSON key.
	KeyFile []byte
	// Subject is the administrator the service account acts as: admin@example.com.
	Subject string
	// Customer is the Workspace: my_customer (the administrator's own), or its ID.
	Customer string
	// GroupQuery, when set, syncs only the groups it finds (the API's query syntax: email:eng-*).
	GroupQuery string
	// BaseURL is the API's; tests point it elsewhere.
	BaseURL string
}

// Source is a Workspace as a directory.Source. It says no deltas: each sync reads it whole.
type Source struct {
	cfg    Config
	client *http.Client
}

// New returns the Workspace source over cfg.
func New(ctx context.Context, cfg Config) (*Source, error) {
	var key struct {
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
		TokenURI    string `json:"token_uri"`
	}
	if err := json.Unmarshal(cfg.KeyFile, &key); err != nil || key.ClientEmail == "" || key.PrivateKey == "" {
		return nil, errors.New("the Google service account key is not a service account's JSON key")
	}
	if cfg.Subject == "" {
		return nil, errors.New("name the Workspace administrator the service account acts as (subject)")
	}
	if cfg.Customer == "" {
		cfg.Customer = "my_customer"
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://admin.googleapis.com"
	}
	if key.TokenURI == "" {
		key.TokenURI = "https://oauth2.googleapis.com/token"
	}
	jc := &jwt.Config{Email: key.ClientEmail, PrivateKey: []byte(key.PrivateKey), Scopes: Scopes, TokenURL: key.TokenURI, Subject: cfg.Subject}
	return &Source{cfg: cfg, client: jc.Client(ctx)}, nil
}

// NewWithClient is a source that calls the API with client as it is: for tests.
func NewWithClient(cfg Config, client *http.Client) *Source {
	if cfg.Customer == "" {
		cfg.Customer = "my_customer"
	}
	return &Source{cfg: cfg, client: client}
}

// Changes says no deltas.
func (s *Source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// get reads one page of the API.
func (s *Source) get(ctx context.Context, path string, q url.Values, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(s.cfg.BaseURL, "/")+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("the Google Directory API: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the Google Directory API answered %s for %s: %s", resp.Status, path, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, into)
}

// pages reads every page of a list.
func (s *Source) pages(ctx context.Context, path string, q url.Values, page func([]byte) (next string, err error)) error {
	for {
		var raw json.RawMessage
		if err := s.get(ctx, path, q, &raw); err != nil {
			return err
		}
		next, err := page(raw)
		if err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		q.Set("pageToken", next)
	}
}

type user struct {
	ID           string `json:"id"`
	PrimaryEmail string `json:"primaryEmail"`
	Name         struct {
		FullName string `json:"fullName"`
	} `json:"name"`
	Suspended bool `json:"suspended"`
	Archived  bool `json:"archived"`
}

type group struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

type member struct {
	ID     string `json:"id"`
	Type   string `json:"type"` // USER, GROUP, CUSTOMER
	Status string `json:"status"`
}

// Snapshot reads the Workspace's users, its groups (those GroupQuery finds), and each group's
// members. A user's handle is their primary email's local part; a group's name, its email's.
func (s *Source) Snapshot(ctx context.Context) (*directory.Snapshot, error) {
	snap := &directory.Snapshot{}
	users := map[string]bool{}
	q := url.Values{"customer": {s.cfg.Customer}, "maxResults": {"500"}, "projection": {"basic"}}
	err := s.pages(ctx, "/admin/directory/v1/users", q, func(raw []byte) (string, error) {
		var page struct {
			Users         []user `json:"users"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return "", err
		}
		for _, u := range page.Users {
			handle, _, _ := strings.Cut(u.PrimaryEmail, "@")
			snap.People = append(snap.People, directory.Person{
				ExternalID: u.ID, Handle: handle, Email: strings.ToLower(u.PrimaryEmail), Name: u.Name.FullName,
				Active: !u.Suspended && !u.Archived,
			})
			users[u.ID] = true
		}
		return page.NextPageToken, nil
	})
	if err != nil {
		return nil, err
	}
	q = url.Values{"customer": {s.cfg.Customer}, "maxResults": {"200"}}
	if s.cfg.GroupQuery != "" {
		q.Set("query", s.cfg.GroupQuery)
	}
	var groups []group
	err = s.pages(ctx, "/admin/directory/v1/groups", q, func(raw []byte) (string, error) {
		var page struct {
			Groups        []group `json:"groups"`
			NextPageToken string  `json:"nextPageToken"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return "", err
		}
		groups = append(groups, page.Groups...)
		return page.NextPageToken, nil
	})
	if err != nil {
		return nil, err
	}
	synced := map[string]bool{}
	for _, g := range groups {
		synced[g.ID] = true
	}
	for _, g := range groups {
		name, _, _ := strings.Cut(g.Email, "@")
		dg := directory.Group{ExternalID: g.ID, Name: name}
		mq := url.Values{"maxResults": {"200"}}
		err := s.pages(ctx, "/admin/directory/v1/groups/"+url.PathEscape(g.ID)+"/members", mq, func(raw []byte) (string, error) {
			var page struct {
				Members       []member `json:"members"`
				NextPageToken string   `json:"nextPageToken"`
			}
			if err := json.Unmarshal(raw, &page); err != nil {
				return "", err
			}
			for _, m := range page.Members {
				switch {
				case m.Type == "USER" && users[m.ID]:
					dg.Members = append(dg.Members, directory.Member{ExternalID: m.ID})
				case m.Type == "GROUP" && synced[m.ID]:
					dg.Members = append(dg.Members, directory.Member{ExternalID: m.ID, Group: true})
				}
				// Anyone else — outside the Workspace, a group not synced, the whole customer — is
				// no one ok knows.
			}
			return page.NextPageToken, nil
		})
		if err != nil {
			return nil, err
		}
		snap.Groups = append(snap.Groups, dg)
	}
	return snap, nil
}
