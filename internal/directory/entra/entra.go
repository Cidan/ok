// Package entra reads a Microsoft Entra ID tenant as ok's system of record, through Microsoft
// Graph: its users, its groups, and their members. ok signs in as an application with the client
// credentials grant, and needs the application permissions User.Read.All and GroupMember.Read.All.
package entra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/oauth2/clientcredentials"

	"github.com/Cidan/ok/internal/directory"
)

// Config is a tenant to read.
type Config struct {
	TenantID     string
	ClientID     string
	ClientSecret string
	// GroupFilter, when set, syncs only the groups it selects: an OData filter, such as
	// startswith(displayName,'eng').
	GroupFilter string
	// IncludeGuests syncs guest users too; by default only the tenant's members are people.
	IncludeGuests bool
	// GraphURL and TokenURL are Microsoft's; tests point them elsewhere.
	GraphURL string
	TokenURL string
}

// Source is a tenant as a directory.Source. It says no deltas: each sync reads it whole.
type Source struct {
	cfg    Config
	client *http.Client
}

// New returns the tenant source over cfg.
func New(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.TenantID == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, errors.New("an Entra ID tenant needs a tenant ID, a client ID, and a client secret")
	}
	if cfg.GraphURL == "" {
		cfg.GraphURL = "https://graph.microsoft.com/v1.0"
	}
	if cfg.TokenURL == "" {
		cfg.TokenURL = "https://login.microsoftonline.com/" + url.PathEscape(cfg.TenantID) + "/oauth2/v2.0/token"
	}
	cc := &clientcredentials.Config{
		ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret, TokenURL: cfg.TokenURL,
		Scopes: []string{"https://graph.microsoft.com/.default"},
	}
	return &Source{cfg: cfg, client: cc.Client(ctx)}, nil
}

// Changes says no deltas.
func (s *Source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// list reads every page of a Graph collection, from a path under GraphURL, following
// @odata.nextLink.
func (s *Source) list(ctx context.Context, path string, q url.Values, each func(json.RawMessage) error) error {
	next := strings.TrimSuffix(s.cfg.GraphURL, "/") + path + "?" + q.Encode()
	for next != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		if err != nil {
			return err
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return fmt.Errorf("Microsoft Graph: %w", err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Microsoft Graph answered %s for %s: %s", resp.Status, path, strings.TrimSpace(string(body)))
		}
		var page struct {
			Value    []json.RawMessage `json:"value"`
			NextLink string            `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return err
		}
		for _, v := range page.Value {
			if err := each(v); err != nil {
				return err
			}
		}
		next = page.NextLink
	}
	return nil
}

type user struct {
	ID                string `json:"id"`
	UserPrincipalName string `json:"userPrincipalName"`
	Mail              string `json:"mail"`
	DisplayName       string `json:"displayName"`
	AccountEnabled    *bool  `json:"accountEnabled"`
	UserType          string `json:"userType"`
}

type group struct {
	ID           string `json:"id"`
	DisplayName  string `json:"displayName"`
	MailNickname string `json:"mailNickname"`
}

type object struct {
	Type string `json:"@odata.type"`
	ID   string `json:"id"`
}

// Snapshot reads the tenant's users, its groups (those GroupFilter selects), and each group's
// members. A user's handle is their user principal name's local part, their email their mail (or
// their user principal name); a group's name, its mail nickname (or its display name).
func (s *Source) Snapshot(ctx context.Context) (*directory.Snapshot, error) {
	snap := &directory.Snapshot{}
	users := map[string]bool{}
	q := url.Values{"$select": {"id,userPrincipalName,mail,displayName,accountEnabled,userType"}, "$top": {"999"}}
	err := s.list(ctx, "/users", q, func(raw json.RawMessage) error {
		var u user
		if err := json.Unmarshal(raw, &u); err != nil {
			return err
		}
		if u.UserType == "Guest" && !s.cfg.IncludeGuests {
			return nil
		}
		handle, _, _ := strings.Cut(u.UserPrincipalName, "@")
		email := u.Mail
		if email == "" {
			email = u.UserPrincipalName
		}
		snap.People = append(snap.People, directory.Person{
			ExternalID: u.ID, Handle: handle, Email: strings.ToLower(email), Name: u.DisplayName,
			Active: u.AccountEnabled == nil || *u.AccountEnabled,
		})
		users[u.ID] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	q = url.Values{"$select": {"id,displayName,mailNickname"}, "$top": {"999"}}
	if s.cfg.GroupFilter != "" {
		q.Set("$filter", s.cfg.GroupFilter)
	}
	var groups []group
	err = s.list(ctx, "/groups", q, func(raw json.RawMessage) error {
		var g group
		if err := json.Unmarshal(raw, &g); err != nil {
			return err
		}
		groups = append(groups, g)
		return nil
	})
	if err != nil {
		return nil, err
	}
	synced := map[string]bool{}
	for _, g := range groups {
		synced[g.ID] = true
	}
	for _, g := range groups {
		name := g.MailNickname
		if name == "" {
			name = g.DisplayName
		}
		dg := directory.Group{ExternalID: g.ID, Name: name}
		err := s.list(ctx, "/groups/"+url.PathEscape(g.ID)+"/members", url.Values{"$select": {"id"}, "$top": {"999"}}, func(raw json.RawMessage) error {
			var o object
			if err := json.Unmarshal(raw, &o); err != nil {
				return err
			}
			switch {
			case o.Type == "#microsoft.graph.user" && users[o.ID]:
				dg.Members = append(dg.Members, directory.Member{ExternalID: o.ID})
			case o.Type == "#microsoft.graph.group" && synced[o.ID]:
				dg.Members = append(dg.Members, directory.Member{ExternalID: o.ID, Group: true})
			}
			// Devices, service principals, guests, and groups not synced are no one ok knows.
			return nil
		})
		if err != nil {
			return nil, err
		}
		snap.Groups = append(snap.Groups, dg)
	}
	return snap, nil
}
