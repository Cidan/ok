package scim

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The schemas a response names.
const (
	userSchema         = "urn:ietf:params:scim:schemas:core:2.0:User"
	groupSchema        = "urn:ietf:params:scim:schemas:core:2.0:Group"
	listSchema         = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	errorSchema        = "urn:ietf:params:scim:api:messages:2.0:Error"
	providerSchema     = "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
	resourceTypeSchema = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
	schemaSchema       = "urn:ietf:params:scim:schemas:core:2.0:Schema"
)

// Handler serves SCIM under its base path: /id/scim/v2.
type Handler struct {
	store Store
	// token is the SHA-256 of the bearer token clients present.
	token [32]byte
	base  string // the base URL resources' locations are under
	// changed is told after every change, so the sync follows soon.
	changed func()
}

// NewHandler serves store to clients presenting token, at base (https://ok.example.com/id/scim/v2).
// changed, when set, is told of every change.
func NewHandler(store Store, token, base string, changed func()) (*Handler, error) {
	if len(token) < 32 {
		return nil, errors.New("the SCIM token is at least 32 characters")
	}
	if changed == nil {
		changed = func() {}
	}
	return &Handler{store: store, token: sha256.Sum256([]byte(token)), base: strings.TrimSuffix(base, "/"), changed: changed}, nil
}

// scimError is a SCIM error response.
type scimError struct {
	status   int
	scimType string
	detail   string
}

func (h *Handler) fail(w http.ResponseWriter, e scimError) {
	body := map[string]any{"schemas": []string{errorSchema}, "status": strconv.Itoa(e.status), "detail": e.detail}
	if e.scimType != "" {
		body["scimType"] = e.scimType
	}
	write(w, e.status, body)
}

func write(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/scim+json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}

func (h *Handler) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		h.fail(w, scimError{http.StatusNotFound, "", "no such resource"})
	case errors.Is(err, ErrConflict):
		h.fail(w, scimError{http.StatusConflict, "uniqueness", err.Error()})
	default:
		var pe errPatch
		if errors.As(err, &pe) {
			h.fail(w, scimError{http.StatusBadRequest, "invalidValue", pe.msg})
			return
		}
		slog.Error("scim: the store", "err", err)
		h.fail(w, scimError{http.StatusInternalServerError, "", "the directory could not answer"})
	}
}

// ServeHTTP authenticates the client and routes the request; the path is what follows the base.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(auth, "Bearer ")
	sum := sha256.Sum256([]byte(tok))
	if !ok || subtle.ConstantTimeCompare(sum[:], h.token[:]) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ok scim"`)
		h.fail(w, scimError{http.StatusUnauthorized, "", "present the SCIM bearer token"})
		return
	}
	path := strings.Trim(r.URL.Path, "/")
	if i := strings.Index(path, "scim/v2"); i >= 0 {
		path = strings.Trim(path[i+len("scim/v2"):], "/")
	}
	parts := strings.Split(path, "/")
	switch {
	case path == "ServiceProviderConfig" && r.Method == http.MethodGet:
		h.providerConfig(w)
	case path == "ResourceTypes" && r.Method == http.MethodGet:
		h.resourceTypes(w)
	case path == "Schemas" && r.Method == http.MethodGet:
		h.schemas(w)
	case parts[0] == "Users" && len(parts) <= 2:
		h.users(w, r, parts[1:])
	case parts[0] == "Groups" && len(parts) <= 2:
		h.groups(w, r, parts[1:])
	default:
		h.fail(w, scimError{http.StatusNotFound, "", "no such endpoint"})
	}
}

func newResourceID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

func readJSON(r *http.Request, into any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}

// meta is a resource's metadata.
func (h *Handler) meta(kind, id string, created, modified time.Time) map[string]any {
	return map[string]any{
		"resourceType": kind, "location": h.base + "/" + kind + "s/" + id,
		"created": created.UTC().Format(time.RFC3339), "lastModified": modified.UTC().Format(time.RFC3339),
		"version": fmt.Sprintf(`W/"%d"`, modified.UnixNano()),
	}
}

func (h *Handler) userJSON(u User) map[string]any {
	out := map[string]any{"schemas": []string{userSchema}, "id": u.ID, "userName": u.UserName, "active": u.Active, "meta": h.meta("User", u.ID, u.Created, u.Modified)}
	if u.ExternalID != "" {
		out["externalId"] = u.ExternalID
	}
	if u.DisplayName != "" {
		out["displayName"] = u.DisplayName
	}
	if u.Name != (Name{}) {
		out["name"] = u.Name
	}
	if len(u.Emails) > 0 {
		out["emails"] = u.Emails
	}
	return out
}

func (h *Handler) groupJSON(ctx context.Context, g Group, withMembers bool) map[string]any {
	out := map[string]any{"schemas": []string{groupSchema}, "id": g.ID, "displayName": g.DisplayName, "meta": h.meta("Group", g.ID, g.Created, g.Modified)}
	if g.ExternalID != "" {
		out["externalId"] = g.ExternalID
	}
	if withMembers {
		ms := []map[string]string{}
		for _, id := range g.Members {
			kind := "User"
			if _, err := h.store.User(ctx, id); errors.Is(err, ErrNotFound) {
				kind = "Group"
			}
			ms = append(ms, map[string]string{"value": id, "type": kind, "$ref": h.base + "/" + kind + "s/" + id})
		}
		out["members"] = ms
	}
	return out
}

// page reads startIndex and count: SCIM's 1-based paging.
func page(r *http.Request) (start, count int) {
	start, count = 1, 100
	if v, err := strconv.Atoi(r.URL.Query().Get("startIndex")); err == nil && v > 1 {
		start = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("count")); err == nil && v >= 0 {
		count = min(v, 1000)
	}
	return start, count
}

func list(w http.ResponseWriter, total, start int, items []map[string]any) {
	if items == nil {
		items = []map[string]any{}
	}
	write(w, http.StatusOK, map[string]any{"schemas": []string{listSchema}, "totalResults": total, "startIndex": start, "itemsPerPage": len(items), "Resources": items})
}

func window[T any](all []T, start, count int) []T {
	if start-1 >= len(all) {
		return nil
	}
	end := min(start-1+count, len(all))
	return all[start-1 : end]
}

// userBody is a user as a client sends it.
type userBody struct {
	UserName    string          `json:"userName"`
	ExternalID  string          `json:"externalId"`
	DisplayName string          `json:"displayName"`
	Name        Name            `json:"name"`
	Emails      []Email         `json:"emails"`
	Active      json.RawMessage `json:"active"`
}

func (b userBody) user() (User, error) {
	u := User{UserName: b.UserName, ExternalID: b.ExternalID, DisplayName: b.DisplayName, Name: b.Name, Emails: b.Emails, Active: true}
	if len(b.Active) > 0 && string(b.Active) != "null" {
		var err error
		if u.Active, err = asBool(b.Active); err != nil {
			return u, err
		}
	}
	if u.UserName == "" {
		return u, errPatch{"userName is required"}
	}
	return u, nil
}

func (h *Handler) users(w http.ResponseWriter, r *http.Request, rest []string) {
	ctx := r.Context()
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodGet:
			all, err := h.store.Users(ctx)
			if err != nil {
				h.storeError(w, err)
				return
			}
			if fs := r.URL.Query().Get("filter"); fs != "" {
				f, err := parseFilter(fs)
				if err != nil {
					h.fail(w, scimError{http.StatusBadRequest, "invalidFilter", err.Error()})
					return
				}
				var kept []User
				for _, u := range all {
					if f.matchUser(u) {
						kept = append(kept, u)
					}
				}
				all = kept
			}
			sort.Slice(all, func(i, j int) bool {
				return all[i].Created.Before(all[j].Created) || all[i].Created.Equal(all[j].Created) && all[i].ID < all[j].ID
			})
			start, count := page(r)
			var items []map[string]any
			for _, u := range window(all, start, count) {
				items = append(items, h.userJSON(u))
			}
			list(w, len(all), start, items)
		case http.MethodPost:
			var b userBody
			if err := readJSON(r, &b); err != nil {
				h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
				return
			}
			u, err := b.user()
			if err != nil {
				h.storeError(w, err)
				return
			}
			u.ID = newResourceID()
			if u, err = h.store.CreateUser(ctx, u); err != nil {
				h.storeError(w, err)
				return
			}
			h.changed()
			w.Header().Set("Location", h.base+"/Users/"+u.ID)
			write(w, http.StatusCreated, h.userJSON(u))
		default:
			h.fail(w, scimError{http.StatusMethodNotAllowed, "", "GET or POST"})
		}
		return
	}
	id := rest[0]
	u, err := h.store.User(ctx, id)
	if err != nil {
		h.storeError(w, err)
		return
	}
	switch r.Method {
	case http.MethodGet:
		write(w, http.StatusOK, h.userJSON(u))
	case http.MethodPut:
		var b userBody
		if err := readJSON(r, &b); err != nil {
			h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
			return
		}
		nu, err := b.user()
		if err != nil {
			h.storeError(w, err)
			return
		}
		nu.ID, nu.Created = u.ID, u.Created
		if nu, err = h.store.ReplaceUser(ctx, nu); err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		write(w, http.StatusOK, h.userJSON(nu))
	case http.MethodPatch:
		var p patchRequest
		if err := readJSON(r, &p); err != nil {
			h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
			return
		}
		nu, err := patchUser(u, p.Operations)
		if err == nil {
			nu, err = h.store.ReplaceUser(ctx, nu)
		}
		if err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		write(w, http.StatusOK, h.userJSON(nu))
	case http.MethodDelete:
		if err := h.store.DeleteUser(ctx, id); err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		w.WriteHeader(http.StatusNoContent)
	default:
		h.fail(w, scimError{http.StatusMethodNotAllowed, "", "GET, PUT, PATCH, or DELETE"})
	}
}

// groupBody is a group as a client sends it.
type groupBody struct {
	DisplayName string          `json:"displayName"`
	ExternalID  string          `json:"externalId"`
	Members     json.RawMessage `json:"members"`
}

func (b groupBody) group() (Group, error) {
	g := Group{DisplayName: b.DisplayName, ExternalID: b.ExternalID}
	if g.DisplayName == "" {
		return g, errPatch{"displayName is required"}
	}
	if len(b.Members) > 0 && string(b.Members) != "null" {
		ms, err := memberValues(b.Members)
		if err != nil {
			return g, err
		}
		g.Members = union(nil, ms)
	}
	return g, nil
}

// excludesMembers reports whether a request asks for groups without their members, as clients
// listing groups do.
func excludesMembers(r *http.Request) bool {
	for _, a := range strings.Split(r.URL.Query().Get("excludedAttributes"), ",") {
		if strings.EqualFold(strings.TrimSpace(a), "members") {
			return true
		}
	}
	return false
}

func (h *Handler) groups(w http.ResponseWriter, r *http.Request, rest []string) {
	ctx := r.Context()
	if len(rest) == 0 {
		switch r.Method {
		case http.MethodGet:
			all, err := h.store.Groups(ctx)
			if err != nil {
				h.storeError(w, err)
				return
			}
			if fs := r.URL.Query().Get("filter"); fs != "" {
				f, err := parseFilter(fs)
				if err != nil {
					h.fail(w, scimError{http.StatusBadRequest, "invalidFilter", err.Error()})
					return
				}
				var kept []Group
				for _, g := range all {
					if f.matchGroup(g) {
						kept = append(kept, g)
					}
				}
				all = kept
			}
			sort.Slice(all, func(i, j int) bool {
				return all[i].Created.Before(all[j].Created) || all[i].Created.Equal(all[j].Created) && all[i].ID < all[j].ID
			})
			start, count := page(r)
			var items []map[string]any
			for _, g := range window(all, start, count) {
				items = append(items, h.groupJSON(ctx, g, !excludesMembers(r)))
			}
			list(w, len(all), start, items)
		case http.MethodPost:
			var b groupBody
			if err := readJSON(r, &b); err != nil {
				h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
				return
			}
			g, err := b.group()
			if err != nil {
				h.storeError(w, err)
				return
			}
			g.ID = newResourceID()
			if g, err = h.store.CreateGroup(ctx, g); err != nil {
				h.storeError(w, err)
				return
			}
			h.changed()
			w.Header().Set("Location", h.base+"/Groups/"+g.ID)
			write(w, http.StatusCreated, h.groupJSON(ctx, g, true))
		default:
			h.fail(w, scimError{http.StatusMethodNotAllowed, "", "GET or POST"})
		}
		return
	}
	id := rest[0]
	g, err := h.store.Group(ctx, id)
	if err != nil {
		h.storeError(w, err)
		return
	}
	switch r.Method {
	case http.MethodGet:
		write(w, http.StatusOK, h.groupJSON(ctx, g, !excludesMembers(r)))
	case http.MethodPut:
		var b groupBody
		if err := readJSON(r, &b); err != nil {
			h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
			return
		}
		ng, err := b.group()
		if err != nil {
			h.storeError(w, err)
			return
		}
		ng.ID, ng.Created = g.ID, g.Created
		if ng, err = h.store.ReplaceGroup(ctx, ng); err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		write(w, http.StatusOK, h.groupJSON(ctx, ng, true))
	case http.MethodPatch:
		var p patchRequest
		if err := readJSON(r, &p); err != nil {
			h.fail(w, scimError{http.StatusBadRequest, "invalidSyntax", err.Error()})
			return
		}
		ng, err := patchGroup(g, p.Operations)
		if err == nil {
			ng, err = h.store.ReplaceGroup(ctx, ng)
		}
		if err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.store.DeleteGroup(ctx, id); err != nil {
			h.storeError(w, err)
			return
		}
		h.changed()
		w.WriteHeader(http.StatusNoContent)
	default:
		h.fail(w, scimError{http.StatusMethodNotAllowed, "", "GET, PUT, PATCH, or DELETE"})
	}
}

func (h *Handler) providerConfig(w http.ResponseWriter) {
	write(w, http.StatusOK, map[string]any{
		"schemas":        []string{providerSchema},
		"patch":          map[string]bool{"supported": true},
		"bulk":           map[string]any{"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
		"filter":         map[string]any{"supported": true, "maxResults": 1000},
		"changePassword": map[string]bool{"supported": false},
		"sort":           map[string]bool{"supported": false},
		"etag":           map[string]bool{"supported": false},
		"authenticationSchemes": []map[string]any{{
			"type": "oauthbearertoken", "name": "Bearer token", "description": "The SCIM token the installation configured.", "primary": true,
		}},
		"meta": map[string]string{"resourceType": "ServiceProviderConfig", "location": h.base + "/ServiceProviderConfig"},
	})
}

func (h *Handler) resourceTypes(w http.ResponseWriter) {
	types := []map[string]any{
		{"schemas": []string{resourceTypeSchema}, "id": "User", "name": "User", "endpoint": "/Users", "schema": userSchema,
			"meta": map[string]string{"resourceType": "ResourceType", "location": h.base + "/ResourceTypes/User"}},
		{"schemas": []string{resourceTypeSchema}, "id": "Group", "name": "Group", "endpoint": "/Groups", "schema": groupSchema,
			"meta": map[string]string{"resourceType": "ResourceType", "location": h.base + "/ResourceTypes/Group"}},
	}
	list(w, len(types), 1, types)
}

func attr(name, typ string, multi, required bool, sub ...map[string]any) map[string]any {
	a := map[string]any{"name": name, "type": typ, "multiValued": multi, "required": required, "mutability": "readWrite", "returned": "default", "uniqueness": "none", "caseExact": false}
	if len(sub) > 0 {
		a["subAttributes"] = sub
	}
	return a
}

func (h *Handler) schemas(w http.ResponseWriter) {
	user := map[string]any{"schemas": []string{schemaSchema}, "id": userSchema, "name": "User", "attributes": []map[string]any{
		attr("userName", "string", false, true), attr("externalId", "string", false, false), attr("displayName", "string", false, false),
		attr("active", "boolean", false, false),
		attr("name", "complex", false, false, attr("formatted", "string", false, false), attr("givenName", "string", false, false), attr("familyName", "string", false, false)),
		attr("emails", "complex", true, false, attr("value", "string", false, false), attr("type", "string", false, false), attr("primary", "boolean", false, false)),
	}, "meta": map[string]string{"resourceType": "Schema", "location": h.base + "/Schemas/" + userSchema}}
	group := map[string]any{"schemas": []string{schemaSchema}, "id": groupSchema, "name": "Group", "attributes": []map[string]any{
		attr("displayName", "string", false, true), attr("externalId", "string", false, false),
		attr("members", "complex", true, false, attr("value", "string", false, false), attr("type", "string", false, false)),
	}, "meta": map[string]string{"resourceType": "Schema", "location": h.base + "/Schemas/" + groupSchema}}
	list(w, 2, 1, []map[string]any{user, group})
}
