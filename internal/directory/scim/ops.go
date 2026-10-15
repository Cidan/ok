package scim

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// condition is one comparison of a filter: attr eq value.
type condition struct {
	attr  string // lowercased: username, externalid, emails.value, …
	op    string // eq, ne, co, sw
	value string
}

// filter is conditions all of which hold (SCIM's and). Provisioning clients send no more; a filter
// using or, not, or grouping is refused as invalidFilter.
type filter []condition

var errFilter = errors.New("a filter here is comparisons joined by and: attr eq \"value\"")

// parseFilter reads a filter: comparisons (eq, ne, co, sw) of an attribute with a value — quoted,
// or bare as some clients send it — joined by and.
func parseFilter(s string) (filter, error) {
	toks, err := tokens(s)
	if err != nil {
		return nil, err
	}
	var f filter
	for i := 0; i < len(toks); {
		if i+3 > len(toks) {
			return nil, errFilter
		}
		op := strings.ToLower(toks[i+1].text)
		if toks[i].quoted || toks[i+1].quoted || (op != "eq" && op != "ne" && op != "co" && op != "sw") {
			return nil, errFilter
		}
		attr := strings.ToLower(toks[i].text)
		// emails[type eq "work"].value compares the work address; ok compares any address.
		if j := strings.IndexByte(attr, '['); j >= 0 {
			if k := strings.LastIndexByte(attr, ']'); k > j {
				attr = attr[:j] + attr[k+1:]
			}
		}
		f = append(f, condition{attr: attr, op: op, value: toks[i+2].text})
		i += 3
		if i < len(toks) {
			if strings.ToLower(toks[i].text) != "and" || toks[i].quoted || i+1 == len(toks) {
				return nil, errFilter
			}
			i++
		}
	}
	if len(f) == 0 {
		return nil, errFilter
	}
	return f, nil
}

type token struct {
	text   string
	quoted bool
}

// tokens splits a filter at spaces outside quotes and brackets.
func tokens(s string) ([]token, error) {
	var out []token
	i := 0
	for i < len(s) {
		if unicode.IsSpace(rune(s[i])) {
			i++
			continue
		}
		if s[i] == '"' {
			var b strings.Builder
			i++
			for ; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				b.WriteByte(s[i])
			}
			if i == len(s) {
				return nil, errFilter
			}
			i++
			out = append(out, token{text: b.String(), quoted: true})
			continue
		}
		j, depth := i, 0
		for ; j < len(s); j++ {
			switch s[j] {
			case '[':
				depth++
			case ']':
				depth--
			case '"':
				if depth == 0 {
					return nil, errFilter
				}
			}
			if depth == 0 && unicode.IsSpace(rune(s[j])) {
				break
			}
		}
		if s[i] == '(' || s[i] == ')' {
			return nil, errFilter
		}
		out = append(out, token{text: s[i:j]})
		i = j
	}
	return out, nil
}

// compare applies an operator to a resource's value and a filter's.
func compare(op, have, want string, caseExact bool) bool {
	if !caseExact {
		have, want = strings.ToLower(have), strings.ToLower(want)
	}
	switch op {
	case "eq":
		return have == want
	case "ne":
		return have != want
	case "co":
		return strings.Contains(have, want)
	case "sw":
		return strings.HasPrefix(have, want)
	}
	return false
}

// matchUser reports whether a user meets a filter. An attribute ok does not keep never matches:
// a client asking whether a user has a manager is told no.
func (f filter) matchUser(u User) bool {
	for _, c := range f {
		var vals []string
		caseExact := false
		switch c.attr {
		case "id":
			vals, caseExact = []string{u.ID}, true
		case "username":
			vals = []string{u.UserName}
		case "externalid":
			vals, caseExact = []string{u.ExternalID}, true
		case "displayname":
			vals = []string{u.DisplayName}
		case "emails", "emails.value":
			for _, e := range u.Emails {
				vals = append(vals, e.Value)
			}
		case "active":
			vals = []string{strconv.FormatBool(u.Active)}
		case "name.givenname":
			vals = []string{u.Name.GivenName}
		case "name.familyname":
			vals = []string{u.Name.FamilyName}
		default:
			return false
		}
		ok := false
		for _, v := range vals {
			ok = ok || compare(c.op, v, c.value, caseExact)
		}
		if !ok {
			return false
		}
	}
	return true
}

// matchGroup reports whether a group meets a filter.
func (f filter) matchGroup(g Group) bool {
	for _, c := range f {
		var vals []string
		caseExact := false
		switch c.attr {
		case "id":
			vals, caseExact = []string{g.ID}, true
		case "displayname":
			vals = []string{g.DisplayName}
		case "externalid":
			vals, caseExact = []string{g.ExternalID}, true
		case "members", "members.value":
			vals, caseExact = g.Members, true
		default:
			return false
		}
		ok := false
		for _, v := range vals {
			ok = ok || compare(c.op, v, c.value, caseExact)
		}
		if !ok {
			return false
		}
	}
	return true
}

// patchOp is one PATCH operation.
type patchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

type patchRequest struct {
	Operations []patchOp `json:"Operations"`
}

// errPatch is a PATCH ok cannot apply.
type errPatch struct{ msg string }

func (e errPatch) Error() string { return e.msg }

// asBool reads a boolean as clients send it: true, or "True".
func asBool(raw json.RawMessage) (bool, error) {
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return b, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.ParseBool(strings.ToLower(s)); err == nil {
			return v, nil
		}
	}
	return false, errPatch{"active is true or false"}
}

func asString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errPatch{fmt.Sprintf("%s is not a string", raw)}
	}
	return s, nil
}

// patchUser applies PATCH operations to a user.
func patchUser(u User, ops []patchOp) (User, error) {
	for _, op := range ops {
		kind := strings.ToLower(op.Op)
		if kind != "add" && kind != "replace" && kind != "remove" {
			return u, errPatch{fmt.Sprintf("op %q is add, replace, or remove", op.Op)}
		}
		if op.Path == "" {
			// No path: the value is attributes, each replaced (dotted and filtered paths too).
			var attrs map[string]json.RawMessage
			if err := json.Unmarshal(op.Value, &attrs); err != nil {
				return u, errPatch{"an operation without a path has an object of attributes as its value"}
			}
			for path, v := range attrs {
				var err error
				if u, err = setUser(u, kind, path, v); err != nil {
					return u, err
				}
			}
			continue
		}
		var err error
		if u, err = setUser(u, kind, op.Path, op.Value); err != nil {
			return u, err
		}
	}
	return u, nil
}

// setUser sets (or, for remove, clears) one attribute of a user.
func setUser(u User, kind, path string, v json.RawMessage) (User, error) {
	p := strings.ToLower(path)
	if strings.HasPrefix(p, "urn:ietf:params:scim:schemas:core:2.0:user:") {
		p = strings.TrimPrefix(p, "urn:ietf:params:scim:schemas:core:2.0:user:")
	}
	str := func() (string, error) {
		if kind == "remove" {
			return "", nil
		}
		return asString(v)
	}
	var err error
	switch {
	case p == "username":
		var s string
		if s, err = str(); err == nil {
			if s == "" {
				return u, errPatch{"userName is required"}
			}
			u.UserName = s
		}
	case p == "externalid":
		u.ExternalID, err = str()
	case p == "displayname":
		u.DisplayName, err = str()
	case p == "active":
		if kind == "remove" {
			u.Active = false
		} else {
			u.Active, err = asBool(v)
		}
	case p == "name":
		if kind == "remove" {
			u.Name = Name{}
		} else if err = json.Unmarshal(v, &u.Name); err != nil {
			err = errPatch{"name is an object"}
		}
	case p == "name.givenname":
		u.Name.GivenName, err = str()
	case p == "name.familyname":
		u.Name.FamilyName, err = str()
	case p == "name.formatted":
		u.Name.Formatted, err = str()
	case p == "emails":
		if kind == "remove" {
			u.Emails = nil
		} else {
			var es []Email
			if err = json.Unmarshal(v, &es); err != nil {
				err = errPatch{"emails is a list of addresses"}
			} else if kind == "add" {
				u.Emails = append(u.Emails, es...)
			} else {
				u.Emails = es
			}
		}
	case strings.HasPrefix(p, "emails[") && strings.HasSuffix(p, "].value"):
		// emails[type eq "work"].value: the address of that type.
		typ, ok := filterType(path)
		if !ok {
			return u, errPatch{fmt.Sprintf("path %q is not emails[type eq \"…\"].value", path)}
		}
		var s string
		if s, err = str(); err == nil {
			found := false
			for i := range u.Emails {
				if strings.EqualFold(u.Emails[i].Type, typ) {
					found = true
					if kind == "remove" {
						u.Emails = append(u.Emails[:i], u.Emails[i+1:]...)
					} else {
						u.Emails[i].Value = s
					}
					break
				}
			}
			if !found && kind != "remove" {
				u.Emails = append(u.Emails, Email{Value: s, Type: typ, Primary: len(u.Emails) == 0})
			}
		}
	default:
		// Attributes ok does not keep — a phone number, a manager — are accepted and dropped.
	}
	return u, err
}

// filterType is the type a path's filter names: emails[type eq "work"].value is work.
func filterType(path string) (string, bool) {
	i, j := strings.IndexByte(path, '['), strings.LastIndexByte(path, ']')
	if i < 0 || j < i {
		return "", false
	}
	f, err := parseFilter(path[i+1 : j])
	if err != nil || len(f) != 1 || f[0].attr != "type" || f[0].op != "eq" {
		return "", false
	}
	return f[0].value, true
}

// memberValues reads a list of members: [{"value": id}, …].
func memberValues(raw json.RawMessage) ([]string, error) {
	var ms []struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &ms); err != nil {
		var one struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(raw, &one); err != nil || one.Value == "" {
			return nil, errPatch{"members is a list of {\"value\": id}"}
		}
		return []string{one.Value}, nil
	}
	var out []string
	for _, m := range ms {
		if m.Value != "" {
			out = append(out, m.Value)
		}
	}
	return out, nil
}

// patchGroup applies PATCH operations to a group.
func patchGroup(g Group, ops []patchOp) (Group, error) {
	for _, op := range ops {
		kind := strings.ToLower(op.Op)
		p := strings.ToLower(op.Path)
		switch {
		case kind != "add" && kind != "replace" && kind != "remove":
			return g, errPatch{fmt.Sprintf("op %q is add, replace, or remove", op.Op)}
		case p == "members":
			var ids []string
			if len(op.Value) > 0 && string(op.Value) != "null" {
				var err error
				if ids, err = memberValues(op.Value); err != nil {
					return g, err
				}
			}
			switch kind {
			case "add":
				g.Members = union(g.Members, ids)
			case "replace":
				g.Members = union(nil, ids)
			case "remove":
				if len(ids) == 0 {
					g.Members = nil
				} else {
					g.Members = minus(g.Members, ids)
				}
			}
		case strings.HasPrefix(p, "members[") && kind == "remove":
			// members[value eq "id"]
			f, err := parseFilter(op.Path[len("members[") : len(op.Path)-1])
			if err != nil || len(f) != 1 || f[0].attr != "value" || f[0].op != "eq" {
				return g, errPatch{fmt.Sprintf("path %q is not members[value eq \"id\"]", op.Path)}
			}
			g.Members = minus(g.Members, []string{f[0].value})
		case p == "displayname":
			s, err := asString(op.Value)
			if err != nil || s == "" {
				return g, errPatch{"displayName is a string, and required"}
			}
			g.DisplayName = s
		case p == "externalid":
			if kind == "remove" {
				g.ExternalID = ""
			} else {
				s, err := asString(op.Value)
				if err != nil {
					return g, err
				}
				g.ExternalID = s
			}
		case p == "":
			var attrs map[string]json.RawMessage
			if err := json.Unmarshal(op.Value, &attrs); err != nil {
				return g, errPatch{"an operation without a path has an object of attributes as its value"}
			}
			for k, v := range attrs {
				var err error
				if g, err = patchGroup(g, []patchOp{{Op: op.Op, Path: k, Value: v}}); err != nil {
					return g, err
				}
			}
		default:
			// Attributes ok does not keep are accepted and dropped.
		}
	}
	return g, nil
}

func union(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range append(append([]string(nil), a...), b...) {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func minus(a, b []string) []string {
	drop := map[string]bool{}
	for _, x := range b {
		drop[x] = true
	}
	var out []string
	for _, x := range a {
		if !drop[x] {
			out = append(out, x)
		}
	}
	return out
}
