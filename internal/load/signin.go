package load

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/dial"
)

// signInWait bounds one person's sign-in, which waits out a server's restart meanwhile.
const signInWait = 5 * time.Minute

// SignIn signs a person in as they would sign a machine in (ok auth login): a device login, which
// it approves as the person would in a browser — signing in at the installation's OpenID Connect
// provider with a password (Dex's password login), then approving the device's code — keeping the
// credential in dir. Its calls wait for a connection, for signInWait at most: a run's sign-ins
// outlast a server's replicas rolling.
func SignIn(ctx context.Context, server, dir, login, password string) (dial.Credential, error) {
	srv, err := auth.ParseServer(server)
	if err != nil {
		return nil, err
	}
	srv.WaitForReady = true
	ctx, cancel := context.WithTimeout(ctx, signInWait)
	defer cancel()
	approved := make(chan error, 1)
	if _, err := auth.Login(ctx, srv, dir, "ok load", io.Discard, func(u string) error {
		go func() {
			err := approve(ctx, u, login, password)
			approved <- err
			if err != nil {
				// No approval is coming: the login it would have ended ends now.
				cancel()
			}
		}()
		return nil
	}); err != nil {
		select {
		case aerr := <-approved:
			if aerr != nil {
				return nil, fmt.Errorf("signing %s in: %w", login, aerr)
			}
		default:
		}
		return nil, fmt.Errorf("signing %s in: %w", login, err)
	}
	if err := <-approved; err != nil {
		return nil, err
	}
	return auth.Load(dir)
}

// page is what a page holds that a browser's person would act on: its links and its forms.
type page struct {
	at    *url.URL
	body  string
	links []string
	forms []form
}

type form struct {
	action, method string
	fields         map[string]string
}

// parse reads a page's links and forms.
func parse(at *url.URL, body string) page {
	p := page{at: at, body: body}
	z := html.NewTokenizer(strings.NewReader(body))
	var cur *form
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			return p
		}
		t := z.Token()
		attr := func(k string) string {
			for _, a := range t.Attr {
				if a.Key == k {
					return a.Val
				}
			}
			return ""
		}
		switch {
		case tt == html.EndTagToken && t.Data == "form":
			cur = nil
		case t.Data == "a" && attr("href") != "":
			p.links = append(p.links, attr("href"))
		case t.Data == "form" && tt != html.EndTagToken:
			p.forms = append(p.forms, form{action: attr("action"), method: strings.ToLower(attr("method")), fields: map[string]string{}})
			cur = &p.forms[len(p.forms)-1]
		case t.Data == "input" && cur != nil && attr("name") != "":
			cur.fields[attr("name")] = attr("value")
		}
	}
}

// approve approves a device login at its page, signing in there first.
func approve(ctx context.Context, start, login, password string) error {
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, Timeout: 30 * time.Second}
	fetch := func(method, u string, data url.Values) (page, error) {
		var body io.Reader
		if data != nil {
			body = strings.NewReader(data.Encode())
		}
		req, err := http.NewRequestWithContext(ctx, method, u, body)
		if err != nil {
			return page{}, err
		}
		if data != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		resp, err := c.Do(req)
		if err != nil {
			return page{}, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return page{}, err
		}
		if resp.StatusCode >= 400 {
			return page{}, fmt.Errorf("%s %s: %s", method, resp.Request.URL, resp.Status)
		}
		return parse(resp.Request.URL, string(b)), nil
	}
	resolve := func(p page, ref string) string {
		r, err := url.Parse(ref)
		if err != nil {
			return ref
		}
		return p.at.ResolveReference(r).String()
	}
	p, err := fetch(http.MethodGet, start, nil)
	for step := 0; err == nil && step < 10; step++ {
		switch {
		case strings.HasSuffix(p.at.Path, "/id/device") && hasForm(p, "/id/device"):
			f := formOf(p, "/id/device")
			v := url.Values{}
			for k, val := range f.fields {
				v.Set(k, val)
			}
			v.Set("decision", "approve")
			if p, err = fetch(http.MethodPost, resolve(p, f.action), v); err != nil {
				return err
			}
			if !strings.Contains(p.body, "Approved") {
				return errors.New("the device page did not approve the sign-in")
			}
			return nil
		case linkWith(p, "/id/login/oidc/") != "":
			p, err = fetch(http.MethodGet, resolve(p, linkWith(p, "/id/login/oidc/")), nil)
		case formWithField(p, "password") != nil:
			f := formWithField(p, "password")
			v := url.Values{}
			for k, val := range f.fields {
				v.Set(k, val)
			}
			v.Set("login", login)
			v.Set("password", password)
			action := f.action
			if action == "" {
				action = p.at.String()
			}
			p, err = fetch(http.MethodPost, resolve(p, action), v)
		case linkWith(p, "/auth/local") != "":
			p, err = fetch(http.MethodGet, resolve(p, linkWith(p, "/auth/local")), nil)
		default:
			return fmt.Errorf("signing in, stuck at %s", p.at)
		}
	}
	if err != nil {
		return err
	}
	return errors.New("signing in took too many steps")
}

func linkWith(p page, part string) string {
	for _, l := range p.links {
		if strings.Contains(l, part) {
			return l
		}
	}
	return ""
}

func hasForm(p page, action string) bool { return formOf(p, action) != nil }

func formOf(p page, action string) *form {
	for i := range p.forms {
		if strings.HasSuffix(p.forms[i].action, action) {
			return &p.forms[i]
		}
	}
	return nil
}

func formWithField(p page, field string) *form {
	for i := range p.forms {
		if _, ok := p.forms[i].fields[field]; ok {
			return &p.forms[i]
		}
	}
	return nil
}
