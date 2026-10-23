#!/usr/bin/env python3
"""Approves ok's device sign-in as Dex's static person, as a browser would (staging's run).

Usage: device-login.py VERIFICATION_URL PASSWORD_FILE
Reaches the in-cluster hosts through local port-forwards: ok-server.ok.svc:8472 on 127.0.0.1:18472,
dex.dex.svc:5556 on 127.0.0.1:15556.
"""
import html.parser
import http.client
import http.cookiejar
import socket
import sys
import urllib.parse
import urllib.request

ROUTES = {("ok-server.ok.svc", 8472): ("127.0.0.1", 18472), ("dex.dex.svc", 5556): ("127.0.0.1", 15556)}


class Routed(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.create_connection(ROUTES.get((self.host, self.port), (self.host, self.port)), self.timeout)


class RoutedHandler(urllib.request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(Routed, req)


class Page(html.parser.HTMLParser):
    """A page's links and forms."""

    def __init__(self):
        super().__init__()
        self.links, self.forms, self._form = [], [], None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a" and a.get("href"):
            self.links.append(a["href"])
        elif tag == "form":
            self._form = {"action": a.get("action", ""), "method": a.get("method", "get").lower(), "fields": {}, "buttons": []}
            self.forms.append(self._form)
        elif tag in ("input", "button") and self._form is not None and a.get("name"):
            if tag == "button" or a.get("type") == "submit":
                self._form["buttons"].append((a["name"], a.get("value", "")))
            else:
                self._form["fields"][a["name"]] = a.get("value", "")

    def handle_endtag(self, tag):
        if tag == "form":
            self._form = None


def main():
    url, password = sys.argv[1], open(sys.argv[2]).read().strip()
    opener = urllib.request.build_opener(RoutedHandler(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def fetch(u, data=None):
        resp = opener.open(urllib.request.Request(u, data=urllib.parse.urlencode(data).encode() if data else None), timeout=30)
        body = resp.read().decode("utf-8", "replace")
        p = Page()
        p.feed(body)
        print(f"  {resp.status} {resp.geturl()}", file=sys.stderr)
        return resp.geturl(), body, p

    at, body, page = fetch(url)
    for _ in range(8):
        if "/id/device" in at and any(f["action"].endswith("/id/device") for f in page.forms):
            form = next(f for f in page.forms if f["action"].endswith("/id/device"))
            data = dict(form["fields"], decision="approve")
            at, body, page = fetch(urllib.parse.urljoin(at, form["action"]), data)
            if "Approved" not in body:
                sys.exit(f"the device page did not approve:\n{body[:2000]}")
            print("approved")
            return
        login = [l for l in page.links if "/id/login/oidc/" in l]
        if login:
            at, body, page = fetch(urllib.parse.urljoin(at, login[0]))
            continue
        local = [l for l in page.links if "/auth/local" in l]
        if local and not any("password" in f["fields"] for f in page.forms):
            at, body, page = fetch(urllib.parse.urljoin(at, local[0]))
            continue
        pw = [f for f in page.forms if "password" in f["fields"]]
        if pw:
            data = dict(pw[0]["fields"], login="load@example.com", password=password)
            at, body, page = fetch(urllib.parse.urljoin(at, pw[0]["action"] or at), data)
            continue
        sys.exit(f"stuck at {at}:\n{body[:2000]}")
    sys.exit("too many steps")


if __name__ == "__main__":
    main()
