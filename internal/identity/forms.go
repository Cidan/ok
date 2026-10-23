package identity

import "strings"

// The two presentations of names (docs/hosted.md, "Names"): a principal has one name in the
// directory, DNS-like — ada.human.example.com, eng.acme.team.example.com — and is shown as its
// mode has it: an installation's people by handle and everything else whole, as always; a hosted
// service's @ada (a person), @acme/eng (a team), acme/ci (an organization's robot), acme (an
// organization). What people write is parsed in either form in either mode, so an installation's
// OWNERS files keep working once it moves in.

// Present is how a name is shown, hosted or not. A name outside the domain is shown as it is.
func (n Names) Present(name string, hosted bool) string {
	k, ok := n.KindOf(name)
	if !ok {
		return name
	}
	labels := strings.Split(strings.TrimSuffix(name, "."+n.domain), ".")
	switch {
	case k == Human && hosted:
		return "@" + labels[0]
	case k == Human:
		return labels[0]
	case !hosted:
		return name
	case k == TeamMember:
		return "@" + labels[1] + "/" + labels[0]
	case k == Robot && len(labels) == 3:
		return labels[1] + "/" + labels[0]
	case k == Org, k == Robot, k == GroupMember, k == ACLMember:
		return labels[0]
	}
	return name
}

// ParsePrincipal is the directory's name of a principal someone wrote: a full name as it is;
// @ada or ada a person; acme/ci an organization's robot (ok/scm one of ok's own). ok is false for
// what names none of them.
func (n Names) ParsePrincipal(form string) (string, bool) {
	form = strings.ToLower(strings.TrimSpace(form))
	if _, ok := n.KindOf(form); ok {
		return form, true
	}
	if owner, name, ok := strings.Cut(form, "/"); ok && !strings.HasPrefix(form, "@") {
		if ValidLabel(owner) && ValidLabel(name) {
			return n.Robot(name + "." + owner), true
		}
		return "", false
	}
	h := strings.TrimPrefix(form, "@")
	if !ValidLabel(h) {
		return "", false
	}
	return n.Human(h), true
}

// ParseSet is the directory's name of a set someone wrote for approval: a full name as it is; eng
// an ACL the groups-and-ACLs tree declares; @acme/eng (or acme/eng) an organization's team. ok is
// false for what names neither.
func (n Names) ParseSet(form string) (string, bool) {
	form = strings.ToLower(strings.TrimSpace(form))
	if k, ok := n.KindOf(form); ok {
		return form, k == ACLMember || k == GroupMember || k == TeamMember
	}
	if org, team, ok := strings.Cut(strings.TrimPrefix(form, "@"), "/"); ok {
		if ValidLabel(org) && ValidLabel(team) {
			return n.Team(team, org), true
		}
		return "", false
	}
	if !ValidLabel(form) {
		return "", false
	}
	return n.Of(ACL, form), true
}
