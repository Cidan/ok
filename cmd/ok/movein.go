package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/directory/ldapdir"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/id/service"
	idstore "github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// identityMoveIn moves an installation in as an organization of a hosted service (docs/hosted.md,
// "Moving an installation in"), from identity's database, with ok's roles stopped: run with the
// installation's configuration, it converts the directory in one transaction, then says what of
// the configuration to change before the roles start again as a hosted service's.
func identityMoveIn(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("identity move-in", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	handle := flags.String("organization", "", "the handle the installation's organization takes (default: tenancy.organization's)")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if flags.NArg() != 0 {
		return usageError("ok identity move-in [-config ok.yaml] [-organization HANDLE]")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if cfg.Tenancy.Hosted() {
		return errors.New("run it with the installation's configuration (tenancy.mode: installation), before changing it to a hosted service's")
	}
	ctx := context.Background()
	b, _, done, err := role.OpenBackend(ctx, cfg, 2)
	if err != nil {
		return err
	}
	defer done()
	st := idstore.New(b, cfg.Names())
	if _, err := st.CheckMode(ctx, false); err != nil {
		return err
	}
	o, err := st.OrganizationByHandle(ctx, cfg.Tenancy.Organization)
	if err != nil {
		return fmt.Errorf("the installation's organization, %s: %w", cfg.Tenancy.Organization, err)
	}
	st.SetInstallation(o.ID)
	provider, scimToken, notes, err := movedProvider(cfg, o.ID)
	if err != nil {
		return err
	}
	admins, err := service.New(service.Config{Store: st, BootstrapAdmins: cfg.Identity.BootstrapAdmins, Installation: o.ID}).
		ListAdmins(ctx, &identitypb.ListAdminsRequest{})
	if err != nil {
		return fmt.Errorf("the installation's administrators: %w", err)
	}
	rep, err := st.MoveIn(ctx, idstore.MoveInPlan{Organization: o.ID, Handle: *handle, Owners: admins.GetPrincipalIds(), Provider: provider,
		SCIMToken: scimToken})
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "moved the installation in as the organization %s", rep.Handle)
	if rep.Renamed {
		fmt.Fprintf(stdout, " (renamed from %s, which redirects)", cfg.Tenancy.Organization)
	}
	fmt.Fprintf(stdout, ":\n  %d repositories, now %s/<name>\n  %d members, %d of them owners (the administrators)\n  %d teams, from its synced groups\n  %d robots, now the organization's\n",
		rep.Repositories, rep.Handle, rep.Members, rep.Owners, rep.Teams, rep.Robots)
	if provider != nil {
		fmt.Fprintf(stdout, "  its system of record, %s, the organization's identity provider, %d people linked to it\n", provider.Source, rep.Linked)
	}
	for _, r := range rep.KeptRobots {
		fmt.Fprintf(stdout, "  kept %s ok's own\n", r)
	}
	notes = append(notes, "set tenancy.mode: hosted", "remove identity.system_of_record (the organization's identity provider is it now)")
	if tr := cfg.Identity.TreeRepository; tr != "" && !strings.Contains(tr, "/") {
		notes = append(notes, fmt.Sprintf("set identity.tree_repository: %s/%s (its full name now)", rep.Handle, tr))
	}
	if provider != nil && cfg.Identity.SealingKeyFile != "" {
		notes = append(notes, "keep identity.sealing_key_file: the provider's secrets are sealed with it")
	}
	fmt.Fprintln(stdout, "then, before ok's roles start again:")
	for _, n := range notes {
		fmt.Fprintf(stdout, "  - %s\n", n)
	}
	return nil
}

// movedProvider is the installation's system of record as its organization's identity provider,
// its secrets sealed; nil for one ok cannot run for an organization (a static file), and notes for
// the operator.
func movedProvider(cfg *config.Config, org string) (*idstore.IdentityProvider, string, []string, error) {
	sor := cfg.Identity.SystemOfRecord
	if sor == nil {
		return nil, "", nil, nil
	}
	if sor.Static != nil {
		return nil, "", []string{"the static system of record does not move: its people stay as they are, the organization with no identity provider"}, nil
	}
	read := func(file, what string) (string, error) {
		if file == "" {
			return "", nil
		}
		v, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("%s: %w", what, err)
		}
		return strings.TrimSpace(string(v)), nil
	}
	var set enterprise.Settings
	var sec enterprise.Secrets
	var scimToken string
	var notes []string
	var err error
	switch {
	case sor.Google != nil:
		g := sor.Google
		set.Google = &enterprise.Google{Subject: g.Subject, Customer: g.Customer, GroupQuery: g.GroupQuery}
		var key string
		if key, err = read(g.KeyFile, "identity.system_of_record.google.key_file"); err != nil {
			return nil, "", nil, err
		}
		sec.GoogleKey = []byte(key)
	case sor.Entra != nil:
		e := sor.Entra
		set.Entra = &enterprise.Entra{TenantID: e.TenantID, ClientID: e.ClientID, GroupFilter: e.GroupFilter, IncludeGuests: e.IncludeGuests}
		if sec.EntraClientSecret, err = read(e.ClientSecretFile, "identity.system_of_record.entra.client_secret_file"); err != nil {
			return nil, "", nil, err
		}
	case sor.LDAP != nil:
		l := sor.LDAP
		ca, err := read(l.CAFile, "identity.system_of_record.ldap.ca_file")
		if err != nil {
			return nil, "", nil, err
		}
		set.LDAP = &enterprise.LDAP{URL: l.URL, StartTLS: l.StartTLS, CA: ca, BindDN: l.BindDN,
			People: ldapdir.People{BaseDN: l.People.BaseDN, Filter: l.People.Filter, DisabledFilter: l.People.DisabledFilter, IDAttribute: l.People.IDAttribute,
				HandleAttribute: l.People.HandleAttribute, EmailAttribute: l.People.EmailAttribute, NameAttribute: l.People.NameAttribute},
			Groups: ldapdir.Groups{BaseDN: l.Groups.BaseDN, Filter: l.Groups.Filter, IDAttribute: l.Groups.IDAttribute, NameAttribute: l.Groups.NameAttribute,
				MemberAttribute: l.Groups.MemberAttribute}}
		if sec.LDAPBindPassword, err = read(l.BindPasswordFile, "identity.system_of_record.ldap.bind_password_file"); err != nil {
			return nil, "", nil, err
		}
		notes = append(notes, "the organization's LDAP directory is reached at a public address alone: one on a private network stays unread")
	case sor.SCIM != nil:
		set.SCIM = &enterprise.SCIM{}
		if scimToken, err = read(sor.SCIM.TokenFile, "identity.system_of_record.scim.token_file"); err != nil {
			return nil, "", nil, err
		}
		notes = append(notes, "point the SCIM provider at <public_url>/id/scim/v2/orgs/<organization>: its token stays the same")
	}
	source, err := set.Source()
	if err != nil {
		return nil, "", nil, err
	}
	raw, err := json.Marshal(set)
	if err != nil {
		return nil, "", nil, err
	}
	p := &idstore.IdentityProvider{Source: source, Settings: raw, Interval: sor.Interval, PlainHandles: true}
	if len(sec.GoogleKey) > 0 || sec.EntraClientSecret != "" || sec.LDAPBindPassword != "" || sec.OIDCClientSecret != "" {
		if cfg.Identity.SealingKeyFile == "" {
			return nil, "", nil, errors.New("identity.sealing_key_file: the system of record's secrets are sealed with it, as an organization's are")
		}
		sealer, err := seal.Load(cfg.Identity.SealingKeyFile)
		if err != nil {
			return nil, "", nil, fmt.Errorf("identity.sealing_key_file: %w", err)
		}
		if p.Sealed, err = enterprise.Seal(sealer, org, sec); err != nil {
			return nil, "", nil, err
		}
	}
	return p, scimToken, notes, nil
}
