package pages

import "github.com/Cidan/ok/internal/control/pages/servicegroup"

// The Services pages' groups (servicegroup), named for the pages that render them.
type (
	ServiceSummary = servicegroup.Service
	ServiceGroup   = servicegroup.Group
)

// ServiceHref is a service's page: its group's, opened on it.
func ServiceHref(id int64) string { return servicegroup.ServicePage(id) }

// serviceGroupTitle is a group's page's document title: what the pane shows, the group's.
func serviceGroupTitle(v ServiceGroupView) string {
	t := v.Group.Name + " · Services · " + Brand
	if v.Service != nil && v.Service.Name != v.Group.Name {
		return v.Service.Name + " · " + t
	}
	return t
}

// serviceGroupCrumbs are where the pane is: the Services page — the content region — then, in a
// group with an overview, the overview and the service the pane shows, in the pane.
func serviceGroupCrumbs(v ServiceGroupView) []Crumb {
	crumbs := []Crumb{{Label: "Services", Href: "/services", Target: "#content"}}
	if !v.Group.Overview() {
		return append(crumbs, Crumb{Label: v.Group.Name})
	}
	if v.Selected == 0 {
		return append(crumbs, Crumb{Label: v.Group.Name})
	}
	crumbs = append(crumbs, Crumb{Label: v.Group.Name, Href: v.Group.Href(), Target: "#service-main", Swap: swapPane})
	if sv, ok := v.Group.Member(v.Selected); ok {
		crumbs = append(crumbs, Crumb{Label: sv.Name})
	}
	return crumbs
}

// serviceNavTitle is a sub-nav row's title: the service, where it is declared, and its status.
func serviceNavTitle(sv ServiceSummary) string {
	t := sv.Name + " · " + sv.Repository + " · " + sv.StatusLabel
	if !sv.Declared {
		t += " · No Longer Declared"
	}
	return t
}
