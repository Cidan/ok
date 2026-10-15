package scim_test

import (
	"testing"

	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/directory/scim/scimtest"
)

func TestMemory(t *testing.T) {
	scimtest.Run(t, func(*testing.T) scim.Store { return scim.NewMemory() })
}
