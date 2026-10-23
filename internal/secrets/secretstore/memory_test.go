package secretstore_test

import (
	"testing"

	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/secretstore/storetest"
)

func TestMemory(t *testing.T) { storetest.Run(t, secretstore.NewMemory()) }
