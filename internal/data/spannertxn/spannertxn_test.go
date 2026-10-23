package spannertxn_test

import (
	"testing"

	"github.com/Cidan/ok/internal/data/spannertxn"
)

// begin stands where ReadWrite and Begun call callSite: the frame it skips.
func begin() string { return spannertxn.CallSite() }

type store struct{}

func (store) inTx() string { return begin() }

func (store) Txn() string {
	run := func() string { return store{}.inTx() }
	return run()
}

func publish() string { return store{}.inTx() }

func land() string { return store{}.Txn() }

// A transaction is named for the code that began it, past ok's transaction wrappers and the
// closures within them.
func TestCallSite(t *testing.T) {
	for _, c := range []struct {
		got, want string
	}{
		{publish(), "data/spannertxn_test.publish"},
		{land(), "data/spannertxn_test.land"},
	} {
		if c.got != c.want {
			t.Errorf("site %q, want %q", c.got, c.want)
		}
	}
}
