package keys_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kmsapi "cloud.google.com/go/kms/apiv1"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	"github.com/Cidan/ok/internal/testgcp"
)

func TestKeyringRoot(t *testing.T) {
	ctx := context.Background()
	var o keys.Opener
	root, err := o.Root(ctx, keystest.Keyring(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	other, err := o.Root(ctx, keystest.Keyring(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	keystest.Run(t, root, other)
}

func TestGCPRoot(t *testing.T) {
	ctx := context.Background()
	o := keys.Opener{GCP: keystest.GCPKMS(t)}
	root, err := o.Root(ctx, "gcp-kms:"+keystest.GCPKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	other, err := o.Root(ctx, "gcp-kms:"+keystest.GCPKeys[1])
	if err != nil {
		t.Fatal(err)
	}
	keystest.Run(t, root, other)
}

func TestAWSRoot(t *testing.T) {
	ctx := context.Background()
	o := keys.Opener{AWS: keystest.AWSKMS(t)}
	root, err := o.Root(ctx, "aws-kms:"+keystest.AWSKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	other, err := o.Root(ctx, "aws-kms:"+keystest.AWSKeys[1])
	if err != nil {
		t.Fatal(err)
	}
	keystest.Run(t, root, other)
}

// A keyring's new primary wraps from then on, and its old keys still unwrap what they wrapped.
func TestKeyringRotation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	file := filepath.Join(dir, "keyring")
	first, _ := keys.NewKeyringLine("old")
	if err := os.WriteFile(file, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := keys.OpenKeyring(file)
	if err != nil {
		t.Fatal(err)
	}
	aad := keys.RepositoryKeyAAD(1, 1)
	w, _ := k.Wrap(ctx, []byte("key"), aad)
	second, _ := keys.NewKeyringLine("new")
	if err := os.WriteFile(file, []byte(second+first), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err = keys.OpenKeyring(file)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := k.Unwrap(ctx, w, aad); err != nil || string(got) != "key" {
		t.Errorf("the old key's wrap: %q, %v", got, err)
	}
	if w2, _ := k.Wrap(ctx, []byte("key"), aad); !bytes.HasPrefix(w2, []byte("\x03new")) {
		t.Errorf("the new primary did not wrap: %q", w2[:4])
	}
	for _, bad := range []string{"", "# only a comment\n", "k1 abc\n", "k1 " + strings.Repeat("0", 64) + "\nk1 " + strings.Repeat("1", 64) + "\n", "k 1 x\n"} {
		if _, err := keys.ParseKeyring([]byte(bad)); err == nil {
			t.Errorf("keyring %q taken", bad)
		}
	}
}

// Each layer binds what it is for: a data key or a value moved to another path, digest, or
// repository opens nothing.
func TestSealBindsWhatItIsFor(t *testing.T) {
	k, err := keys.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	var d secrets.Digest
	d[0] = 1
	sealed, err := keys.Seal(k, []byte("hunter2"), keys.ValueAAD(7, "secret/a", d))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := keys.Open(k, sealed, keys.ValueAAD(7, "secret/a", d)); err != nil || string(got) != "hunter2" {
		t.Fatalf("opened %q, %v", got, err)
	}
	var d2 secrets.Digest
	for name, aad := range map[string][]byte{
		"another path":       keys.ValueAAD(7, "secret/b", d),
		"another digest":     keys.ValueAAD(7, "secret/a", d2),
		"another repository": keys.ValueAAD(8, "secret/a", d),
		"a data key's":       keys.DataKeyAAD(7, "secret/a", d, 1),
	} {
		if _, err := keys.Open(k, sealed, aad); !errors.Is(err, keys.ErrOpen) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A path and a digest cannot run together into another pair's bytes.
	if bytes.Equal(keys.ValueAAD(1, "secret/ab", d), keys.ValueAAD(1, "secret/a", d)) {
		t.Error("two values' associated data are alike")
	}
}

// An ID the opener cannot reach is refused, by name.
func TestOpenerRefuses(t *testing.T) {
	var o keys.Opener
	for _, id := range []string{"gcp-kms:projects/x", "aws-kms:arn:x", "vault:x", "nothing", "keyring:" + filepath.Join(t.TempDir(), "none")} {
		if _, err := o.Root(context.Background(), id); err == nil {
			t.Errorf("%s: opened", id)
		}
	}
}

// A KMS is reached when a root first needs it, once.
func TestOpenerDialsOnce(t *testing.T) {
	ctx := context.Background()
	dials := 0
	gcp := keystest.GCPKMS(t)
	o := keys.Opener{DialGCP: func(context.Context) (keys.GCPKMS, error) { dials++; return gcp, nil }}
	for _, k := range keystest.GCPKeys {
		if _, err := o.Root(ctx, "gcp-kms:"+k); err != nil {
			t.Fatal(err)
		}
	}
	if dials != 1 {
		t.Errorf("Cloud KMS dialed %d times", dials)
	}
}

// A root in a real Cloud KMS passes the conformance suite (internal/testgcp).
func TestGCPRootLive(t *testing.T) {
	rootID, otherID := testgcp.KMS(t)
	ctx := context.Background()
	o := &keys.Opener{DialGCP: func(ctx context.Context) (keys.GCPKMS, error) { return kmsapi.NewKeyManagementClient(ctx) }}
	root, err := o.Root(ctx, rootID)
	if err != nil {
		t.Fatal(err)
	}
	other, err := o.Root(ctx, otherID)
	if err != nil {
		t.Fatal(err)
	}
	keystest.Run(t, root, other)
}
