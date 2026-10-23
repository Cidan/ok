package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A deploy's and a post job's secrets are files below their directory, each its owner's alone.
func TestWriteDir(t *testing.T) {
	dir := t.TempDir()
	values := map[string][]byte{"secret/token": []byte("t0ken"), "secret/prod/db-password": []byte("hunter2")}
	if err := WriteDir(dir, values); err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]string{"token": "t0ken", "prod/db-password": "hunter2"} {
		f := filepath.Join(dir, p)
		got, err := os.ReadFile(f)
		if err != nil || string(got) != want {
			t.Errorf("%s: %q, %v", p, got, err)
		}
		if st, _ := os.Stat(f); st.Mode().Perm() != 0o400 {
			t.Errorf("%s: mode %v", p, st.Mode())
		}
	}
	if st, _ := os.Stat(filepath.Join(dir, "prod")); st.Mode().Perm() != 0o700 {
		t.Errorf("prod/: mode %v", st.Mode())
	}
	if err := WriteDir(t.TempDir(), map[string][]byte{"secret/acl.yaml": nil}); err == nil {
		t.Error("wrote an acl.yaml as a secret")
	}
}

// What a migration or a job printed names a secret where its value was, longest first; a value
// too short to tell from text is left.
func TestRedacted(t *testing.T) {
	values := map[string][]byte{"secret/a": []byte("hunter2"), "secret/b": []byte("hunter2hunter2"), "secret/c": []byte("ab")}
	got := string(Redacted([]byte("pw=hunter2hunter2 old=hunter2 ab"), values))
	if want := "pw=[secret //secret/b] old=[secret //secret/a] ab"; got != want {
		t.Errorf("redacted: %q, want %q", got, want)
	}
}

// TempDir writes values on a memory file system, each its owner's alone, and removes them.
func TestTempDir(t *testing.T) {
	if _, err := memoryDir(); err != nil {
		t.Skip(err)
	}
	dir, remove, err := TempDir(map[string][]byte{"secret/prod/db": []byte("hunter2")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(dir, "/dev/shm/ok-secrets-") {
		t.Errorf("made %s", dir)
	}
	st, err := os.Stat(filepath.Join(dir, "prod", "db"))
	if err != nil || st.Mode().Perm() != 0o400 {
		t.Errorf("the value's file: %v, %v", st, err)
	}
	remove()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("left behind: %v", err)
	}
	if dir, remove, err := TempDir(nil); err != nil || dir != "" {
		t.Errorf("no values: %q, %v", dir, err)
	} else {
		remove()
	}
}
