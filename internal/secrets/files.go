package secrets

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// DirEnv names, to a deploy's migrations and a post job, the directory their secrets are in
// (docs/secrets.md, "Readers that are not people"): each a file at its path below //secret.
const DirEnv = "OK_SECRETS_DIR"

// WriteDir writes values, by secret path, as files under dir — secret/prod/db-password as
// dir/prod/db-password — each readable by its owner alone, in directories only its owner enters.
// dir must exist and be empty; a caller makes it on a memory file system, of its own, and removes
// it when the values are no longer needed.
func WriteDir(dir string, values map[string][]byte) error {
	paths := make([]string, 0, len(values))
	for p := range values {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	for _, p := range paths {
		if !ValidPath(p) {
			return fmt.Errorf("%q is not a secret's path", p)
		}
		if len(values[p]) > MaxValue {
			return fmt.Errorf("%s: a value of %d bytes", p, len(values[p]))
		}
		dst := filepath.Join(dir, filepath.FromSlash(strings.TrimPrefix(p, Dir+"/")))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(dst, values[p], 0o400); err != nil {
			return err
		}
	}
	return nil
}

// TempDir writes values in a directory of their own on a memory file system — WriteDir's layout —
// and returns it, with what removes it once the values are no longer needed. No values, no
// directory: "" and a no-op.
func TempDir(values map[string][]byte) (dir string, remove func(), err error) {
	if len(values) == 0 {
		return "", func() {}, nil
	}
	parent, err := memoryDir()
	if err != nil {
		return "", nil, err
	}
	if dir, err = os.MkdirTemp(parent, "ok-secrets-"); err != nil {
		return "", nil, err
	}
	remove = func() { _ = os.RemoveAll(dir) }
	if err := WriteDir(dir, values); err != nil {
		remove()
		return "", nil, err
	}
	return dir, remove, nil
}

// MinRedacted is the shortest value Redacted replaces: shorter ones cannot be told from ordinary
// text, and replacing every "a" would say more than it hides.
const MinRedacted = 4

// Redacted replaces every value's appearance in text — a migration's output, a job's log — with
// what names it, longest values first, so one that holds another is replaced whole. Values shorter
// than MinRedacted are left.
func Redacted(text []byte, values map[string][]byte) []byte {
	type named struct {
		path  string
		value []byte
	}
	var vs []named
	for p, v := range values {
		if len(v) >= MinRedacted {
			vs = append(vs, named{p, v})
		}
	}
	slices.SortFunc(vs, func(a, b named) int {
		if len(a.value) != len(b.value) {
			return len(b.value) - len(a.value)
		}
		return strings.Compare(a.path, b.path)
	})
	for _, v := range vs {
		text = bytes.ReplaceAll(text, v.value, []byte("[secret "+Ref(v.path)+"]"))
	}
	return text
}
