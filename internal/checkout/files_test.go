package checkout

import (
	"context"
	"errors"
	"io/fs"
	"testing"
)

// FilesAt holds the files asked for that the commit has, and the directories above them.
func TestFilesAt(t *testing.T) {
	src := &fakeSource{commits: map[byte]map[string]string{1: {"a.txt": "one", "lib/b.go": "package b", "link": "->a.txt", "other": "x"}}}
	fsys, err := FilesAt(context.Background(), src, 1, []byte{1}, []string{"lib/b.go", "link", "gone.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if data, err := fs.ReadFile(fsys, "lib/b.go"); err != nil || string(data) != "package b" {
		t.Errorf("lib/b.go: %q, %v", data, err)
	}
	if data, err := fs.ReadFile(fsys, "link"); err != nil || string(data) != "a.txt" {
		t.Errorf("a symlink reads as its target: %q, %v", data, err)
	}
	for _, p := range []string{"gone.txt", "other"} {
		if _, err := fs.ReadFile(fsys, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: %v", p, err)
		}
	}
	if info, err := fs.Stat(fsys, "lib"); err != nil || !info.IsDir() {
		t.Errorf("lib: %v, %v", info, err)
	}
	if _, err := fsys.Open("../x"); err == nil {
		t.Error("an invalid path opened")
	}
}
