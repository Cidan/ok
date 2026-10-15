package worker

import (
	"os"
	"path/filepath"
	"testing"
)

// Room counts what the kernel cannot reclaim against the limit, a reserve for the run and for
// each one starting; a cgroup without a limit always has room.
func TestMemoryRoom(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const gib = 1 << 30
	room := MemoryRoom(dir, 4*gib)
	if !room(0) {
		t.Error("no cgroup files: no room")
	}
	write("memory.max", "max\n")
	write("memory.stat", "anon 1073741824\nfile 21474836480\nshmem 0\n")
	if !room(10) {
		t.Error("no limit: no room")
	}
	write("memory.max", "25769803776\n") // 24 GiB, 1 GiB held, 20 GiB of file cache
	for starting, want := range map[int]bool{0: true, 4: true, 5: false} {
		if got := room(starting); got != want {
			t.Errorf("24 GiB, 1 GiB held, %d starting: room %v, want %v", starting, got, want)
		}
	}
	write("memory.stat", "anon 17179869184\nfile 1073741824\nshmem 5368709120\n") // 21 GiB held
	if room(0) {
		t.Error("21 of 24 GiB held: room for a 4 GiB run beside it")
	}
}
