package worker

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MemoryRoom is a Config.Room for a worker whose cgroup (v2, at dir) has a memory limit: there is
// room for another run while the memory the worker's processes hold and the kernel cannot
// reclaim — anonymous memory, and shared memory's — leaves reserve for it and for each starting
// run. The rest of the cgroup's use is file cache, which the kernel gives back as runs need it.
// A cgroup without a limit, or one the worker cannot read, always has room.
func MemoryRoom(dir string, reserve int64) func(starting int) bool {
	return func(starting int) bool {
		limit, held, ok := memoryUse(dir)
		return !ok || limit-held >= reserve*int64(1+starting)
	}
}

// memoryUse is a cgroup's memory limit and what of its use cannot be reclaimed; ok is false when
// it has no limit, or its files cannot be read.
func memoryUse(dir string) (limit, held int64, ok bool) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if err != nil {
		return 0, 0, false
	}
	limit, err = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64) // "max" has none
	if err != nil {
		return 0, 0, false
	}
	stat, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return 0, 0, false
	}
	sc := bufio.NewScanner(bytes.NewReader(stat))
	for sc.Scan() {
		key, value, _ := strings.Cut(sc.Text(), " ")
		if key != "anon" && key != "shmem" {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		held += n
	}
	return limit, held, true
}
