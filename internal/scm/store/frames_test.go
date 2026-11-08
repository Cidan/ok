package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// recordsPack writes a pack of version 2 — records, each compressed alone, as packs were before
// frames — of a network's records, stored as their locations say, and moves them to it as compaction
// would have; the packs they were in are left with nothing live.
func recordsPack(t *testing.T, s *Store, repo int64) int {
	t.Helper()
	ctx := context.Background()
	net := netOf(t, s, repo)
	v, err := s.contentView(ctx, backend.NetworkScope(net))
	if err != nil {
		t.Fatal(err)
	}
	type rec struct {
		id   encoding.ID
		kind encoding.Kind
		raw  []byte
	}
	var recs []rec
	byPack := map[uint64][]contentindex.Keyed{}
	if err := v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if contentindex.Located(e.Kind) {
			byPack[e.Loc.Pack] = append(byPack[e.Loc.Pack], contentindex.Keyed{ID: id, Entry: e})
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	li := s.live(backend.NetworkScope(net))
	for num, ks := range byPack {
		pid, err := s.packID(ctx, net, li, num)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range ks {
			l := packLocation(k.Entry.Loc)
			stored, err := s.blobs.GetRange(ctx, packKey(net, pid), l.DataOffset(), int64(l.StoredLength))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := pack.Read(s.dicts.of(net), k.Entry.Kind, k.ID, l, stored)
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, rec{k.ID, k.Entry.Kind, bytes.Clone(raw)})
		}
	}
	var buf bytes.Buffer
	buf.WriteString("OKSCMPAK")
	buf.Write(binary.LittleEndian.AppendUint32(nil, 2))
	buf.Write(bytes.Repeat([]byte{7}, 16))
	type entry struct {
		id  encoding.ID
		off int64
	}
	var index []entry
	locs := map[encoding.ID]pack.Location{}
	for _, r := range recs {
		off := int64(buf.Len())
		buf.Write(r.id[:])
		buf.WriteByte(byte(pack.Stored))
		buf.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(r.raw))))
		buf.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(r.raw))))
		buf.Write(r.raw)
		index = append(index, entry{r.id, off})
		locs[r.id] = pack.Location{Offset: off, Codec: pack.Stored, RawLength: uint32(len(r.raw)), StoredLength: uint32(len(r.raw))}
	}
	slices.SortFunc(index, func(a, b entry) int { return bytes.Compare(a.id[:], b.id[:]) })
	indexOff := buf.Len()
	buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(index))))
	for _, e := range index {
		buf.Write(e.id[:])
		buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(e.off)))
	}
	buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(indexOff)))
	buf.WriteString("OKSCMEND")
	id := pack.ID(buf.Bytes())
	if _, err := pack.Verify(bytes.NewReader(buf.Bytes()), int64(buf.Len()), id, nil); err != nil {
		t.Fatalf("the version 2 pack: %v", err)
	}
	if err := s.blobs.Put(ctx, packKey(net, id), bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
		t.Fatal(err)
	}
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		num, err := tx.InsertPack(ctx, net, id, int64(buf.Len()), 0)
		if err != nil {
			return err
		}
		for _, r := range recs {
			s.logIndex(tx, backend.NetworkScope(net), contentindex.Update{Op: contentindex.Move, ID: r.id, Entry: contentindex.Entry{
				Kind: r.kind, Loc: indexLocation(num, locs[r.id])}})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return len(recs)
}

// locatedEntries is a network's chunk index, whole, read by a store with nothing cached.
func locatedEntries(t *testing.T, s *Store, repo int64) map[encoding.ID]contentindex.Entry {
	t.Helper()
	ctx := context.Background()
	v, err := reopen(t, s).contentView(ctx, backend.NetworkScope(netOf(t, s, repo)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[encoding.ID]contentindex.Entry{}
	if err := v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if contentindex.Located(e.Kind) {
			out[id] = e
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// Packs written before frames — records, each compressed alone — are read where the index says, and
// a GC pass rewrites every one into frames: small files many to a frame, the rest each a frame of
// its own, every file reading back the same, fsck finding nothing wrong.
func TestCompactionUpgradesRecordPacks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	files := map[string]string{}
	for i := range 40 {
		files[fmt.Sprintf("dir%d/f%02d.go", i%4, i)] = string(goFile(i))
	}
	big := string(bytesOf(600_000, 9))
	files["big.bin"] = big
	e.landChange("files", files)
	n := recordsPack(t, e.s, e.repo)
	before := locatedEntries(t, e.s, e.repo)
	for id, en := range before {
		if en.Loc.Framed {
			t.Fatalf("%s %s is in a frame, not the version 2 pack's record", en.Kind, id)
		}
	}
	if len(before) != n {
		t.Fatalf("%d located, %d records written", len(before), n)
	}
	for path, c := range files {
		if got := readAll(t, reopen(t, e.s), e.repo, fileID(t, c)); string(got) != c {
			t.Fatalf("%s reads back wrong from the version 2 pack", path)
		}
	}
	clean(t, e.s, e.repo)

	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.PacksUpgraded != 1 || r.PacksWritten != 1 || r.RecordsFramed < 40 {
		t.Fatalf("the pass did not rewrite the version 2 pack into frames: %s", r)
	}
	after := locatedEntries(t, e.s, e.repo)
	if len(after) != len(before) {
		t.Fatalf("%d located after, %d before", len(after), len(before))
	}
	frames := map[int64]int{}
	for id, en := range after {
		l := packLocation(en.Loc)
		if !l.Framed {
			t.Errorf("%s %s is not in a frame after the pass", en.Kind, id)
		}
		frames[l.Offset]++
	}
	if len(frames) >= len(after)/2 {
		t.Errorf("%d records in %d frames: small files were not framed together", len(after), len(frames))
	}
	for path, c := range files {
		if got := readAll(t, reopen(t, e.s), e.repo, fileID(t, c)); string(got) != c {
			t.Fatalf("%s reads back wrong from frames", path)
		}
	}
	clean(t, e.s, e.repo)
	again, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if again.PacksWritten != 0 || again.PacksUpgraded != 0 {
		t.Fatalf("a settled repository's packs rewritten: %s", again)
	}
}

// Compaction frames a file's versions on main together, apart from main's head: the head's small
// files share frames by path, and a file's older versions, found along main's history, share frames
// of their own by path.
func TestCompactionFramesHistoryByPath(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	version := func(n int) string {
		return string(goFile(1)) + strings.Repeat(fmt.Sprintf("// revision %d\n", n), n+1)
	}
	others := map[string]string{}
	for i := range 20 {
		others[fmt.Sprintf("lib/f%02d.go", i)] = string(goFile(100 + i))
	}
	others["a/x.go"] = version(0)
	e.landChange("first", others)
	// A commit's tree is the files it names: each revision names them all, changing a/x.go alone.
	for n := 1; n < 5; n++ {
		others["a/x.go"] = version(n)
		e.landChange(fmt.Sprintf("revision %d", n), others)
	}
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.PacksWritten == 0 {
		t.Fatalf("nothing compacted: %s", r)
	}
	entries := locatedEntries(t, e.s, e.repo)
	frameOf := func(contents string) contentindex.Location {
		t.Helper()
		id := pack.ChunkID([]byte(contents))
		en, ok := entries[id]
		if !ok {
			t.Fatalf("no location of a chunk of %q", contents[:20])
		}
		return en.Loc
	}
	head := frameOf(version(4))
	old := frameOf(version(0))
	if packLocation(old).Whole() || packLocation(head).Whole() {
		t.Fatalf("small files alone in frames: head %+v, old %+v", head, old)
	}
	for n := 1; n < 4; n++ {
		l := frameOf(version(n))
		if l.Pack != old.Pack || l.Offset != old.Offset {
			t.Errorf("revision %d is in another frame than revision 0: %+v, %+v", n, l, old)
		}
	}
	if head.Pack == old.Pack && head.Offset == old.Offset {
		t.Error("main's head shares a frame with the file's older versions")
	}
	lib := frameOf(string(goFile(105)))
	if lib.Pack != head.Pack || lib.Offset != head.Offset {
		t.Errorf("main's head files are in more than one frame: %+v, %+v", lib, head)
	}
	for n := range 5 {
		if got := readAll(t, reopen(t, e.s), e.repo, fileID(t, version(n))); string(got) != version(n) {
			t.Fatalf("revision %d reads back wrong", n)
		}
	}
	clean(t, e.s, e.repo)
}
