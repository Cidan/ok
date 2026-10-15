package encoding

import (
	"testing"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// ObjectKind travels in place of the kind byte, so each value must be that byte.
func TestProtoObjectKindsAreKindBytes(t *testing.T) {
	for proto, kind := range map[scmpb.ObjectKind]Kind{
		scmpb.ObjectKind_OBJECT_KIND_FILE:         KindFile,
		scmpb.ObjectKind_OBJECT_KIND_SYMLINK:      KindSymlink,
		scmpb.ObjectKind_OBJECT_KIND_TREE:         KindTree,
		scmpb.ObjectKind_OBJECT_KIND_COPY_HISTORY: KindCopyHistory,
		scmpb.ObjectKind_OBJECT_KIND_COMMIT:       KindCommit,
		scmpb.ObjectKind_OBJECT_KIND_OPERATION:    KindOperation,
		scmpb.ObjectKind_OBJECT_KIND_VIEW:         KindView,
		scmpb.ObjectKind_OBJECT_KIND_TREE_SIZES:   KindTreeSizes,
	} {
		if int32(proto) != int32(kind) {
			t.Errorf("%s = %d, kind byte %#02x", proto, proto, uint8(kind))
		}
	}
	if n := len(scmpb.ObjectKind_name); n != 9 {
		t.Errorf("ObjectKind has %d values; every one must map to a kind byte here", n)
	}
}
