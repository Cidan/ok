// Package scmtest builds and publishes repository content for tests, and for ok's load generator,
// that talk to an SCM over its protocol, as ok's jj stages and sends it: objects in their
// canonical encodings, files split into chunks, and one Publish exchange that sends exactly what
// the SCM says it lacks.
package scmtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Objects builds canonical objects the way jj's client stages them.
type Objects struct {
	t      testing.TB
	seq    int
	Order  []*scmpb.EncodedObject
	Files  map[encoding.ID][]byte
	Chunks map[encoding.ID][]byte
	// Workspace is whose operation log a publish's operations and views go to.
	Workspace string
	// GuardsSecrets promises, as ok's jj does, to store no secret's value (docs/secrets.md).
	GuardsSecrets bool
}

func NewObjects(t testing.TB) *Objects {
	return &Objects{t: t, Files: map[encoding.ID][]byte{}, Chunks: map[encoding.ID][]byte{}}
}

// Builder is Objects outside a test: an object that does not encode panics.
func Builder() *Objects {
	return &Objects{Files: map[encoding.ID][]byte{}, Chunks: map[encoding.ID][]byte{}}
}

// Add encodes an object and stages it, returning its ID.
func (b *Objects) Add(o encoding.Object) encoding.ID {
	canonical, err := encoding.Encode(o)
	if err != nil {
		if b.t == nil {
			panic(err)
		}
		b.t.Helper()
		b.t.Fatal(err)
	}
	id := encoding.Hash(canonical)
	b.Order = append(b.Order, &scmpb.EncodedObject{Kind: scmpb.ObjectKind(o.Kind()), Id: id[:], Body: canonical[1:]})
	return id
}

// File stages a file's contents, returning its ID.
func (b *Objects) File(contents []byte) encoding.ID {
	h := encoding.NewHasher(encoding.KindFile)
	h.Write(contents)
	id := h.Sum()
	b.Files[id] = contents
	return id
}

// Commit stages a commit of tree on parents, each commit a change of its own.
func (b *Objects) Commit(tree encoding.ID, desc string, parents ...encoding.ID) encoding.ID {
	b.seq++
	sig := encoding.Signature{Name: "t", Email: "t@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(b.seq)}}
	var change encoding.ChangeID
	copy(change[:], fmt.Sprintf("svc-change-%05d", b.seq))
	return b.Add(&encoding.Commit{
		Parents: parents, RootTree: encoding.Merge[encoding.ID]{tree}, ConflictLabels: encoding.Merge[string]{""},
		ChangeID: change, Description: desc, Author: sig, Committer: sig,
	})
}

// Operation stages an operation on parents, whose view is view.
func (b *Objects) Operation(view encoding.ID, parents ...encoding.ID) encoding.ID {
	b.seq++
	return b.Add(&encoding.Operation{
		ViewID: view, Parents: parents,
		Metadata: encoding.OperationMetadata{
			Start: encoding.Timestamp{Millis: int64(b.seq)}, End: encoding.Timestamp{Millis: int64(b.seq)},
			Description: "op", Hostname: "test", Username: "t",
		},
		CommitPredecessors: map[encoding.ID][]encoding.ID{},
	})
}

// ChunkSize is how much of a file each chunk Manifests stages holds, but the last.
const ChunkSize = 64 << 10

// Manifests are the staged files' manifests, their chunks staged.
func (b *Objects) Manifests() []*scmpb.FileManifest {
	var out []*scmpb.FileManifest
	for id, contents := range b.Files {
		m := &scmpb.FileManifest{FileId: id[:], Size: uint64(len(contents)), PathHint: "f"}
		for off := 0; off < len(contents); off += ChunkSize {
			c := contents[off:min(off+ChunkSize, len(contents))]
			cid := pack.ChunkID(c)
			b.Chunks[cid] = c
			m.Chunks = append(m.Chunks, &scmpb.ChunkRef{Id: cid[:], Length: uint32(len(c))})
		}
		out = append(out, m)
	}
	return out
}

// Begin is a Publish's first request, naming everything staged.
func (b *Objects) Begin(repo int64) *scmpb.PublishRequest {
	refs := make([]*scmpb.ObjectRef, len(b.Order))
	for i, o := range b.Order {
		refs[i] = &scmpb.ObjectRef{Kind: o.Kind, Id: o.Id}
	}
	return &scmpb.PublishRequest{Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo, Objects: refs, Files: b.Manifests(), Workspace: b.Workspace,
		GuardsSecrets: b.GuardsSecrets}}}
}

// Publish runs a whole Publish exchange, sending exactly what the server reports missing, and
// moving the workspace's operation heads as heads says (nil: not at all).
func (b *Objects) Publish(ctx context.Context, store scmpb.StoreClient, repo int64, heads *scmpb.OpHeadsUpdate) ([][]byte, *scmpb.PublishResponse_Missing, error) {
	stream, err := store.Publish(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.Send(b.Begin(repo)); err != nil {
		return nil, nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	missing := resp.GetMissing()
	byID := map[string]*scmpb.EncodedObject{}
	for _, o := range b.Order {
		byID[string(o.Id)] = o
	}
	for _, r := range missing.GetObjects() {
		if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Object{Object: byID[string(r.GetId())]}}); err != nil {
			return nil, missing, err
		}
	}
	for _, cid := range missing.GetChunks() {
		if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Chunk{Chunk: &scmpb.ChunkData{Id: cid, Data: b.Chunks[encoding.ID(cid)]}}}); err != nil {
			return nil, missing, err
		}
	}
	if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Commit_{Commit: &scmpb.PublishRequest_Commit{OpHeads: heads}}}); err != nil {
		return nil, missing, err
	}
	done, err := stream.Recv()
	if err != nil {
		return nil, missing, err
	}
	return done.GetDone().GetOpHeads(), missing, nil
}
