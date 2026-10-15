//! Writing a file's contents: chunked as they stream, hashed to the file's ID, and staged in
//! the outbox. A file up to 128 KiB is one chunk; a larger one is cut by FastCDC (2020;
//! 16 KiB minimum, 64 KiB average, 256 KiB maximum), so an edit to a large file re-sends
//! only the chunks around it. A file's ID never depends on how it was chunked.

use std::io;

use fastcdc::v2020::AsyncStreamCDC;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use ok_encoding::Hasher;
use ok_encoding::Kind;

use crate::cache::Manifest;
use crate::cache::chunk_id;
use crate::ids::Id;
use crate::shared::Shared;

/// The largest file stored as a single chunk.
pub const SINGLE_CHUNK: usize = 128 << 10;

/// The largest file the SCM holds (its MaxFileSize, docs/scm.md). A larger one is refused as it is
/// written, so it is never staged: a staged file the SCM refuses would fail every publish after it.
pub const MAX_FILE: u64 = 50 << 20;
const MIN: u32 = 16 << 10;
const AVG: u32 = 64 << 10;
const MAX: u32 = 256 << 10;

/// Writes `contents`, written at `path_hint`, and returns the file's ID. A file of one chunk — most
/// of a repository's — is staged in one append, chunk and manifest; a larger one's chunks as they
/// stream, then its manifest. Neither is synced: the tree, commit, or checkpoint that names the file
/// is staged durably, which makes all staged before it durable too (Outbox::stage_file_with).
pub async fn write_file(
    shared: &Shared,
    path_hint: &str,
    contents: &mut (dyn AsyncRead + Send + Unpin),
) -> io::Result<Id> {
    let mut head = Vec::with_capacity(SINGLE_CHUNK + 1);
    (&mut *contents)
        .take(SINGLE_CHUNK as u64 + 1)
        .read_to_end(&mut head)
        .await?;
    let too_large = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{path_hint} is larger than 50 MiB, the largest file ok holds: remove it, or \
                 leave it untracked (an ignore file, then `jj file untrack`)"
            ),
        )
    };
    let staged = |id: &Id| shared.outbox.has_chunk(id) || shared.cache.holds_chunk(id);
    let mut hasher = Hasher::new(Kind::File);
    if head.len() <= SINGLE_CHUNK {
        hasher.update(&head);
        let id = hasher.finalize();
        if !shared.cache.holds_file(&id) {
            let mut chunks = Vec::new();
            let mut raw: Vec<(Id, &[u8])> = Vec::new();
            if !head.is_empty() {
                let cid = chunk_id(&head);
                chunks.push((cid, head.len() as u32));
                if !staged(&cid) {
                    raw.push((cid, &head));
                }
            }
            shared.outbox.stage_file_with(
                &raw,
                &Manifest {
                    id,
                    size: head.len() as u64,
                    chunks,
                    path_hint: path_hint.to_owned(),
                },
            )?;
        }
        return Ok(id);
    }
    let mut chunks = Vec::new();
    let mut written = 0u64;
    let source = futures::io::Cursor::new(head).chain(contents);
    let mut cdc = AsyncStreamCDC::new(source, MIN, AVG, MAX);
    let mut stream = std::pin::pin!(cdc.as_stream());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(io::Error::other)?;
        let data = &chunk.data;
        written += data.len() as u64;
        if written > MAX_FILE {
            return Err(too_large());
        }
        let cid = chunk_id(data);
        if !staged(&cid) {
            shared.outbox.stage_chunk(&cid, data)?;
        }
        hasher.update(data);
        chunks.push((cid, data.len() as u32));
    }
    let id = hasher.finalize();
    if !shared.cache.holds_file(&id) {
        shared.outbox.stage_file_with(
            &[],
            &Manifest {
                id,
                size: written,
                chunks,
                path_hint: path_hint.to_owned(),
            },
        )?;
    }
    Ok(id)
}
