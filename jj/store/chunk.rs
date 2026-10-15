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
const MIN: u32 = 16 << 10;
const AVG: u32 = 64 << 10;
const MAX: u32 = 256 << 10;

/// Writes `contents`, written at `path_hint`, and returns the file's ID.
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
    let mut hasher = Hasher::new(Kind::File);
    let mut chunks = Vec::new();
    let mut stage = |data: &[u8]| -> io::Result<()> {
        let id = chunk_id(data);
        if !shared.outbox.has_chunk(&id) && !shared.cache.has_chunk(&id) {
            shared.outbox.stage_chunk(&id, data)?;
        }
        hasher.update(data);
        chunks.push((id, data.len() as u32));
        Ok(())
    };
    if head.len() <= SINGLE_CHUNK {
        if !head.is_empty() {
            stage(&head)?;
        }
    } else {
        let source = futures::io::Cursor::new(head).chain(contents);
        let mut cdc = AsyncStreamCDC::new(source, MIN, AVG, MAX);
        let mut stream = std::pin::pin!(cdc.as_stream());
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(io::Error::other)?;
            stage(&chunk.data)?;
        }
    }
    let id = hasher.finalize();
    let size = chunks.iter().map(|c: &(Id, u32)| u64::from(c.1)).sum();
    if !shared.cache.has_file(&id) {
        // The chunks must be durable before the manifest that names them.
        shared.outbox.sync_chunks()?;
        shared.outbox.stage_file(&Manifest {
            id,
            size,
            chunks,
            path_hint: path_hint.to_owned(),
        })?;
    }
    Ok(id)
}
