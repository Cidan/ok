//! A workspace's inodes on disk (docs/okfs.md, "Where state lives"): a snapshot
//! (`inodes.snap`) and the records logged since (`inodes.log`), each a 4-byte big-endian length
//! and a `Record`. Loading replays both; compaction writes the model whole as a new snapshot and
//! empties the log. A record is written (to the page cache) before the reply it accompanies, so a
//! crashed daemon's successor finds everything a process was told.
//!
//! A snapshot's first record marks the log it was compacted from (`CompactionMark`: its length
//! and hash). Compaction makes the snapshot durable — its data, then the directory naming it —
//! before it truncates the log, so a power loss leaves either the old snapshot and its whole log,
//! or the new snapshot and a log that either was truncated or still starts with exactly the
//! marked bytes, which the snapshot holds and loading skips: no record is lost, and none is
//! replayed over a snapshot that already has it.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufWriter;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use ok_proto::ok::okfs::v1 as pb;
use prost::Message as _;

/// Where a model's records go.
pub trait Log: Send {
    fn record(&mut self, r: &pb::Record) -> io::Result<()>;
    /// Makes what was recorded durable.
    fn sync(&mut self) -> io::Result<()>;
    /// Replaces everything recorded with `records`.
    fn compact(&mut self, records: &[pb::Record]) -> io::Result<()>;
    /// Records since the last compaction.
    fn logged(&self) -> u64;
}

/// A log that keeps its records in memory: for tests, which read them back.
#[derive(Clone, Default)]
pub struct MemLog(pub Arc<Mutex<Vec<pb::Record>>>);

impl Log for MemLog {
    fn record(&mut self, r: &pb::Record) -> io::Result<()> {
        self.0.lock().unwrap().push(r.clone());
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn compact(&mut self, records: &[pb::Record]) -> io::Result<()> {
        *self.0.lock().unwrap() = records.to_vec();
        Ok(())
    }
    fn logged(&self) -> u64 {
        self.0.lock().unwrap().len() as u64
    }
}

/// The log in a workspace's state directory.
pub struct FileLog {
    dir: PathBuf,
    log: BufWriter<File>,
    len: u64,
    /// The log's bytes since it was last emptied, and their hash: the next snapshot's mark.
    bytes: u64,
    hash: blake3::Hasher,
}

const SNAP: &str = "inodes.snap";
const LOG: &str = "inodes.log";

fn encode(r: &pb::Record, out: &mut Vec<u8>) {
    let body = r.encode_to_vec();
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
}

/// A file's bytes; none when there is no file.
fn read_bytes(path: &Path) -> io::Result<Vec<u8>> {
    match std::fs::read(path) {
        Ok(b) => Ok(b),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Decodes records, and how many bytes they took: a torn last record (a crash mid-write) is
/// ignored.
fn decode(buf: &[u8]) -> (Vec<pb::Record>, usize) {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 4 <= buf.len() {
        let n = u32::from_be_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
        if at + 4 + n > buf.len() {
            break;
        }
        match pb::Record::decode(&buf[at + 4..at + 4 + n]) {
            Ok(r) => out.push(r),
            Err(_) => break,
        }
        at += 4 + n;
    }
    (out, at)
}

/// Syncs a directory, making the names it holds durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Replaces a file's contents with `data`, durably and whole: a temporary file synced, renamed
/// over it, and the directory synced.
fn replace(dir: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

/// The mark of a snapshot, if its first record is one.
fn mark_of(snapshot: &[pb::Record]) -> Option<&pb::CompactionMark> {
    match snapshot.first().and_then(|r| r.record.as_ref()) {
        Some(pb::record::Record::Compaction(m)) => Some(m),
        _ => None,
    }
}

impl FileLog {
    /// Opens a state directory's log, returning it and every record it holds, the snapshot's
    /// first.
    pub fn open(dir: &Path) -> io::Result<(Self, Vec<pb::Record>)> {
        std::fs::create_dir_all(dir)?;
        let (mut records, _) = decode(&read_bytes(&dir.join(SNAP))?);
        let log = read_bytes(&dir.join(LOG))?;
        // The log the snapshot was compacted from, if its truncation was lost: skipped.
        let mut start = 0;
        if let Some(m) = mark_of(&records) {
            let n = m.log_bytes as usize;
            if n <= log.len() && blake3::hash(&log[..n]).as_bytes()[..] == m.log_blake3[..] {
                start = n;
            }
            records.remove(0);
        }
        let (logged, used) = decode(&log[start..]);
        let len = logged.len() as u64;
        // Rewritten only to drop what was skipped or torn, so new records follow whole ones.
        if start > 0 || start + used < log.len() {
            replace(dir, LOG, &log[start..start + used])?;
        }
        let kept = &log[start..start + used];
        let mut hash = blake3::Hasher::new();
        hash.update(kept);
        records.extend(logged);
        let f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(dir.join(LOG))?;
        Ok((
            Self {
                dir: dir.to_owned(),
                log: BufWriter::new(f),
                len,
                bytes: kept.len() as u64,
                hash,
            },
            records,
        ))
    }
}

impl Log for FileLog {
    fn record(&mut self, r: &pb::Record) -> io::Result<()> {
        let mut buf = Vec::new();
        encode(r, &mut buf);
        self.log.write_all(&buf)?;
        self.log.flush()?;
        self.len += 1;
        self.bytes += buf.len() as u64;
        self.hash.update(&buf);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.log.flush()?;
        self.log.get_ref().sync_data()
    }

    fn compact(&mut self, records: &[pb::Record]) -> io::Result<()> {
        self.log.flush()?;
        let mark = pb::Record {
            record: Some(pb::record::Record::Compaction(pb::CompactionMark {
                log_bytes: self.bytes,
                log_blake3: self.hash.finalize().as_bytes().to_vec(),
            })),
        };
        let mut buf = Vec::new();
        encode(&mark, &mut buf);
        for r in records {
            encode(r, &mut buf);
        }
        // The snapshot durable — its data and its name — before the log it replaces is emptied.
        replace(&self.dir, SNAP, &buf)?;
        let f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.dir.join(LOG))?;
        f.sync_all()?;
        self.log = BufWriter::new(OpenOptions::new().append(true).open(self.dir.join(LOG))?);
        self.len = 0;
        self.bytes = 0;
        self.hash = blake3::Hasher::new();
        Ok(())
    }

    fn logged(&self) -> u64 {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drop(n: u64) -> pb::Record {
        pb::Record {
            record: Some(pb::record::Record::Drop(n)),
        }
    }

    fn drops(records: &[pb::Record]) -> Vec<u64> {
        records
            .iter()
            .map(|r| match r.record {
                Some(pb::record::Record::Drop(n)) => n,
                _ => panic!("not a drop: {r:?}"),
            })
            .collect()
    }

    fn open(dir: &Path) -> (FileLog, Vec<u64>) {
        let (log, records) = FileLog::open(dir).unwrap();
        let ds = drops(&records);
        (log, ds)
    }

    fn log_bytes(dir: &Path) -> Vec<u8> {
        std::fs::read(dir.join(LOG)).unwrap()
    }

    // Records survive a reopen, and a compaction: the snapshot, then what was logged after it.
    #[test]
    fn reopens_whole() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, got) = open(dir.path());
        assert!(got.is_empty());
        log.record(&drop(1)).unwrap();
        log.record(&drop(2)).unwrap();
        log.compact(&[drop(1), drop(2)]).unwrap();
        log.record(&drop(3)).unwrap();
        drop_log(log);
        let (mut log, got) = open(dir.path());
        assert_eq!(got, vec![1, 2, 3]);
        assert_eq!(log.logged(), 1);
        log.record(&drop(4)).unwrap();
        drop_log(log);
        assert_eq!(open(dir.path()).1, vec![1, 2, 3, 4]);
    }

    // A compaction whose snapshot landed but whose truncation was lost: the old log, which the
    // snapshot holds, is not replayed over it — and what was logged after it still is.
    #[test]
    fn a_lost_truncation_is_not_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.record(&drop(1)).unwrap();
        log.record(&drop(2)).unwrap();
        log.sync().unwrap();
        let stale = log_bytes(dir.path());
        log.compact(&[drop(9)]).unwrap();
        drop_log(log);
        std::fs::write(dir.path().join(LOG), &stale).unwrap();
        assert_eq!(open(dir.path()).1, vec![9]);
        // The skipped bytes are gone from the log, so they are not skipped twice.
        assert!(log_bytes(dir.path()).is_empty());

        // The same, with a record that followed before the crash.
        let (mut log, _) = open(dir.path());
        log.record(&drop(5)).unwrap();
        log.sync().unwrap();
        let stale = log_bytes(dir.path());
        log.compact(&[drop(9), drop(5)]).unwrap();
        drop_log(log);
        let mut after = stale.clone();
        encode(&drop(6), &mut after);
        std::fs::write(dir.path().join(LOG), &after).unwrap();
        assert_eq!(open(dir.path()).1, vec![9, 5, 6]);
    }

    // A snapshot without a mark — an older binary's — replays the whole log, as it always did.
    #[test]
    fn an_unmarked_snapshot_replays_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut snap = Vec::new();
        encode(&drop(1), &mut snap);
        std::fs::write(dir.path().join(SNAP), &snap).unwrap();
        let mut log = Vec::new();
        encode(&drop(2), &mut log);
        std::fs::write(dir.path().join(LOG), &log).unwrap();
        assert_eq!(open(dir.path()).1, vec![1, 2]);
    }

    // A log an older binary wrote after a marked snapshot, which does not start with the marked
    // bytes, is replayed whole.
    #[test]
    fn another_log_after_a_mark_is_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.record(&drop(1)).unwrap();
        log.compact(&[drop(1)]).unwrap();
        drop_log(log);
        let mut other = Vec::new();
        encode(&drop(7), &mut other);
        std::fs::write(dir.path().join(LOG), &other).unwrap();
        assert_eq!(open(dir.path()).1, vec![1, 7]);
    }

    // A torn last record is cut off, and records written after follow whole ones.
    #[test]
    fn a_torn_tail_is_cut() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.record(&drop(1)).unwrap();
        drop_log(log);
        let mut bytes = log_bytes(dir.path());
        bytes.extend_from_slice(&[0, 0, 0, 9, 1, 2]);
        std::fs::write(dir.path().join(LOG), &bytes).unwrap();
        let (mut log, got) = open(dir.path());
        assert_eq!(got, vec![1]);
        log.record(&drop(2)).unwrap();
        drop_log(log);
        assert_eq!(open(dir.path()).1, vec![1, 2]);
    }

    fn drop_log(log: FileLog) {
        std::mem::drop(log);
    }
}
