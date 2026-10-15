//! A workspace's inodes on disk (docs/okfs.md, "Where state lives"): a snapshot
//! (`inodes.snap`) and the records logged since (`inodes.log`), each a 4-byte big-endian length
//! and a `Record`. Loading replays both; compaction writes the model whole as a new snapshot and
//! empties the log. A record is written (to the page cache) before the reply it accompanies, so a
//! crashed daemon's successor finds everything a process was told.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufWriter;
use std::io::Read as _;
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
}

const SNAP: &str = "inodes.snap";
const LOG: &str = "inodes.log";

fn encode(r: &pb::Record, out: &mut Vec<u8>) {
    let body = r.encode_to_vec();
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
}

/// Reads a file of records; a torn last record (a crash mid-write) is ignored.
fn read_file(path: &Path) -> io::Result<Vec<pb::Record>> {
    let mut buf = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut buf)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    }
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
    Ok(out)
}

impl FileLog {
    /// Opens a state directory's log, returning it and every record it holds, the snapshot's
    /// first.
    pub fn open(dir: &Path) -> io::Result<(Self, Vec<pb::Record>)> {
        std::fs::create_dir_all(dir)?;
        let mut records = read_file(&dir.join(SNAP))?;
        let logged = read_file(&dir.join(LOG))?;
        let len = logged.len() as u64;
        records.extend(logged);
        // A torn tail is cut off, so new records follow whole ones.
        let mut whole = Vec::new();
        for r in &records[records.len() - len as usize..] {
            encode(r, &mut whole);
        }
        std::fs::write(dir.join(LOG), &whole)?;
        let f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(dir.join(LOG))?;
        Ok((
            Self {
                dir: dir.to_owned(),
                log: BufWriter::new(f),
                len,
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
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.log.flush()?;
        self.log.get_ref().sync_data()
    }

    fn compact(&mut self, records: &[pb::Record]) -> io::Result<()> {
        let mut buf = Vec::new();
        for r in records {
            encode(r, &mut buf);
        }
        let tmp = self.dir.join(format!("{SNAP}.tmp"));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.dir.join(SNAP))?;
        let f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.dir.join(LOG))?;
        f.sync_all()?;
        self.log = BufWriter::new(OpenOptions::new().append(true).open(self.dir.join(LOG))?);
        self.len = 0;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn logged(&self) -> u64 {
        self.len
    }
}
