//! The session against the kernel: an in-memory file system mounted in a namespace of the test's
//! own, driven by ordinary file-system calls.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::io::Read as _;
use std::io::Seek as _;
use std::io::Write as _;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::Attr;
use crate::Ctx;
use crate::DirBuf;
use crate::DirEntry;
use crate::DirEntryPlus;
use crate::Entry;
use crate::Filesystem;
use crate::Notifier;
use crate::Open;
use crate::SetAttr;

const CHILD: &str = "OKFS_FUSE_KERNEL_TEST";

struct Node {
    attr: Attr,
    data: Vec<u8>,
    children: BTreeMap<OsString, u64>,
    target: Vec<u8>,
}

#[derive(Default)]
struct MemFs {
    nodes: Mutex<HashMap<u64, Node>>,
    next: Mutex<u64>,
    notifier: Mutex<Option<Notifier>>,
}

const TTL: Duration = Duration::from_secs(60);

impl MemFs {
    fn new() -> Self {
        let fs = MemFs::default();
        fs.nodes.lock().unwrap().insert(
            1,
            Node {
                attr: Attr {
                    ino: 1,
                    mode: crate::abi::S_IFDIR | 0o755,
                    nlink: 2,
                    uid: unsafe { libc::geteuid() },
                    gid: unsafe { libc::getegid() },
                    ..Attr::default()
                },
                data: Vec::new(),
                children: BTreeMap::new(),
                target: Vec::new(),
            },
        );
        *fs.next.lock().unwrap() = 2;
        fs
    }

    fn entry(attr: Attr) -> Entry {
        Entry {
            attr,
            generation: 0,
            entry_ttl: TTL,
            attr_ttl: TTL,
        }
    }

    fn add(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        target: &[u8],
    ) -> crate::Result<Entry> {
        let mut nodes = self.nodes.lock().unwrap();
        let p = nodes.get(&parent).ok_or(libc::ENOENT)?;
        if p.children.contains_key(name) {
            return Err(libc::EEXIST);
        }
        let mut next = self.next.lock().unwrap();
        let ino = *next;
        *next += 1;
        let attr = Attr {
            ino,
            mode,
            nlink: 1,
            uid: ctx.uid,
            gid: ctx.gid,
            size: target.len() as u64,
            ..Attr::default()
        };
        nodes.insert(
            ino,
            Node {
                attr,
                data: Vec::new(),
                children: BTreeMap::new(),
                target: target.to_vec(),
            },
        );
        nodes
            .get_mut(&parent)
            .unwrap()
            .children
            .insert(name.to_owned(), ino);
        Ok(Self::entry(attr))
    }
}

impl Filesystem for MemFs {
    fn init(&self, notifier: Notifier) {
        *self.notifier.lock().unwrap() = Some(notifier);
    }

    fn lookup(&self, _: &Ctx, parent: u64, name: &OsStr) -> crate::Result<Option<Entry>> {
        let nodes = self.nodes.lock().unwrap();
        let p = nodes.get(&parent).ok_or(libc::ENOENT)?;
        Ok(p.children.get(name).map(|ino| Self::entry(nodes[ino].attr)))
    }

    fn getattr(&self, _: &Ctx, ino: u64, _: Option<u64>) -> crate::Result<(Attr, Duration)> {
        let nodes = self.nodes.lock().unwrap();
        Ok((nodes.get(&ino).ok_or(libc::ENOENT)?.attr, TTL))
    }

    fn setattr(&self, _: &Ctx, ino: u64, set: &SetAttr) -> crate::Result<(Attr, Duration)> {
        let mut nodes = self.nodes.lock().unwrap();
        let n = nodes.get_mut(&ino).ok_or(libc::ENOENT)?;
        if let Some(size) = set.size {
            n.data.resize(size as usize, 0);
            n.attr.size = size;
        }
        if let Some(mode) = set.mode {
            n.attr.mode = (n.attr.mode & crate::abi::S_IFMT) | (mode & 0o7777);
        }
        if let Some(crate::TimeOrNow::Time(t)) = set.mtime {
            n.attr.mtime = t;
        }
        Ok((n.attr, TTL))
    }

    fn readlink(&self, _: &Ctx, ino: u64) -> crate::Result<Vec<u8>> {
        Ok(self
            .nodes
            .lock()
            .unwrap()
            .get(&ino)
            .ok_or(libc::ENOENT)?
            .target
            .clone())
    }

    fn symlink(&self, ctx: &Ctx, parent: u64, name: &OsStr, target: &[u8]) -> crate::Result<Entry> {
        self.add(ctx, parent, name, crate::abi::S_IFLNK | 0o777, target)
    }

    fn mkdir(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> crate::Result<Entry> {
        self.add(
            ctx,
            parent,
            name,
            crate::abi::S_IFDIR | (mode & !umask & 0o7777),
            b"",
        )
    }

    fn create(
        &self,
        ctx: &Ctx,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _: i32,
    ) -> crate::Result<(Entry, Open)> {
        let e = self.add(
            ctx,
            parent,
            name,
            crate::abi::S_IFREG | (mode & !umask & 0o7777),
            b"",
        )?;
        Ok((e, Open { fh: 7, flags: 0 }))
    }

    fn open(&self, _: &Ctx, ino: u64, flags: i32) -> crate::Result<Open> {
        let mut nodes = self.nodes.lock().unwrap();
        let n = nodes.get_mut(&ino).ok_or(libc::ENOENT)?;
        if flags & libc::O_TRUNC != 0 {
            n.data.clear();
            n.attr.size = 0;
        }
        Ok(Open {
            fh: 7,
            flags: crate::abi::FOPEN_KEEP_CACHE,
        })
    }

    fn read(&self, _: &Ctx, ino: u64, _: u64, offset: u64, size: u32) -> crate::Result<Vec<u8>> {
        let nodes = self.nodes.lock().unwrap();
        let data = &nodes.get(&ino).ok_or(libc::ENOENT)?.data;
        let start = (offset as usize).min(data.len());
        let end = (start + size as usize).min(data.len());
        Ok(data[start..end].to_vec())
    }

    fn write(
        &self,
        _: &Ctx,
        ino: u64,
        _: u64,
        offset: u64,
        data: &[u8],
        _: u32,
    ) -> crate::Result<u32> {
        let mut nodes = self.nodes.lock().unwrap();
        let n = nodes.get_mut(&ino).ok_or(libc::ENOENT)?;
        let end = offset as usize + data.len();
        if n.data.len() < end {
            n.data.resize(end, 0);
        }
        n.data[offset as usize..end].copy_from_slice(data);
        n.attr.size = n.data.len() as u64;
        Ok(data.len() as u32)
    }

    fn unlink(&self, _: &Ctx, parent: u64, name: &OsStr) -> crate::Result<()> {
        let mut nodes = self.nodes.lock().unwrap();
        let ino = nodes
            .get_mut(&parent)
            .ok_or(libc::ENOENT)?
            .children
            .remove(name)
            .ok_or(libc::ENOENT)?;
        nodes.remove(&ino);
        Ok(())
    }

    fn rmdir(&self, ctx: &Ctx, parent: u64, name: &OsStr) -> crate::Result<()> {
        {
            let nodes = self.nodes.lock().unwrap();
            let ino = *nodes
                .get(&parent)
                .ok_or(libc::ENOENT)?
                .children
                .get(name)
                .ok_or(libc::ENOENT)?;
            if !nodes[&ino].children.is_empty() {
                return Err(libc::ENOTEMPTY);
            }
        }
        self.unlink(ctx, parent, name)
    }

    fn rename(
        &self,
        _: &Ctx,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _: u32,
    ) -> crate::Result<()> {
        let mut nodes = self.nodes.lock().unwrap();
        let ino = nodes
            .get_mut(&parent)
            .ok_or(libc::ENOENT)?
            .children
            .remove(name)
            .ok_or(libc::ENOENT)?;
        if let Some(old) = nodes
            .get_mut(&newparent)
            .ok_or(libc::ENOENT)?
            .children
            .insert(newname.to_owned(), ino)
        {
            nodes.remove(&old);
        }
        Ok(())
    }

    fn readdir(
        &self,
        _: &Ctx,
        ino: u64,
        _: u64,
        offset: u64,
        buf: &mut DirBuf<'_>,
    ) -> crate::Result<()> {
        let nodes = self.nodes.lock().unwrap();
        let dir = nodes.get(&ino).ok_or(libc::ENOENT)?;
        let mut all: Vec<(OsString, u64, u32)> = vec![
            (".".into(), ino, crate::abi::S_IFDIR),
            ("..".into(), 1, crate::abi::S_IFDIR),
        ];
        for (name, child) in &dir.children {
            all.push((name.clone(), *child, nodes[child].attr.mode));
        }
        for (i, (name, child, mode)) in all.into_iter().enumerate().skip(offset as usize) {
            let entry = DirEntry {
                ino: child,
                offset: i as u64 + 1,
                kind: mode,
                name: name.clone(),
            };
            let ok = if buf.is_plus() {
                let lookup = (name != "." && name != "..").then(|| Self::entry(nodes[&child].attr));
                buf.push_plus(&DirEntryPlus { entry, lookup })
            } else {
                buf.push(&entry)
            };
            if !ok {
                break;
            }
        }
        Ok(())
    }
}

/// Runs the scenario in a child in its own namespaces (the test binary, re-executed).
#[test]
fn kernel() {
    if !std::path::Path::new("/dev/fuse").exists() {
        panic!("no /dev/fuse: the test target declares devices: [fuse] (docs/building.md, Tests)");
    }
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "tests::kernel_child",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(CHILD, "1");
    crate::testns::unshare(&mut cmd);
    let out = cmd.output().expect("run the child");
    assert!(
        out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("okfs-kernel-scenario-done"),
        "the child: {}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn kernel_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    fs::create_dir(&mnt).unwrap();
    let fd = crate::mount(&mnt, &crate::MountOptions::default()).expect("mount");
    let fs_ = Arc::new(MemFs::new());
    let session = crate::Session::new(fd);
    let negotiated = Arc::new(Mutex::new(None));
    let n2 = negotiated.clone();
    let server = {
        let fs_ = fs_.clone();
        std::thread::spawn(move || session.run(fs_, 4, move |n| *n2.lock().unwrap() = Some(n)))
    };

    // Files: create, write, read back across opens, append, truncate.
    let f = mnt.join("hello.txt");
    fs::write(&f, b"hello, okfs\n").unwrap();
    assert_eq!(fs::read(&f).unwrap(), b"hello, okfs\n");
    let mut h = fs::OpenOptions::new().append(true).open(&f).unwrap();
    h.write_all(b"more\n").unwrap();
    drop(h);
    assert_eq!(fs::read_to_string(&f).unwrap(), "hello, okfs\nmore\n");
    let mut h = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&f)
        .unwrap();
    h.seek(std::io::SeekFrom::Start(7)).unwrap();
    let mut s = String::new();
    h.read_to_string(&mut s).unwrap();
    assert_eq!(s, "okfs\nmore\n");
    h.set_len(5).unwrap();
    drop(h);
    assert_eq!(fs::read(&f).unwrap(), b"hello");
    // A write larger than one request.
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    fs::write(mnt.join("big"), &big).unwrap();
    assert_eq!(fs::read(mnt.join("big")).unwrap(), big);
    assert_eq!(
        fs::metadata(mnt.join("big")).unwrap().len(),
        big.len() as u64
    );

    // Directories: mkdir, listing (READDIRPLUS and its lookups), rename, rmdir.
    fs::create_dir(mnt.join("d")).unwrap();
    for i in 0..300 {
        fs::write(mnt.join("d").join(format!("f{i:03}")), format!("{i}")).unwrap();
    }
    let mut names: Vec<_> = fs::read_dir(mnt.join("d"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names.len(), 300);
    assert_eq!(names[0], "f000");
    assert_eq!(fs::read_to_string(mnt.join("d/f123")).unwrap(), "123");
    fs::rename(mnt.join("d/f123"), mnt.join("moved")).unwrap();
    assert!(!mnt.join("d/f123").exists());
    assert_eq!(fs::read_to_string(mnt.join("moved")).unwrap(), "123");
    assert_eq!(
        fs::remove_dir(mnt.join("d")).unwrap_err().raw_os_error(),
        Some(libc::ENOTEMPTY)
    );
    for i in 0..300 {
        if i != 123 {
            fs::remove_file(mnt.join("d").join(format!("f{i:03}"))).unwrap();
        }
    }
    fs::remove_dir(mnt.join("d")).unwrap();
    assert!(!mnt.join("d").exists());

    // Symlinks and modes.
    std::os::unix::fs::symlink("hello.txt", mnt.join("link")).unwrap();
    assert_eq!(
        fs::read_link(mnt.join("link")).unwrap(),
        std::path::Path::new("hello.txt")
    );
    assert_eq!(fs::read(mnt.join("link")).unwrap(), b"hello");
    fs::set_permissions(&f, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(fs::metadata(&f).unwrap().mode() & 0o777, 0o755);

    // Invalidation: the file system changes a file behind the kernel's cached pages, and says so.
    let ino = fs::metadata(&f).unwrap().ino();
    {
        let mut nodes = fs_.nodes.lock().unwrap();
        let n = nodes.get_mut(&ino).unwrap();
        n.data = b"HELLO".to_vec();
    }
    let notifier = fs_.notifier.lock().unwrap().clone().unwrap();
    notifier.inval_inode(ino, 0, 0).unwrap();
    assert_eq!(fs::read(&f).unwrap(), b"HELLO");
    // And an entry: a name the kernel cached as absent appears once invalidated.
    assert!(!mnt.join("later").exists());
    fs_.add(
        &Ctx {
            uid: 0,
            gid: 0,
            pid: 0,
        },
        1,
        OsStr::new("later"),
        crate::abi::S_IFREG | 0o644,
        b"",
    )
    .unwrap();
    notifier.inval_entry(1, OsStr::new("later")).unwrap();
    assert!(mnt.join("later").exists());

    assert!(negotiated.lock().unwrap().unwrap().minor >= crate::abi::MIN_KERNEL_MINOR);
    crate::unmount(&mnt).unwrap();
    server.join().unwrap().unwrap();
    println!("okfs-kernel-scenario-done");
}
