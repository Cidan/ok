//! The server over a real TCP connection, called as the kernel's client calls it, serving a small
//! file system in memory.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;

use crate::Attr;
use crate::Ctx;
use crate::Entry;
use crate::Filesystem;
use crate::FsStat;
use crate::Kind;
use crate::Listing;
use crate::Options;
use crate::Result;
use crate::Server;
use crate::SetAttr;
use crate::SetTime;
use crate::Time;
use crate::client::Client;
use crate::nfs3;
use crate::rpc;
use crate::rpc::Cred;
use crate::rpc::Reply;

const OWNER: u32 = 1000;
const ROOT: u64 = 1;

struct Node {
    kind: Kind,
    perm: u32,
    data: Vec<u8>,
    parent: u64,
    children: BTreeMap<Vec<u8>, u64>,
    nlink: u32,
    mtime: Time,
    ctime: Time,
}

/// A file system in memory, for the server to serve.
struct MemFs {
    nodes: Mutex<(HashMap<u64, Node>, u64, i64)>,
}

impl MemFs {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            ROOT,
            Node {
                kind: Kind::Dir,
                perm: 0o755,
                data: Vec::new(),
                parent: ROOT,
                children: BTreeMap::new(),
                nlink: 2,
                mtime: Time::default(),
                ctime: Time::default(),
            },
        );
        Self {
            nodes: Mutex::new((nodes, ROOT + 1, 1)),
        }
    }
}

fn tick(clock: &mut i64) -> Time {
    *clock += 1;
    Time {
        secs: *clock,
        nanos: 0,
    }
}

impl Filesystem for MemFs {
    fn getattr(&self, _: &Ctx, ino: u64) -> Result<Attr> {
        let g = self.nodes.lock().unwrap();
        let n = g.0.get(&ino).ok_or(libc::ESTALE)?;
        Ok(Attr {
            kind: n.kind,
            perm: n.perm,
            nlink: n.nlink,
            uid: OWNER,
            gid: OWNER,
            size: n.data.len() as u64,
            used: n.data.len() as u64,
            rdev: (0, 0),
            fileid: ino,
            atime: n.mtime,
            mtime: n.mtime,
            ctime: n.ctime,
        })
    }

    fn setattr(&self, _: &Ctx, ino: u64, set: &SetAttr) -> Result<()> {
        let mut g = self.nodes.lock().unwrap();
        let t = tick(&mut g.2);
        let n = g.0.get_mut(&ino).ok_or(libc::ESTALE)?;
        if let Some(m) = set.mode {
            n.perm = m & 0o7777;
        }
        if let Some(sz) = set.size {
            n.data.resize(sz as usize, 0);
            n.mtime = t;
        }
        match set.mtime {
            Some(SetTime::At(at)) => n.mtime = at,
            Some(SetTime::Now) => n.mtime = t,
            None => {}
        }
        n.ctime = t;
        Ok(())
    }

    fn lookup(&self, _: &Ctx, dir: u64, name: &[u8]) -> Result<u64> {
        let g = self.nodes.lock().unwrap();
        let d = g.0.get(&dir).ok_or(libc::ESTALE)?;
        if d.kind != Kind::Dir {
            return Err(libc::ENOTDIR);
        }
        match name {
            b"." => Ok(dir),
            b".." => Ok(d.parent),
            _ => d.children.get(name).copied().ok_or(libc::ENOENT),
        }
    }

    fn readlink(&self, _: &Ctx, ino: u64) -> Result<Vec<u8>> {
        let g = self.nodes.lock().unwrap();
        let n = g.0.get(&ino).ok_or(libc::ESTALE)?;
        if n.kind != Kind::Symlink {
            return Err(libc::EINVAL);
        }
        Ok(n.data.clone())
    }

    fn read(&self, _: &Ctx, ino: u64, offset: u64, count: u32) -> Result<Vec<u8>> {
        let g = self.nodes.lock().unwrap();
        let n = g.0.get(&ino).ok_or(libc::ESTALE)?;
        if n.kind == Kind::Dir {
            return Err(libc::EISDIR);
        }
        let start = (offset as usize).min(n.data.len());
        let end = (start + count as usize).min(n.data.len());
        Ok(n.data[start..end].to_vec())
    }

    fn write(&self, _: &Ctx, ino: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let mut g = self.nodes.lock().unwrap();
        let t = tick(&mut g.2);
        let n = g.0.get_mut(&ino).ok_or(libc::ESTALE)?;
        let end = offset as usize + data.len();
        if n.data.len() < end {
            n.data.resize(end, 0);
        }
        n.data[offset as usize..end].copy_from_slice(data);
        n.mtime = t;
        n.ctime = t;
        Ok(data.len() as u32)
    }

    fn create(
        &self,
        _: &Ctx,
        dir: u64,
        name: &[u8],
        kind: Kind,
        mode: u32,
        target: &[u8],
    ) -> Result<u64> {
        let mut g = self.nodes.lock().unwrap();
        let t = tick(&mut g.2);
        let ino = g.1;
        let d = g.0.get_mut(&dir).ok_or(libc::ESTALE)?;
        if d.children.contains_key(name) {
            return Err(libc::EEXIST);
        }
        d.children.insert(name.to_vec(), ino);
        d.mtime = t;
        d.ctime = t;
        g.1 += 1;
        g.0.insert(
            ino,
            Node {
                kind,
                perm: mode,
                data: target.to_vec(),
                parent: dir,
                children: BTreeMap::new(),
                nlink: if kind == Kind::Dir { 2 } else { 1 },
                mtime: t,
                ctime: t,
            },
        );
        Ok(ino)
    }

    fn remove(&self, _: &Ctx, dir: u64, name: &[u8]) -> Result<()> {
        let mut g = self.nodes.lock().unwrap();
        let t = tick(&mut g.2);
        let ino =
            *g.0.get(&dir)
                .ok_or(libc::ESTALE)?
                .children
                .get(name)
                .ok_or(libc::ENOENT)?;
        if g.0[&ino].kind == Kind::Dir {
            return Err(libc::EISDIR);
        }
        let d = g.0.get_mut(&dir).unwrap();
        d.children.remove(name);
        d.mtime = t;
        let n = g.0.get_mut(&ino).unwrap();
        n.nlink -= 1;
        if n.nlink == 0 {
            g.0.remove(&ino);
        }
        Ok(())
    }

    fn rmdir(&self, _: &Ctx, dir: u64, name: &[u8]) -> Result<()> {
        let mut g = self.nodes.lock().unwrap();
        let ino =
            *g.0.get(&dir)
                .ok_or(libc::ESTALE)?
                .children
                .get(name)
                .ok_or(libc::ENOENT)?;
        let n = &g.0[&ino];
        if n.kind != Kind::Dir {
            return Err(libc::ENOTDIR);
        }
        if !n.children.is_empty() {
            return Err(libc::ENOTEMPTY);
        }
        g.0.get_mut(&dir).unwrap().children.remove(name);
        g.0.remove(&ino);
        Ok(())
    }

    fn rename(&self, _: &Ctx, from: u64, name: &[u8], to: u64, to_name: &[u8]) -> Result<()> {
        let mut g = self.nodes.lock().unwrap();
        let ino =
            g.0.get_mut(&from)
                .ok_or(libc::ESTALE)?
                .children
                .remove(name)
                .ok_or(libc::ENOENT)?;
        let old =
            g.0.get_mut(&to)
                .ok_or(libc::ESTALE)?
                .children
                .insert(to_name.to_vec(), ino);
        if let Some(old) = old {
            g.0.remove(&old);
        }
        g.0.get_mut(&ino).unwrap().parent = to;
        Ok(())
    }

    fn link(&self, _: &Ctx, ino: u64, dir: u64, name: &[u8]) -> Result<()> {
        let mut g = self.nodes.lock().unwrap();
        let d = g.0.get_mut(&dir).ok_or(libc::ESTALE)?;
        if d.children.contains_key(name) {
            return Err(libc::EEXIST);
        }
        d.children.insert(name.to_vec(), ino);
        g.0.get_mut(&ino).ok_or(libc::ESTALE)?.nlink += 1;
        Ok(())
    }

    fn readdir(&self, ctx: &Ctx, dir: u64, cookie: u64, plus: bool, max: usize) -> Result<Listing> {
        let names: Vec<(Vec<u8>, u64)> = {
            let g = self.nodes.lock().unwrap();
            let d = g.0.get(&dir).ok_or(libc::ESTALE)?;
            let mut v = vec![(b".".to_vec(), dir), (b"..".to_vec(), d.parent)];
            v.extend(d.children.iter().map(|(n, i)| (n.clone(), *i)));
            v
        };
        let mut entries = Vec::new();
        for (i, (name, ino)) in names.iter().enumerate().skip(cookie as usize) {
            if entries.len() == max {
                return Ok(Listing {
                    entries,
                    eof: false,
                });
            }
            entries.push(Entry {
                name: name.clone(),
                fileid: *ino,
                cookie: i as u64 + 1,
                attr: if plus {
                    Some(self.getattr(ctx, *ino)?)
                } else {
                    None
                },
            });
        }
        Ok(Listing { entries, eof: true })
    }

    fn fsstat(&self, _: &Ctx, _: u64) -> Result<FsStat> {
        Ok(FsStat {
            tbytes: 1 << 30,
            fbytes: 1 << 29,
            abytes: 1 << 29,
            tfiles: 1000,
            ffiles: 500,
            afiles: 500,
        })
    }

    fn commit(&self, _: &Ctx, _: u64) -> Result<()> {
        Ok(())
    }
}

const EXPORT: u64 = 7;
const PATH: &[u8] = b"/ws-one-time-0123456789abcdef";

/// A server serving a MemFs as `EXPORT`, armed for one MNT of `PATH`.
fn serve() -> (Arc<Server>, SocketAddr) {
    let server = Server::new(Options {
        key: [9; 32],
        owner: OWNER,
        workers: 4,
        trace: false,
        starting: false,
    });
    server.export(EXPORT, ROOT, Arc::new(MemFs::new()));
    server.arm(PATH, EXPORT);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let s = server.clone();
    std::thread::spawn(move || s.serve(listener));
    (server, addr)
}

fn mounted() -> (Arc<Server>, SocketAddr, Client, Vec<u8>) {
    let (server, addr) = serve();
    let mut c = Client::connect(addr, OWNER, OWNER).unwrap();
    let root = c.mnt(PATH).unwrap().unwrap();
    (server, addr, c, root)
}

// An armed path mounts once, whoever asks first — mount_nfs asks as root — and no other path
// mounts anything.
#[test]
fn an_armed_path_mounts_once() {
    let (server, addr) = serve();
    let mut c = Client::connect(addr, 0, 0).unwrap();
    assert_eq!(c.mnt(b"/a-path-never-armed").unwrap(), Err(13));
    assert!(server.armed(PATH));
    let root = c.mnt(PATH).unwrap().unwrap();
    assert_eq!(root, server.handle(EXPORT, ROOT).to_vec());
    assert!(!server.armed(PATH));
    assert_eq!(c.mnt(PATH).unwrap(), Err(13), "a path mounted twice");
    // Served as a new export, the file system's old handles are stale.
    server.unexport(EXPORT);
    server.export(EXPORT + 1, ROOT, Arc::new(MemFs::new()));
    let mut owner = Client::connect(addr, OWNER, OWNER).unwrap();
    assert_eq!(owner.getattr(&root).unwrap(), Err(nfs3::STALE));
}

#[test]
fn a_forged_or_stale_handle_and_another_user_are_refused() {
    let (server, addr, mut c, root) = mounted();
    assert_eq!(c.getattr(&root).unwrap().unwrap().fileid, ROOT);
    // Another file's number under the root's hash, and a handle of the wrong length.
    let mut forged = root.clone();
    forged[15] ^= 1;
    assert_eq!(c.getattr(&forged).unwrap(), Err(nfs3::BADHANDLE));
    assert_eq!(c.getattr(&root[..16]).unwrap(), Err(nfs3::BADHANDLE));
    // A handle made under another key.
    let other = crate::handle::Handles::new([8; 32]).make(EXPORT, ROOT);
    assert_eq!(c.getattr(&other).unwrap(), Err(nfs3::BADHANDLE));
    // The kernel's call for another user, on a good handle.
    let mut stranger = Client::connect(addr, OWNER + 1, OWNER).unwrap();
    assert_eq!(stranger.getattr(&root).unwrap(), Err(nfs3::ACCES));
    assert_eq!(stranger.lookup(&root, b"x").unwrap(), Err(nfs3::ACCES));
    // No credential at all, but for a ping.
    let mut none = Client::with_cred(addr, Cred::None).unwrap();
    assert!(matches!(
        none.call(crate::NFS_PROGRAM, 3, 0, &[]).unwrap(),
        Reply::Accepted(rpc::Accept::Success, _)
    ));
    assert_eq!(
        none.call(crate::NFS_PROGRAM, 3, nfs3::GETATTR, &[])
            .unwrap(),
        Reply::Denied
    );
    // An export no longer served.
    server.unexport(EXPORT);
    assert_eq!(c.getattr(&root).unwrap(), Err(nfs3::STALE));
}

#[test]
fn files_are_made_written_read_and_listed() {
    let (_, _, mut c, root) = mounted();
    let f = c.create(&root, b"hello.txt", 0o644).unwrap().unwrap();
    assert_eq!(
        c.create(&root, b"hello.txt", 0o644).unwrap(),
        Err(nfs3::EXIST)
    );
    let (n, (before, after)) = c.write(&f, 0, b"hello, world\n").unwrap().unwrap();
    assert_eq!(n, 13);
    let (before, after) = (before.unwrap(), after.unwrap());
    assert_eq!(
        (before.0, after.size),
        (0, 13),
        "the write's before and after"
    );
    let (data, eof) = c.read(&f, 7, 100).unwrap().unwrap();
    assert_eq!((data.as_slice(), eof), (&b"world\n"[..], true));
    let (data, eof) = c.read(&f, 0, 5).unwrap().unwrap();
    assert_eq!((data.as_slice(), eof), (&b"hello"[..], false));
    let (found, attr) = c.lookup(&root, b"hello.txt").unwrap().unwrap();
    assert_eq!(found, f);
    assert_eq!(attr.unwrap().size, 13);
    assert_eq!(c.lookup(&root, b"nope").unwrap(), Err(nfs3::NOENT));
    assert_eq!(
        c.lookup(&root, &[b'x'; 256]).unwrap(),
        Err(nfs3::NAMETOOLONG)
    );

    let d = c.mkdir(&root, b"sub", 0o755).unwrap().unwrap();
    let l = c.symlink(&d, b"link", b"../hello.txt").unwrap().unwrap();
    assert_eq!(c.readlink(&l).unwrap().unwrap(), b"../hello.txt");
    c.link(&f, &d, b"hard").unwrap().unwrap();
    assert_eq!(c.getattr(&f).unwrap().unwrap().nlink, 2);
    assert_eq!(c.rmdir(&root, b"sub").unwrap(), Err(nfs3::NOTEMPTY));
    c.rename(&d, b"hard", &root, b"moved").unwrap().unwrap();
    let listed: Vec<Vec<u8>> = c
        .readdir(&root, 4096)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(
        listed,
        [&b"."[..], b"..", b"hello.txt", b"moved", b"sub"].map(|n| n.to_vec())
    );
    c.remove(&d, b"link").unwrap().unwrap();
    c.rmdir(&root, b"sub").unwrap().unwrap();
    assert_eq!(c.getattr(&d).unwrap(), Err(nfs3::STALE));
    let (rtmax, wtmax) = c.fsinfo(&root).unwrap().unwrap();
    assert_eq!((rtmax, wtmax), (crate::MAX_IO, crate::MAX_IO));
}

#[test]
fn a_listing_pages_through_every_entry_once() {
    let (_, _, mut c, root) = mounted();
    for i in 0..500 {
        c.create(&root, format!("file-{i:04}").as_bytes(), 0o644)
            .unwrap()
            .unwrap();
    }
    // Small replies, so a listing takes many calls.
    let plain = c.readdir(&root, 512).unwrap().unwrap();
    assert_eq!(plain.len(), 502);
    let mut names: Vec<_> = plain.iter().map(|(n, _)| n.clone()).collect();
    names.dedup();
    assert_eq!(names.len(), 502, "an entry listed twice");
    let plus = c.readdirplus(&root, 2048).unwrap().unwrap();
    assert_eq!(plus.len(), 502);
    for e in &plus[2..] {
        let attr = e.attr.unwrap();
        assert_eq!(attr.fileid, e.fileid);
        // Every handle a listing gives works.
        let fh = e.fh.as_ref().unwrap();
        assert_eq!(c.getattr(fh).unwrap().unwrap().fileid, e.fileid);
    }
    // A reply too small for even one entry.
    let mut w = crate::xdr::Writer::new();
    w.opaque(&root).u64(0).fixed(&[0; 8]).u32(8);
    match c
        .call(crate::NFS_PROGRAM, 3, nfs3::READDIR, &w.into_vec())
        .unwrap()
    {
        Reply::Accepted(_, body) => {
            assert_eq!(
                u32::from_be_bytes(body[..4].try_into().unwrap()),
                nfs3::TOOSMALL
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn creates_retransmitted_or_unchecked_behave_as_nfs_says() {
    let (_, _, mut c, root) = mounted();
    let first = c.create_exclusive(&root, b"x", [1; 8]).unwrap().unwrap();
    assert_eq!(
        c.create_exclusive(&root, b"x", [1; 8]).unwrap().unwrap(),
        first,
        "a retransmitted exclusive create"
    );
    assert_eq!(
        c.create_exclusive(&root, b"x", [2; 8]).unwrap(),
        Err(nfs3::EXIST)
    );
    c.write(&first, 0, b"data").unwrap().unwrap();
    // Unchecked, size 0: the existing file is opened and truncated.
    let again = c
        .create_unchecked(
            &root,
            b"x",
            &SetAttr {
                size: Some(0),
                ..SetAttr::default()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(again, first);
    assert_eq!(c.getattr(&first).unwrap().unwrap().size, 0);
}

#[test]
fn setattr_reports_before_and_after() {
    let (_, _, mut c, root) = mounted();
    let f = c.create(&root, b"f", 0o644).unwrap().unwrap();
    let (before, after) = c
        .setattr(
            &f,
            &SetAttr {
                mode: Some(0o600),
                ..SetAttr::default()
            },
        )
        .unwrap()
        .unwrap();
    let after = after.unwrap();
    assert_eq!(after.perm, 0o600);
    assert!(after.ctime > before.unwrap().2, "ctime did not move");
    assert_eq!(c.access(&f, 0x3f).unwrap().unwrap(), 0x1 | 0x4 | 0x8);
}

#[test]
fn concurrent_clients_are_all_answered() {
    let (_, addr, mut c, root) = mounted();
    let files: Vec<Vec<u8>> = (0..8)
        .map(|i| {
            c.create(&root, format!("f{i}").as_bytes(), 0o644)
                .unwrap()
                .unwrap()
        })
        .collect();
    let threads: Vec<_> = files
        .into_iter()
        .enumerate()
        .map(|(i, fh)| {
            std::thread::spawn(move || {
                let mut c = Client::connect(addr, OWNER, OWNER).unwrap();
                let chunk = vec![i as u8; 64 << 10];
                for k in 0..32u64 {
                    c.write(&fh, k * chunk.len() as u64, &chunk)
                        .unwrap()
                        .unwrap();
                }
                // Read as a client does: no more than the server's maximum at a time, to the end.
                let mut data = Vec::new();
                loop {
                    let (got, eof) = c.read(&fh, data.len() as u64, 4 << 20).unwrap().unwrap();
                    assert!(got.len() <= crate::MAX_IO as usize);
                    data.extend_from_slice(&got);
                    if eof {
                        break;
                    }
                }
                assert_eq!(data.len(), 32 * chunk.len());
                assert!(data.iter().all(|b| *b == i as u8));
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
}

#[test]
fn a_record_too_large_ends_the_connection() {
    let (_, addr, _, _) = mounted();
    let mut conn = std::net::TcpStream::connect(addr).unwrap();
    use std::io::Read as _;
    use std::io::Write as _;
    // A fragment claiming 2 GiB.
    conn.write_all(&0xffff_fff0u32.to_be_bytes()).unwrap();
    let mut buf = [0u8; 1];
    assert_eq!(conn.read(&mut buf).unwrap(), 0);
}

// A server starting in place of another holds a call on an export not yet served until it is,
// rather than finding the kernel's handle stale; once ready, an unknown export is stale.
#[test]
fn a_starting_server_waits_for_its_exports() {
    let server = Server::new(Options {
        key: [9; 32],
        owner: OWNER,
        workers: 4,
        trace: false,
        starting: true,
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let s = server.clone();
    std::thread::spawn(move || s.serve(listener));
    let root = server.handle(EXPORT, ROOT);
    let other = server.handle(EXPORT + 1, ROOT);
    let call = std::thread::spawn(move || {
        let mut c = Client::connect(addr, OWNER, OWNER).unwrap();
        let started = std::time::Instant::now();
        let attr = c.getattr(&root).unwrap();
        (attr, started.elapsed())
    });
    std::thread::sleep(std::time::Duration::from_millis(300));
    server.export(EXPORT, ROOT, Arc::new(MemFs::new()));
    let (attr, waited) = call.join().unwrap();
    assert_eq!(attr.unwrap().fileid, ROOT);
    assert!(
        waited >= std::time::Duration::from_millis(250),
        "it did not wait: {waited:?}"
    );
    server.ready();
    let mut c = Client::connect(addr, OWNER, OWNER).unwrap();
    assert_eq!(c.getattr(&other).unwrap(), Err(nfs3::STALE));
}
