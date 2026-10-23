//! A workspace's secrets through the kernel (docs/secrets.md, "Clients"): a mount over a tree with
//! //secret, whose values a stand-in for ok secrets holds, driven by ordinary file-system calls in a
//! user and mount namespace of the test's own.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read as _;
use std::io::Seek as _;
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;

use ok_proto::ok::okfs::v1 as pb;
use ok_secret::Digest;
use ok_secret::SecretBuf;
use okfs_fuse as fuse;

use crate::fs::Mount;
use crate::fs::SecretStore;
use crate::testfix::Entry;
use crate::testfix::Fixture;
use crate::testfix::FixtureFetch;
use crate::testfix::path;

const CHILD: &str = "OKFS_SECRETS_KERNEL_TEST";
const PROD_ACL: &[u8] = b"readers: [prod]\nwriters: [prod]\n";
const OTHER_ACL: &[u8] = b"readers: [other]\nwriters: [other]\n";
const REPO: i64 = 7;
const OPENING: [u8; 32] = [9; 32];

/// ok secrets, as a mount reaches it: the folders whose secrets the caller may not read, those
/// whose secrets they may not write, each path's unseals, and every version kept, by path and
/// digest.
#[derive(Default)]
struct Store {
    /// The repository keeps no secrets: `secret/` is an ordinary folder.
    keeps_none: bool,
    unreadable: Vec<&'static str>,
    unwritable: Vec<&'static str>,
    unseals: Mutex<HashMap<String, u32>>,
    versions: Mutex<HashMap<(String, Digest), Vec<u8>>>,
    writes: Mutex<Vec<(String, Vec<u8>)>>,
}

impl Store {
    fn keep(&self, path: &str, value: &[u8]) -> Digest {
        let d = ok_secret::commit(REPO, path, &OPENING, value);
        self.versions
            .lock()
            .unwrap()
            .insert((path.to_owned(), d), value.to_vec());
        d
    }

    /// Lets the person read `path` once.
    fn unseal(&self, path: &str) {
        *self
            .unseals
            .lock()
            .unwrap()
            .entry(path.to_owned())
            .or_default() += 1;
    }

    fn readable(&self, path: &str) -> bool {
        !self.unreadable.iter().any(|f| path.starts_with(f))
    }

    fn writable(&self, path: &str) -> bool {
        !self.unwritable.iter().any(|f| path.starts_with(f))
    }
}

impl SecretStore for Store {
    fn keeps_secrets(&self) -> bool {
        !self.keeps_none
    }

    fn read(&self, path: &str, digest: &Digest) -> io::Result<SecretBuf> {
        if !self.readable(path) {
            return Err(io::Error::from_raw_os_error(libc::EACCES));
        }
        let mut unseals = self.unseals.lock().unwrap();
        match unseals.get_mut(path) {
            Some(n) if *n > 0 => *n -= 1,
            _ => return Err(io::Error::from_raw_os_error(libc::EACCES)),
        }
        self.versions
            .lock()
            .unwrap()
            .get(&(path.to_owned(), *digest))
            .map(|v| SecretBuf::new(v.clone()))
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EIO))
    }

    fn write(&self, path: &str, value: &[u8]) -> io::Result<Digest> {
        if !self.writable(path) {
            return Err(io::Error::from_raw_os_error(libc::EACCES));
        }
        self.writes
            .lock()
            .unwrap()
            .push((path.to_owned(), value.to_vec()));
        Ok(self.keep(path, value))
    }

    fn may_write(&self, path: &str) -> io::Result<bool> {
        Ok(self.writable(path))
    }
}

#[test]
fn secrets_kernel() {
    assert!(
        std::path::Path::new("/dev/fuse").exists(),
        "no /dev/fuse: the test needs FUSE (docs/building.md, \"Tests\")"
    );
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "secret_tests::secrets_kernel_child",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(CHILD, "1");
    fuse::testns::unshare(&mut cmd);
    let out = cmd.output().expect("run the child");
    assert!(
        out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("okfs-secrets-scenario-done"),
        "the child: {}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn errno(e: io::Error) -> i32 {
    e.raw_os_error().unwrap_or(0)
}

/// Every byte under `dir` (but the mount), for looking for a value that must not be on disk.
fn on_disk(dir: &std::path::Path, mnt: &std::path::Path) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_owned()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p == mnt {
                continue;
            }
            let m = fs::symlink_metadata(&p).unwrap();
            if m.is_dir() {
                stack.push(p);
            } else if m.is_file() {
                out.extend(fs::read(&p).unwrap());
            }
        }
    }
    out
}

/// Whether ListDir marks a file a secret written and not sealed.
fn entry_pending(mount: &Mount, dir: &str, name: &str) -> bool {
    mount
        .list_dir(&path(dir))
        .unwrap()
        .entries
        .iter()
        .any(|e| e.name == name.as_bytes() && e.secret_pending)
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn secrets_kernel_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    // secret/denied/ is neither readable nor writable; secret/readonly/ only readable;
    // secret/writeonly/ only writable.
    let store = Arc::new(Store {
        unreadable: vec!["secret/denied/", "secret/writeonly/"],
        unwritable: vec!["secret/denied/", "secret/readonly/"],
        ..Default::default()
    });
    let password = store.keep("secret/prod/db-password", b"hunter2-correct-horse");
    let denied = store.keep("secret/denied/token", b"not-yours");
    let readonly = store.keep("secret/readonly/token", b"read-me");
    let writeonly = store.keep("secret/writeonly/token", b"old-unseen");
    let password_ptr = ok_secret::pointer(&password);
    let denied_ptr = ok_secret::pointer(&denied);
    let readonly_ptr = ok_secret::pointer(&readonly);
    let writeonly_ptr = ok_secret::pointer(&writeonly);
    let mut fx = Fixture::new();
    let base = fx.tree(vec![
        ("README", Entry::File(b"readme\n")),
        (
            ".ok",
            Entry::Dir(vec![(
                "repository.yaml",
                Entry::File(b"build: none\nsecrets: {}\n"),
            )]),
        ),
        (
            "secret",
            Entry::Dir(vec![
                (
                    "acl.yaml",
                    Entry::File(b"readers: [admins]\nwriters: [admins]\n"),
                ),
                (
                    "prod",
                    Entry::Dir(vec![
                        ("acl.yaml", Entry::File(PROD_ACL)),
                        ("OWNERS", Entry::File(b"user: alice\n")),
                        ("db-password", Entry::File(&password_ptr)),
                    ]),
                ),
                (
                    "denied",
                    Entry::Dir(vec![
                        ("acl.yaml", Entry::File(OTHER_ACL)),
                        ("token", Entry::File(&denied_ptr)),
                    ]),
                ),
                (
                    "readonly",
                    Entry::Dir(vec![
                        ("acl.yaml", Entry::File(OTHER_ACL)),
                        ("token", Entry::File(&readonly_ptr)),
                    ]),
                ),
                (
                    "writeonly",
                    Entry::Dir(vec![
                        ("acl.yaml", Entry::File(OTHER_ACL)),
                        ("token", Entry::File(&writeonly_ptr)),
                    ]),
                ),
            ]),
        ),
    ]);
    let (model, _log) = fx.model(&base);
    let dir = fx.dir.as_ref().unwrap().path().to_owned();
    let mnt = dir.join("mnt");
    fs::create_dir(&mnt).unwrap();
    let fx = Arc::new(fx);
    let mut mount = Mount::new(
        model,
        Arc::new(FixtureFetch(fx.clone(), Arc::new(Mutex::new(Vec::new())))),
        dir.join("staging"),
        Box::new(|| {}),
    )
    .unwrap();
    mount.use_secrets(store.clone());
    let mount = Arc::new(mount);
    let fd = fuse::mount(&mnt, &fuse::MountOptions::default()).expect("mount");
    let session = fuse::Session::new(fd);
    let server = {
        let mount = mount.clone();
        std::thread::spawn(move || session.run(mount, 4, |_| {}))
    };
    let m = |p: &str| mnt.join(p);

    // A secret reads as its value once for each unseal: without one, EACCES.
    assert_eq!(
        errno(fs::read(m("secret/prod/db-password")).unwrap_err()),
        libc::EACCES
    );
    store.unseal("secret/prod/db-password");
    let mut f = fs::File::open(m("secret/prod/db-password")).unwrap();
    let mut read = Vec::new();
    f.read_to_end(&mut read).unwrap();
    assert_eq!(read, b"hunter2-correct-horse");
    let meta = f.metadata().unwrap();
    assert_eq!(meta.len(), 21, "an open secret's size is its value's");
    assert_eq!(meta.mode() & 0o777, 0o600);
    // The open's value is its own: read again through it, with no unseal.
    f.seek(io::SeekFrom::Start(0)).unwrap();
    read.clear();
    f.read_to_end(&mut read).unwrap();
    assert_eq!(read, b"hunter2-correct-horse");
    drop(f);
    assert_eq!(
        fs::metadata(m("secret/prod/db-password")).unwrap().len(),
        0,
        "a secret no open holds is 0 bytes"
    );
    assert_eq!(
        errno(fs::read(m("secret/prod/db-password")).unwrap_err()),
        libc::EACCES,
        "an unseal serves one read"
    );
    // Its folder's acl.yaml reads as itself, with no unseal.
    assert_eq!(fs::read(m("secret/prod/acl.yaml")).unwrap(), PROD_ACL);
    // An OWNERS file is no secret: it reads, and writes, as itself.
    assert_eq!(fs::read(m("secret/prod/OWNERS")).unwrap(), b"user: alice\n");
    fs::write(m("secret/prod/OWNERS"), b"user: alice\nuser: bob\n").unwrap();
    assert_eq!(
        fs::read(m("secret/prod/OWNERS")).unwrap(),
        b"user: alice\nuser: bob\n"
    );
    assert!(!entry_pending(&mount, "secret/prod", "OWNERS"));
    // Not a member: EACCES, unsealed or not.
    store.unseal("secret/denied/token");
    assert_eq!(
        errno(fs::read(m("secret/denied/token")).unwrap_err()),
        libc::EACCES
    );
    assert_eq!(
        errno(fs::write(m("secret/denied/new"), b"x").unwrap_err()),
        libc::EACCES
    );
    // Reading confers no writing...
    store.unseal("secret/readonly/token");
    assert_eq!(fs::read(m("secret/readonly/token")).unwrap(), b"read-me");
    assert_eq!(
        errno(fs::write(m("secret/readonly/token"), b"x").unwrap_err()),
        libc::EACCES
    );
    // ...and writing no reading: a writer replaces a value they cannot read, and reads back what
    // they wrote from memory until it is sealed — after which it is ok secrets' to serve, or not.
    store.unseal("secret/writeonly/token");
    assert_eq!(
        errno(fs::read(m("secret/writeonly/token")).unwrap_err()),
        libc::EACCES
    );
    fs::write(m("secret/writeonly/token"), b"new-value").unwrap();
    assert_eq!(fs::read(m("secret/writeonly/token")).unwrap(), b"new-value");
    assert!(
        mount
            .seal_secret(&path("secret/writeonly/token"))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        errno(fs::read(m("secret/writeonly/token")).unwrap_err()),
        libc::EACCES,
        "a sealed value is no longer the mount's"
    );
    // A name no repository path can hold is refused in the tree.
    assert_eq!(
        errno(
            fs::write(
                m("secret/prod").join(std::ffi::OsStr::from_bytes(b"\xff")),
                b"x"
            )
            .unwrap_err()
        ),
        libc::EINVAL
    );
    assert_eq!(
        errno(fs::create_dir(m("secret").join(std::ffi::OsStr::from_bytes(b"\xfe"))).unwrap_err()),
        libc::EINVAL
    );

    // A new secret is written in memory: the overlay holds nothing of it.
    fs::write(m("secret/prod/api-key"), b"sk-live-0123456789").unwrap();
    assert_eq!(
        fs::read(m("secret/prod/api-key")).unwrap(),
        b"sk-live-0123456789"
    );
    let listing = mount.list_dir(&path("secret/prod")).unwrap();
    let entry = |l: &pb::ListDirReply, name: &str| {
        l.entries
            .iter()
            .find(|e| e.name == name.as_bytes())
            .cloned()
            .unwrap_or_else(|| panic!("no {name}"))
    };
    assert!(entry(&listing, "api-key").secret_pending);
    assert!(!contains(&on_disk(&dir, &mnt), b"sk-live-0123456789"));
    assert!(!contains(&on_disk(&dir, &mnt), b"hunter2-correct-horse"));

    // Sealing keeps the value in ok secrets and puts its pointer in the overlay.
    let sealed = mount
        .seal_secret(&path("secret/prod/api-key"))
        .unwrap()
        .expect("sealed");
    assert!(!sealed.secret_pending);
    let digest = ok_secret::commit(REPO, "secret/prod/api-key", &OPENING, b"sk-live-0123456789");
    assert_eq!(
        fs::read(&sealed.overlay_path).unwrap(),
        ok_secret::pointer(&digest)
    );
    assert_eq!(
        store.writes.lock().unwrap().as_slice(),
        [
            ("secret/writeonly/token".to_owned(), b"new-value".to_vec()),
            (
                "secret/prod/api-key".to_owned(),
                b"sk-live-0123456789".to_vec()
            )
        ]
    );
    store.unseal("secret/prod/api-key");
    assert_eq!(
        fs::read(m("secret/prod/api-key")).unwrap(),
        b"sk-live-0123456789"
    );

    // A secret read and rewritten as it was keeps its pointer: nothing new is stored.
    store.unseal("secret/prod/db-password");
    let mut f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(m("secret/prod/db-password"))
        .unwrap();
    f.write_all(b"hunter2-correct-horse").unwrap();
    drop(f);
    assert!(
        entry(
            &mount.list_dir(&path("secret/prod")).unwrap(),
            "db-password"
        )
        .secret_pending
    );
    let e = mount
        .seal_secret(&path("secret/prod/db-password"))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(&e.overlay_path).unwrap(), password_ptr);
    assert_eq!(store.writes.lock().unwrap().len(), 2);

    // Truncating and appending change the value in memory: the first open to write that keeps
    // the contents reads them, with an unseal.
    store.unseal("secret/prod/db-password");
    let f = fs::OpenOptions::new()
        .write(true)
        .open(m("secret/prod/db-password"))
        .unwrap();
    f.set_len(7).unwrap();
    drop(f);
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(m("secret/prod/db-password"))
        .unwrap();
    f.write_all(b"!").unwrap();
    drop(f);
    assert_eq!(fs::read(m("secret/prod/db-password")).unwrap(), b"hunter2!");
    assert_eq!(fs::metadata(m("secret/prod/db-password")).unwrap().len(), 8);

    // A pointer written as a secret's value sets its pointer (how a conflict is resolved by hand).
    fs::write(m("secret/prod/db-password"), &password_ptr).unwrap();
    let e = mount
        .seal_secret(&path("secret/prod/db-password"))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(&e.overlay_path).unwrap(), password_ptr);
    assert_eq!(store.writes.lock().unwrap().len(), 2);

    // A value is bound to its path: moving or linking a secret crosses file systems, which mv
    // meets by copying.
    for (from, to) in [
        ("secret/prod/api-key", "secret/prod/renamed"),
        ("README", "secret/prod/README"),
        ("secret/prod/api-key", "api-key"),
    ] {
        assert_eq!(
            errno(fs::rename(m(from), m(to)).unwrap_err()),
            libc::EXDEV,
            "{from} -> {to}"
        );
        assert_eq!(
            errno(fs::hard_link(m(from), m(to)).unwrap_err()),
            libc::EXDEV,
            "link {from}"
        );
    }
    // Only files and folders in the tree.
    assert_eq!(
        errno(std::os::unix::fs::symlink("README", m("secret/prod/link")).unwrap_err()),
        libc::EPERM
    );
    fs::create_dir(m("secret/prod/tls")).unwrap();

    // An editor's swap file is a secret held in memory — and its name no secret's, so it is never
    // stored.
    fs::write(m("secret/prod/.db-password.swp"), b"swap of hunter2").unwrap();
    assert!(
        mount
            .seal_secret(&path("secret/prod/.db-password.swp"))
            .unwrap()
            .is_none()
    );
    assert_eq!(store.writes.lock().unwrap().len(), 2);
    fs::remove_file(m("secret/prod/.db-password.swp")).unwrap();

    // Still nothing of any value on disk.
    let disk = on_disk(&dir, &mnt);
    for v in [
        &b"sk-live-0123456789"[..],
        b"hunter2",
        b"swap of hunter2",
        b"not-yours",
        b"read-me",
        b"new-value",
        b"old-unseen",
    ] {
        assert!(
            !contains(&disk, v),
            "{} is on disk",
            String::from_utf8_lossy(v)
        );
    }

    // After a checkout, a value is read again — with an unseal.
    mount.forget_secrets();
    assert_eq!(
        errno(fs::read(m("secret/prod/api-key")).unwrap_err()),
        libc::EACCES
    );
    store.unseal("secret/prod/api-key");
    assert_eq!(
        fs::read(m("secret/prod/api-key")).unwrap(),
        b"sk-live-0123456789"
    );

    // In a repository that keeps no secrets, secret/ is an ordinary folder: its files are written
    // to the overlay and recorded as any, and symlinks and renames are as anywhere.
    let mut fx2 = Fixture::new();
    let plain = fx2.tree(vec![(
        "secret",
        Entry::Dir(vec![("notes.txt", Entry::File(b"plain notes\n"))]),
    )]);
    let (model2, _log2) = fx2.model(&plain);
    let dir2 = fx2.dir.as_ref().unwrap().path().to_owned();
    let mnt2 = dir2.join("mnt");
    fs::create_dir(&mnt2).unwrap();
    let mut ordinary = Mount::new(
        model2,
        Arc::new(FixtureFetch(
            Arc::new(fx2),
            Arc::new(Mutex::new(Vec::new())),
        )),
        dir2.join("staging"),
        Box::new(|| {}),
    )
    .unwrap();
    ordinary.use_secrets(Arc::new(Store {
        keeps_none: true,
        ..Default::default()
    }));
    let ordinary = Arc::new(ordinary);
    let fd2 = fuse::mount(&mnt2, &fuse::MountOptions::default()).expect("mount");
    let server2 = {
        let mount = ordinary.clone();
        let session = fuse::Session::new(fd2);
        std::thread::spawn(move || session.run(mount, 4, |_| {}))
    };
    assert_eq!(
        fs::read(mnt2.join("secret/notes.txt")).unwrap(),
        b"plain notes\n"
    );
    fs::write(mnt2.join("secret/notes.txt"), b"edited notes\n").unwrap();
    let e = ordinary
        .list_dir(&path("secret"))
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.name == b"notes.txt")
        .unwrap();
    assert!(!e.secret_pending && !e.secret_lost);
    assert_eq!(fs::read(&e.overlay_path).unwrap(), b"edited notes\n");
    assert!(
        ordinary
            .seal_secret(&path("secret/notes.txt"))
            .unwrap()
            .is_none()
    );
    std::os::unix::fs::symlink("notes.txt", mnt2.join("secret/link")).unwrap();
    fs::rename(mnt2.join("secret/notes.txt"), mnt2.join("notes.txt")).unwrap();
    assert!(store.writes.lock().unwrap().len() == 2);
    // Once the workspace's own settings keep secrets, its secrets stay off disk, whatever ok
    // secrets last said of main.
    fs::create_dir(mnt2.join(".ok")).unwrap();
    fs::write(
        mnt2.join(".ok/repository.yaml"),
        b"build: none\nsecrets: {}\n",
    )
    .unwrap();
    fs::write(mnt2.join("secret/token"), b"new-token-value").unwrap();
    assert!(entry_pending(&ordinary, "secret", "token"));
    assert!(!contains(&on_disk(&dir2, &mnt2), b"new-token-value"));
    fuse::unmount(&mnt2).unwrap();
    server2.join().unwrap().unwrap();

    fuse::unmount(&mnt).unwrap();
    server.join().unwrap().unwrap();
    println!("okfs-secrets-scenario-done");
}
