//! A mount against macOS's kernel: the model served over NFS from the fixture's trees, mounted with
//! mount_nfs, and driven by ordinary file-system calls — the counterpart of fs_tests's FUSE mount.

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::MetadataExt as _;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use okfs_nfs as nfs;

use crate::fs::Mount;
use crate::model::ROOT;
use crate::model::resolved_terms;
use crate::nfs::NfsMount;
use crate::nfs::Refresh;
use crate::testfix::Entry;
use crate::testfix::Fixture;
use crate::testfix::FixtureFetch;
use crate::testfix::base_tree;

/// Unmounts when dropped, forcing it, so a failed test leaves no mount behind.
struct Mounted(std::path::PathBuf);

impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = Command::new("/sbin/umount").arg("-f").arg(&self.0).status();
    }
}

#[test]
fn kernel() {
    let mut fx = Fixture::new();
    let base = base_tree(&mut fx);
    let next = fx.tree(vec![
        ("README", Entry::File(b"readme, changed\n")),
        ("added.txt", Entry::File(b"new in the next commit\n")),
        (
            "src",
            Entry::Dir(vec![("a.rs", Entry::File(b"fn a() { next() }\n"))]),
        ),
    ]);
    let (model, _log) = fx.model(&base);
    let dir = fs::canonicalize(fx.dir.as_ref().unwrap().path()).unwrap();
    let mnt = dir.join("mnt");
    fs::create_dir(&mnt).unwrap();
    let fx = Arc::new(fx);
    let mount = Arc::new(
        Mount::new(
            model,
            Arc::new(FixtureFetch(
                fx.clone(),
                Arc::new(std::sync::Mutex::new(Vec::new())),
            )),
            dir.join("staging"),
            Box::new(|| {}),
        )
        .unwrap(),
    );
    let nfs_mount = Arc::new(NfsMount::new(
        mount.clone(),
        Arc::new(Refresh::new(mnt.clone())),
    ));
    let server = nfs::Server::new(nfs::Options {
        key: [5; 32],
        owner: unsafe { libc::geteuid() },
        workers: 8,
        trace: false,
        starting: false,
    });
    server.export(1, ROOT, nfs_mount);
    server.arm(b"/okfs-test", 1);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    {
        let server = server.clone();
        std::thread::spawn(move || server.serve(listener));
    }
    let status = Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(format!(
            "vers=3,tcp,locallocks,intr,nobrowse,noquota,port={port},mountport={port}"
        ))
        .arg("localhost:/okfs-test")
        .arg(&mnt)
        .status()
        .unwrap();
    assert!(status.success(), "mount_nfs: {status}");
    let _mounted = Mounted(mnt.clone());

    // The base, read through the kernel.
    let mut names: Vec<String> = fs::read_dir(&mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, [".jj", "README", "docs", "link", "run.sh", "src"]);
    assert_eq!(fs::read(mnt.join("src/deep/c.rs")).unwrap(), b"fn c() {}\n");
    assert_eq!(fs::metadata(mnt.join("src/a.rs")).unwrap().len(), 10);
    assert_eq!(
        fs::metadata(mnt.join("run.sh")).unwrap().mode() & 0o777,
        0o755
    );
    assert_eq!(
        fs::read_link(mnt.join("link")).unwrap(),
        std::path::Path::new("README")
    );

    // Writing: an edit, a new file, a directory, a rename, a deletion, a chmod.
    fs::write(mnt.join("docs/guide.md"), b"# guide, edited\n").unwrap();
    assert_eq!(
        fs::read(mnt.join("docs/guide.md")).unwrap(),
        b"# guide, edited\n"
    );
    fs::create_dir(mnt.join("made")).unwrap();
    fs::write(mnt.join("made/new.txt"), b"new\n").unwrap();
    fs::rename(mnt.join("made/new.txt"), mnt.join("made/moved.txt")).unwrap();
    fs::remove_file(mnt.join("src/b.rs")).unwrap();
    fs::set_permissions(
        mnt.join("run.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o644),
    )
    .unwrap();
    {
        let m = &mut mount.model();
        let p = |s: &str| crate::testfix::path(s);
        assert!(m.resolve(&p("made/moved.txt")).unwrap().is_some());
        assert!(m.resolve(&p("src/b.rs")).unwrap().is_none());
    }

    // A checkout under the kernel, seen the moment the refresh is asked: a changed file's new
    // contents and size, a new file, and a gone directory.
    assert_eq!(fs::read(mnt.join("README")).unwrap(), b"readme\n");
    assert!(mnt.join("src/deep").exists());
    let recorded = mount.model().recorded().0.clone();
    let (_, stale) = mount
        .with(|m| m.check_out(recorded.clone(), resolved_terms(next.clone()), &[]))
        .unwrap();
    crate::nfs::ask(&mount.invalidate(&stale));
    assert_eq!(fs::read(mnt.join("README")).unwrap(), b"readme, changed\n");
    assert_eq!(
        fs::read(mnt.join("added.txt")).unwrap(),
        b"new in the next commit\n"
    );
    assert_eq!(
        fs::read(mnt.join("src/a.rs")).unwrap(),
        b"fn a() { next() }\n"
    );
    assert!(
        !mnt.join("src/deep").exists(),
        "the gone directory is still listed"
    );
    // What was written stays.
    assert_eq!(fs::read(mnt.join("made/moved.txt")).unwrap(), b"new\n");

    // A file held open reads its new contents after a checkout changes it, as an editor's would.
    let held = fs::File::open(mnt.join("README")).unwrap();
    let recorded = mount.model().recorded().0.clone();
    let (_, stale) = mount
        .with(|m| m.check_out(recorded.clone(), resolved_terms(base.clone()), &[]))
        .unwrap();
    crate::nfs::ask(&mount.invalidate(&stale));
    let mut text = String::new();
    std::io::Read::read_to_string(&mut &held, &mut text).unwrap();
    assert_eq!(text, "readme\n");
    std::thread::sleep(Duration::from_millis(10));
}
