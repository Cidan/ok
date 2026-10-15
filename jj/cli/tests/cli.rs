//! ok's jj on the command line, end to end (docs/jj.md): `jj create`, `jj
//! clone`, `jj cl`, `jj cl get`, and `jj sync`, run as the built binary against an ok scm
//! the test runs in its sandbox (jj/testscm), as two people. ok control lands
//! through the protocol.
//!
//! Every scenario runs twice: in real checkouts (`--real`) and in virtual workspaces
//! (docs/okfs.md), so the two being the same is a test. A virtual run re-executes the test
//! binary in a user and mount namespace of its own (the target binds `/dev/fuse`), where each
//! person runs their own okfs daemon and every `jj`, and the test's own file reads and writes, see
//! its mounts.
//!
//! Linux only: its virtual scenarios mount okfs in namespaces of their own, and its SCM runs where ok's
//! server does.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use ok_store::pb;

const ALICE: &str = "alice.human.example.com";
const BOB: &str = "bob.human.example.com";

/// The jj binary this workspace builds.
fn jj_bin() -> String {
    std::env::var("CARGO_BIN_EXE_jj").expect("CARGO_BIN_EXE_jj: run under cargo test")
}

/// Set in a virtual run's re-executed test binary.
const IN_NAMESPACE: &str = "OK_CLI_E2E_VIRTUAL";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Real,
    Virtual,
}

/// Runs the test `child` (a `_virtual` scenario) in a user and mount namespace of its own.
fn in_namespace(child: &str) {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", child, "--nocapture", "--test-threads=1"])
        .env(IN_NAMESPACE, "1");
    okfs_fuse::testns::unshare(&mut cmd);
    // Its stderr streams, so a scenario that stalls shows how far it got.
    cmd.stderr(Stdio::inherit());
    let out = cmd
        .output()
        .expect("re-run the test binary in its own namespaces");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains(&format!("{child}: scenario done")),
        "{child} in its namespace: {}\n{stdout}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Whether this is a virtual run's re-executed binary (else a virtual scenario does nothing: its
/// `in_namespace` caller runs it).
fn in_virtual_run() -> bool {
    std::env::var_os(IN_NAMESPACE).is_some()
}

/// One person at a workstation: their own home (so their own jj configuration) and the
/// principal a loopback SCM knows them by; in a virtual run, their own okfs daemon.
struct Person {
    home: tempfile::TempDir,
    principal: &'static str,
    mode: Mode,
    daemon: Option<Child>,
    /// The SCM this person's jj reaches (the test SCM, or a proxy to it).
    endpoint: String,
}

impl Person {
    fn new(principal: &'static str, mode: Mode) -> Self {
        let mut p = Self {
            home: tempfile::tempdir().unwrap(),
            principal,
            mode,
            daemon: None,
            endpoint: ok_testscm::endpoint().to_owned(),
        };
        if mode == Mode::Virtual {
            assert!(
                in_virtual_run(),
                "a virtual scenario runs through in_namespace"
            );
            p.daemon = Some(Person::start_daemon(p.home.path(), &p.command(&[])));
        }
        p
    }

    /// Starts a daemon with `env`'s environment, logging to `home/daemon.log`, and waits until it
    /// answers: it binds its control socket before mounting the workspaces it had, and answers
    /// requests only once they are mounted.
    fn start_daemon(home: &Path, env: &Command) -> Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(home.join("daemon.log"))
            .unwrap();
        let socket = home.join("run/control.sock");
        let _ = std::fs::remove_file(&socket);
        let mut cmd = Command::new(jj_bin());
        cmd.args(["fs", "daemon"]).env_clear();
        for (k, v) in env.get_envs() {
            if let Some(v) = v {
                cmd.env(k, v);
            }
        }
        let child = cmd
            .current_dir(home)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("start the okfs daemon");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !socket.exists() {
            assert!(
                Instant::now() < deadline,
                "the daemon did not start:\n{}",
                std::fs::read_to_string(home.join("daemon.log")).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let mut status = Command::new(jj_bin());
        status
            .args(["--no-pager", "fs", "status"])
            .env_clear()
            .current_dir(home);
        for (k, v) in env.get_envs() {
            if let Some(v) = v {
                status.env(k, v);
            }
        }
        let out = status.output().expect("ask the okfs daemon its status");
        assert!(
            out.status.success(),
            "the daemon did not answer:\n{}{}\n--- daemon log ---\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
            std::fs::read_to_string(home.join("daemon.log")).unwrap_or_default()
        );
        child
    }

    fn daemon_log(&self) -> String {
        std::fs::read_to_string(self.home.path().join("daemon.log")).unwrap_or_default()
    }

    /// `jj args…`, as this person: their home, principal, and daemon.
    fn command(&self, args: &[&str]) -> Command {
        let handle = self.principal.split('.').next().unwrap();
        let mut cmd = Command::new(jj_bin());
        cmd.args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("HOME", self.home.path())
            .env("OK_DEV_PRINCIPAL", self.principal)
            .env("JJ_USER", handle)
            .env("JJ_EMAIL", format!("{handle}@example.com"))
            .env("OKFS_STATE_DIR", self.home.path().join("state"))
            .env("OKFS_RUNTIME_DIR", self.home.path().join("run"))
            .env("OKFS_DAEMON", "external");
        cmd
    }

    /// Runs `jj args…` in `dir` (relative to the person's home). A clone or create is real or
    /// virtual as the person's mode says.
    fn jj(&self, dir: &str, args: &[&str]) -> Output {
        self.jj_with(dir, args, self.mode == Mode::Real)
    }

    /// Runs `jj args…`, a clone or create made real when `real`.
    fn jj_with(&self, dir: &str, args: &[&str], real: bool) -> Output {
        let cwd = self.home.path().join(dir);
        std::fs::create_dir_all(&cwd).unwrap();
        let mut all = vec!["--no-pager", "--color=never", "--config"];
        let endpoint = format!("ok.endpoint={}", self.endpoint);
        all.push(&endpoint);
        all.extend_from_slice(args);
        if real && matches!(args.first(), Some(&"clone") | Some(&"create")) {
            all.push("--real");
        }
        eprintln!(
            "[{}] jj {}",
            self.principal.split('.').next().unwrap(),
            args.join(" ")
        );
        let mut child = self
            .command(&all)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("run jj");
        // Drained as it runs, so a full pipe never stalls it.
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let out_t = std::thread::spawn(move || {
            let mut b = Vec::new();
            std::io::Read::read_to_end(&mut stdout, &mut b).ok();
            b
        });
        let err_t = std::thread::spawn(move || {
            let mut b = Vec::new();
            std::io::Read::read_to_end(&mut stderr, &mut b).ok();
            b
        });
        let deadline = Instant::now() + Duration::from_secs(120);
        let status = loop {
            if let Some(s) = child.try_wait().unwrap() {
                break s;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "jj {args:?} did not finish in two minutes\n--- daemon log ---\n{}",
                    self.daemon_log()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        Output {
            status,
            stdout: out_t.join().unwrap(),
            stderr: err_t.join().unwrap(),
        }
    }

    /// Runs jj and requires it to succeed; returns what it printed.
    fn ok(&self, dir: &str, args: &[&str]) -> String {
        let out = self.jj(dir, args);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.status.success(),
            "jj {args:?} failed:\n{text}\n--- daemon log ---\n{}",
            self.daemon_log()
        );
        text
    }

    /// Runs jj and requires it to fail; returns what it printed.
    fn fails(&self, dir: &str, args: &[&str]) -> String {
        let out = self.jj(dir, args);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.status.success(), "jj {args:?} succeeded:\n{text}");
        text
    }

    fn write(&self, path: &str, contents: &str) {
        let p = self.home.path().join(path);
        let wrote = std::fs::create_dir_all(p.parent().unwrap())
            .and_then(|()| std::fs::write(&p, contents));
        if let Err(e) = wrote {
            panic!(
                "writing {path}: {e}\n--- daemon log ---\n{}",
                self.daemon_log()
            );
        }
    }

    fn exists(&self, path: &str) -> bool {
        Path::new(&self.home.path().join(path)).exists()
    }
}

impl Drop for Person {
    /// Stops the daemon first, which unmounts the person's workspaces, so their home is removed
    /// from disk rather than through a mount.
    fn drop(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            unsafe {
                libc::kill(d.id() as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            while d.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = d.kill();
            let _ = d.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_upload_stack_on_someone_else_and_sync() {
    if !ok_testscm::available() {
        return;
    }
    create_upload_stack(Mode::Real).await;
}

#[test]
fn create_upload_stack_on_someone_else_and_sync_virtual() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("create_upload_stack_on_someone_else_and_sync_virtual_run");
}

#[tokio::test(flavor = "multi_thread")]
async fn create_upload_stack_on_someone_else_and_sync_virtual_run() {
    if !ok_testscm::available() {
        return;
    }
    if in_virtual_run() {
        create_upload_stack(Mode::Virtual).await;
        println!("create_upload_stack_on_someone_else_and_sync_virtual_run: scenario done");
    }
}

async fn create_upload_stack(mode: Mode) {
    let name = format!("cli-{}-{mode:?}", std::process::id()).to_lowercase();
    let alice = Person::new(ALICE, mode);
    let bob = Person::new(BOB, mode);

    // alice makes the repository, and uploads a change as a draft CL.
    let out = alice.ok(".", &["create", &name, "repo"]);
    assert!(
        out.contains(&format!("Created repository {name} on ok")),
        "{out}"
    );
    alice.write("repo/a.txt", "one\n");
    alice.ok("repo", &["describe", "-m", "add a"]);
    let out = alice.ok("repo", &["cl"]);
    assert!(out.contains("CL 1 created"), "{out}");
    assert!(
        out.contains(&format!("{}/{name}/cl/1", alice.endpoint)),
        "{out}"
    );
    let out = alice.fails("repo", &["cl"]);
    assert!(out.contains("Nothing new to upload"), "{out}");
    let out = alice.fails("repo", &["cl", "get", "1"]);
    assert!(out.contains("already in your view"), "{out}");

    // bob clones it, gets alice's CL, and stacks his own on it.
    let out = bob.ok(".", &["clone", &name, "repo"]);
    assert!(out.contains("as workspace"), "{out}");
    // A second clone into a directory of the same name is another workspace, with the next
    // free name; one named explicitly and taken is refused.
    let first = out
        .split("as workspace ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap()
        .to_owned();
    let out = bob.ok("again", &["clone", &name, "repo"]);
    assert!(out.contains(&format!("as workspace {first}-2")), "{out}");
    let out = bob.fails("third", &["clone", &name, "repo", "--workspace", &first]);
    assert!(out.contains("you have a workspace named"), "{out}");
    let out = bob.ok("repo", &["cl", "get", "1"]);
    assert!(out.contains("CL 1 patchset 1 is cl/1@ok"), "{out}");
    bob.ok("repo", &["new", "cl/1@ok", "-m", "bob on alice"]);
    bob.write("repo/b.txt", "bob\n");
    let out = bob.ok("repo", &["cl", "--ready"]);
    assert!(out.contains("CL 2 created"), "{out}");
    assert!(
        !out.contains("CL 1"),
        "bob's upload stops at alice's CL:\n{out}"
    );

    // An undescribed change is refused before ok is asked, naming the fix.
    bob.ok("repo", &["new"]);
    bob.write("repo/c.txt", "unfinished\n");
    let out = bob.fails("repo", &["cl", "-r", "@"]);
    assert!(out.contains("has no description"), "{out}");
    assert!(out.contains("jj describe"), "{out}");
    bob.ok("repo", &["abandon", "@"]);

    // ok control sends alice's CL for review, and the landing queue lands it; everyone syncs.
    let repo = ok_testscm::repositories(ALICE)
        .await
        .get_repository(pb::GetRepositoryRequest {
            key: Some(pb::get_repository_request::Key::Name(name.clone())),
        })
        .await
        .unwrap()
        .into_inner();
    let mut control = ok_testscm::review(ok_testscm::CONTROL).await;
    let mut queue = ok_testscm::landing(ok_testscm::AUTOMATION).await;
    control
        .transition(pb::TransitionRequest {
            repository_id: repo.id,
            number: 1,
            kind: pb::transition_request::Kind::RequestReview as i32,
            actor: ALICE.to_owned(),
            reason: String::new(),
        })
        .await
        .unwrap();
    let land = |number: i64, head: Vec<u8>| pb::LandRequest {
        repository_id: repo.id,
        items: vec![pb::land_request::Item {
            number,
            patchset: 1,
            candidate: Vec::new(),
        }],
        actor: ALICE.to_owned(),
        expected_head: head,
        dry_run: false,
    };
    let landed = queue
        .land(land(1, repo.head.clone()))
        .await
        .unwrap()
        .into_inner();

    let out = bob.ok("repo", &["sync"]);
    assert!(out.contains("main is at"), "{out}");
    assert!(out.contains("CL 1 landed as"), "{out}");
    let out = bob.ok(
        "repo",
        &["log", "-r", "trunk()", "--no-graph", "-T", "commit_id"],
    );
    assert_eq!(out.trim(), hex(&landed.head), "bob's trunk() is ok's main");

    // bob's CL lands on alice's; alice syncs and has both changes.
    let head = landed.head.clone();
    queue.land(land(2, head)).await.unwrap();
    let out = alice.ok("repo", &["sync"]);
    assert!(out.contains("main is at"), "{out}");
    assert!(
        alice.exists("repo/b.txt"),
        "alice's working copy has bob's file after syncing"
    );
    let out = alice.ok(
        "repo",
        &["log", "-r", "trunk()", "--no-graph", "-T", "description"],
    );
    assert_eq!(out.trim(), "bob on alice");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Files up to 50 MiB, the largest the SCM holds: ok's jj snapshots them (upstream stops at
/// 1 MiB), leaves a larger one untracked, saying so, and the SCM refuses one a person's
/// configuration let through.
#[tokio::test(flavor = "multi_thread")]
async fn files_up_to_fifty_mebibytes() {
    if !ok_testscm::available() {
        return;
    }
    big_files(Mode::Real);
}

#[test]
fn files_up_to_fifty_mebibytes_virtual() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("files_up_to_fifty_mebibytes_virtual_run");
}

#[test]
fn files_up_to_fifty_mebibytes_virtual_run() {
    if !ok_testscm::available() {
        return;
    }
    if in_virtual_run() {
        big_files(Mode::Virtual);
        println!("files_up_to_fifty_mebibytes_virtual_run: scenario done");
    }
}

fn big_files(mode: Mode) {
    let name = format!("big-{}-{mode:?}", std::process::id()).to_lowercase();
    let alice = Person::new(ALICE, mode);
    alice.ok(".", &["create", &name, "repo"]);
    let two = alice.home.path().join("repo/two.bin");
    std::fs::write(&two, vec![7u8; 2 << 20]).unwrap();
    alice.ok("repo", &["describe", "-m", "two mebibytes"]);
    let out = alice.ok("repo", &["cl"]);
    assert!(out.contains("CL 1 created"), "{out}");

    alice.ok("repo", &["new", "-m", "too big"]);
    let big = alice.home.path().join("repo/big.bin");
    std::fs::write(&big, vec![9u8; (50 << 20) + 1]).unwrap();
    let out = alice.ok("repo", &["st"]);
    assert!(
        out.contains("? big.bin")
            && out.contains("the maximum size allowed is 50.0MiB (52428800 bytes)"),
        "{out}"
    );
    let raised = ["--config", "snapshot.max-new-file-size=60MiB"];
    let out = alice.fails("repo", &[&raised[..], &["cl"]].concat());
    assert!(out.contains("ok holds files up to 50 MiB"), "{out}");
}

impl Person {
    /// The files `dir`'s working-copy commit tracks.
    fn tracked(&self, dir: &str) -> Vec<String> {
        let out = self.jj(dir, &["file", "list"]);
        assert!(
            out.status.success(),
            "jj file list failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

/// Ignore files (docs/jj.md, "Ignoring files"): a `.ignore` in any directory, as jj
/// reads `.gitignore` — which it still does. A new file one names is not tracked, a deeper
/// file's rules win, one written with the files it names applies at once, a tracked file stays
/// tracked, and a clone made before ok's jj had its own working copy reads them too.
#[tokio::test(flavor = "multi_thread")]
async fn ignore_files() {
    if !ok_testscm::available() {
        return;
    }
    ignores(Mode::Real);
}

#[test]
fn ignore_files_virtual() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("ignore_files_virtual_run");
}

#[test]
fn ignore_files_virtual_run() {
    if !ok_testscm::available() {
        return;
    }
    if in_virtual_run() {
        ignores(Mode::Virtual);
        println!("ignore_files_virtual_run: scenario done");
    }
}

fn ignores(mode: Mode) {
    let name = format!("ignore-{}-{mode:?}", std::process::id()).to_lowercase();
    let alice = Person::new(ALICE, mode);
    alice.ok(".", &["create", &name, "repo"]);
    let wc_type = alice.home.path().join("repo/.jj/working_copy/type");
    assert_eq!(
        std::fs::read_to_string(&wc_type).unwrap(),
        if mode == Mode::Real { "ok" } else { "okfs" }
    );

    for (path, contents) in [
        ("repo/.ignore", "*.log\nbuild/\n"),
        ("repo/sub/.ignore", "!keep.log\n"),
        ("repo/.gitignore", "*.tmp\n"),
        ("repo/a.txt", "a\n"),
        ("repo/top.log", "x\n"),
        ("repo/build/out.bin", "x\n"),
        ("repo/sub/keep.log", "x\n"),
        ("repo/sub/drop.log", "x\n"),
        ("repo/scratch.tmp", "x\n"),
    ] {
        alice.write(path, contents);
    }
    assert_eq!(
        alice.tracked("repo"),
        [
            ".gitignore",
            ".ignore",
            "a.txt",
            "sub/.ignore",
            "sub/keep.log"
        ]
    );
    let status = alice.ok("repo", &["st"]);
    for ignored in ["top.log", "build", "drop.log", "scratch.tmp"] {
        assert!(!status.contains(ignored), "{ignored} in:\n{status}");
    }

    // An .ignore written with the files it names applies in the same snapshot.
    alice.write("repo/new/.ignore", "secret\n");
    alice.write("repo/new/secret", "x\n");
    alice.write("repo/new/visible", "x\n");
    let tracked = alice.tracked("repo");
    assert!(
        tracked.contains(&"new/visible".to_owned()) && !tracked.contains(&"new/secret".to_owned()),
        "{tracked:?}"
    );

    // A tracked file an .ignore comes to name stays tracked.
    alice.write("repo/.ignore", "*.log\nbuild/\na.txt\n");
    assert!(alice.tracked("repo").contains(&"a.txt".to_owned()));

    // A clone that recorded jj's own working copy reads .ignore files as well.
    if mode == Mode::Virtual {
        return;
    }
    std::fs::write(&wc_type, "local").unwrap();
    alice.write("repo/later.log", "x\n");
    alice.write("repo/later.txt", "x\n");
    let tracked = alice.tracked("repo");
    assert!(
        tracked.contains(&"later.txt".to_owned()) && !tracked.contains(&"later.log".to_owned()),
        "{tracked:?}"
    );
}

impl Person {
    /// The latest checkpoint `jj fs status` reports for the workspace mounted at `dir` (0: none).
    fn latest_checkpoint(&self, dir: &str) -> i64 {
        let mount = self.home.path().join(dir).display().to_string();
        let out = self.ok(".", &["fs", "status"]);
        let mut lines = out.lines();
        while let Some(l) = lines.next() {
            if l.ends_with(&format!(" at {mount}")) {
                for l in lines.by_ref() {
                    if let Some(n) = l.trim().strip_prefix("latest checkpoint: ") {
                        return n.parse().unwrap_or(0);
                    }
                }
            }
        }
        panic!("no workspace at {mount} in:\n{out}");
    }

    /// Waits until the workspace at `dir` has a checkpoint newer than `after`.
    fn checkpoint_after(&self, dir: &str, after: i64) -> i64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let n = self.latest_checkpoint(dir);
            if n > after {
                return n;
            }
            if Instant::now() >= deadline {
                let status = self.jj(dir, &["fs", "status"]);
                panic!(
                    "no checkpoint after {after}:\n{}{}\n--- daemon log ---\n{}",
                    String::from_utf8_lossy(&status.stdout),
                    String::from_utf8_lossy(&status.stderr),
                    self.daemon_log()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Kills the daemon outright (as a crash would) and starts it again.
    fn crash_daemon(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            d.kill().unwrap();
            d.wait().unwrap();
        }
        let restarted = Person::start_daemon(self.home.path(), &self.command(&[]));
        self.daemon = Some(restarted);
    }
}

/// What only a virtual workspace does (docs/okfs.md): the daemon keeps every write in a
/// checkpoint without a jj command; another machine takes the workspace over at its latest
/// checkpoint, after which the first machine's mount refuses writes; a daemon that crashes is
/// started again with its mounts and written files intact; checkpoints are listed and restored;
/// and `jj fs forget` unmounts the workspace and drops it.
#[test]
fn virtual_workspaces() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("virtual_workspaces_run");
}

#[test]
fn virtual_workspaces_run() {
    if !ok_testscm::available() {
        return;
    }
    if !in_virtual_run() {
        return;
    }
    let name = format!("vw-{}", std::process::id());
    let laptop = Person::new(ALICE, Mode::Virtual);
    let out = laptop.ok(".", &["create", &name, "repo"]);
    let workspace = out
        .split("as workspace ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap()
        .to_owned();

    // A workspace in which nothing was written has no checkpoint — not even once the daemon has
    // checked it at mount, which it does within a tick of its checkpointer (250 ms).
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        assert_eq!(
            laptop.latest_checkpoint("repo"),
            0,
            "a checkpoint of nothing written"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // A write is kept in ok with no jj command run.
    laptop.write("repo/notes.txt", "kept without a snapshot\n");
    let first = laptop.checkpoint_after("repo", 0);

    // Another machine of the same person attaches the workspace: the laptop hands it over at a
    // final checkpoint, and its mount refuses writes from then on.
    let mut desk = Person::new(ALICE, Mode::Virtual);
    let out = desk.ok(".", &["clone", &name, "w", "--attach", &workspace]);
    assert!(
        out.contains(&format!("Attached workspace {workspace}")),
        "{out}"
    );
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w/notes.txt")).unwrap(),
        "kept without a snapshot\n"
    );
    let refused = std::fs::write(laptop.home.path().join("repo/late.txt"), "late\n").unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(libc::EROFS), "{refused}");
    let status = laptop.ok(".", &["fs", "status"]);
    assert!(status.contains("lost: handed over to machine"), "{status}");

    // jj sees the checkpoint's file as a change of the working copy, as written.
    let out = desk.ok("w", &["st"]);
    assert!(out.contains("A notes.txt"), "{out}");

    // The daemon crashes with a file written and not yet in a checkpoint: started again, the
    // mount is back with the file.
    desk.write("w/during.txt", "written before the crash\n");
    desk.crash_daemon();
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::read_to_string(desk.home.path().join("w/during.txt")).is_err() {
        assert!(
            Instant::now() < deadline,
            "the mount did not come back:\n{}",
            desk.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w/during.txt")).unwrap(),
        "written before the crash\n"
    );
    let after_crash = desk.checkpoint_after("w", first);

    // Checkpoints, newest first, name the machine; restoring the first brings back its tree.
    let out = desk.ok("w", &["fs", "checkpoints"]);
    let listed: Vec<i64> = out
        .lines()
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect();
    assert!(
        listed.contains(&first) && listed.contains(&after_crash),
        "{out}"
    );
    assert!(
        listed.windows(2).all(|w| w[0] > w[1]),
        "newest first:\n{out}"
    );
    desk.ok("w", &["fs", "restore", &first.to_string()]);
    assert!(
        !desk.exists("w/during.txt"),
        "restored to before during.txt was written"
    );
    assert!(desk.exists("w/notes.txt"));
    // A restore is an operation: undone, the file is back.
    desk.ok("w", &["undo"]);
    assert!(desk.exists("w/during.txt"));

    // A workspace is one jj workspace, isolated from every other (docs/scm.md,
    // "Decisions", 3): jj refuses to add another to it, and attaching one that was never made is
    // refused too.
    let refused = desk.fails(
        "w",
        &["workspace", "add", "../w2", "--name", "second", "-r", "@"],
    );
    assert!(
        refused.contains("an ok workspace is one jj workspace"),
        "{refused}"
    );
    assert!(!desk.exists("w2/.jj"), "no workspace was added");
    let refused = desk.fails("w", &["workspace", "rename", "elsewhere"]);
    assert!(
        refused.contains("keeps the name ok knows it by"),
        "{refused}"
    );
    let refused = desk.fails(".", &["clone", &name, "w9", "--attach", "never-made"]);
    assert!(refused.contains("no workspace named"), "{refused}");

    // A second clone is a workspace of its own: its log names only its own working copy, it has
    // none of the first's work, and the first has none of its.
    let out = desk.ok(".", &["clone", &name, "w2"]);
    let second = out
        .split("as workspace ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap()
        .to_owned();
    assert_ne!(second, workspace);
    assert!(!desk.exists("w2/notes.txt"), "the second clone is on main");
    desk.write("w2/second.txt", "in the second workspace\n");
    let out = desk.ok("w2", &["st"]);
    assert!(out.contains("A second.txt"), "{out}");
    for (dir, own, other) in [("w", &workspace, &second), ("w2", &second, &workspace)] {
        let list = desk.ok(dir, &["workspace", "list"]);
        assert!(
            list.contains(own.as_str()) && !list.contains(other.as_str()),
            "{dir}: {list}"
        );
    }
    let out = desk.ok("w", &["st"]);
    assert!(!out.contains("second.txt"), "{out}");
    // A stale working copy — moved to another tree by a command that left the files alone — is
    // forgotten without being brought up to date first.
    desk.ok("w2", &["new", "@-", "--ignore-working-copy"]);
    let stale = desk.fails("w2", &["st"]);
    assert!(stale.contains("stale"), "{stale}");
    desk.ok("w2", &["fs", "forget"]);

    // Forgetting unmounts the workspace and removes its repository; the workspace stays on ok,
    // and attaching it again brings back what it held.
    desk.write("w/kept.txt", "kept through a forget\n");
    desk.ok("w", &["fs", "forget"]);
    assert!(
        !desk.exists("w/notes.txt"),
        "still mounted after jj fs forget"
    );
    let out = desk.ok(".", &["fs", "status"]);
    assert!(!out.contains(&workspace), "{out}");
    let out = desk.ok(".", &["clone", &name, "w-again", "--attach", &workspace]);
    assert!(
        out.contains(&format!("Attached workspace {workspace}")),
        "{out}"
    );
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w-again/kept.txt")).unwrap(),
        "kept through a forget\n"
    );
    desk.ok("w-again", &["fs", "forget"]);
    println!("virtual_workspaces_run: scenario done");
}

/// A daemon started again checkpoints what was written before it stopped, wherever it was
/// written: its snapshot lists the written directories, whose trees it has not read yet, so it
/// fetches them from ok — from inside the snapshot's executor.
#[test]
fn checkpoint_after_restart() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("checkpoint_after_restart_run");
}

#[test]
fn checkpoint_after_restart_run() {
    if !ok_testscm::available() {
        return;
    }
    if !in_virtual_run() {
        return;
    }
    let name = format!("restart-{}", std::process::id());
    let mut alice = Person::new(ALICE, Mode::Virtual);
    alice.ok(".", &["create", &name, "repo"]);
    alice.write("repo/a/b/c/base.txt", "in the base tree\n");
    alice.ok("repo", &["commit", "-m", "a nested directory"]);
    let before = alice.latest_checkpoint("repo");
    alice.write("repo/a/b/c/during.txt", "written before the restart\n");
    let covered = alice.checkpoint_after("repo", before);

    alice.crash_daemon();
    // Only the root is looked up from here on: a/b/c's tree is read by the snapshot alone.
    alice.write("repo/after.txt", "written after the restart\n");
    alice.checkpoint_after("repo", covered);
    let status = alice.ok(".", &["fs", "status"]);
    assert!(!status.contains("checkpoint failed"), "{status}");
    let out = alice.ok("repo", &["st"]);
    assert!(
        out.contains("A a/b/c/during.txt") && out.contains("A after.txt"),
        "{out}"
    );
    println!("checkpoint_after_restart_run: scenario done");
}

/// A TCP proxy in front of the test SCM that a test cuts and restores, as a network outage would:
/// cut, it closes every connection and refuses new ones.
struct Proxy {
    addr: std::net::SocketAddr,
    up: std::sync::Arc<std::sync::atomic::AtomicBool>,
    conns: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
}

impl Proxy {
    fn new(target: &str) -> Self {
        use std::sync::atomic::Ordering;
        let target = target.trim_start_matches("http://").to_owned();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = Proxy {
            addr: listener.local_addr().unwrap(),
            up: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            conns: Default::default(),
        };
        let (up, conns) = (p.up.clone(), p.conns.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else {
                    continue;
                };
                if !up.load(Ordering::SeqCst) {
                    continue;
                }
                let Ok(server) = std::net::TcpStream::connect(&target) else {
                    continue;
                };
                let mut c = conns.lock().unwrap();
                c.push(client.try_clone().unwrap());
                c.push(server.try_clone().unwrap());
                drop(c);
                for (mut from, mut to) in [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ] {
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                        let _ = to.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        p
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn cut(&self) {
        self.up.store(false, std::sync::atomic::Ordering::SeqCst);
        for c in self.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
    }

    fn restore(&self) {
        self.up.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Offline (docs/okfs.md, "Lifecycle and recovery"): with ok unreachable, what was read
/// before reads, what was never read fails, writes work, and `jj fs status` says what is wrong;
/// once ok is back, what was written meanwhile is checkpointed and the rest reads.
#[test]
fn offline() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("offline_run");
}

#[test]
fn offline_run() {
    if !ok_testscm::available() {
        return;
    }
    if !in_virtual_run() {
        return;
    }
    let name = format!("offline-{}", std::process::id());
    // One machine writes two files into a commit and uploads it; another, whose path to ok the
    // test controls, works on it in a workspace of its own, from the CL.
    let laptop = Person::new(ALICE, Mode::Virtual);
    laptop.ok(".", &["create", &name, "repo"]);
    laptop.write("repo/read.txt", "read before the outage\n");
    laptop.write("repo/unread.txt", "never read before the outage\n");
    laptop.ok("repo", &["commit", "-m", "two files"]);
    laptop.ok("repo", &["cl"]);

    let proxy = Proxy::new(ok_testscm::endpoint());
    let mut desk = Person::new(ALICE, Mode::Virtual);
    desk.endpoint = proxy.endpoint();
    desk.ok(".", &["clone", &name, "w"]);
    desk.ok("w", &["cl", "get", "1"]);
    desk.ok("w", &["new", "cl/1@ok"]);
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w/read.txt")).unwrap(),
        "read before the outage\n"
    );
    let before = desk.latest_checkpoint("w");

    proxy.cut();
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w/read.txt")).unwrap(),
        "read before the outage\n"
    );
    let err = std::fs::read_to_string(desk.home.path().join("w/unread.txt")).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EIO), "{err}");
    desk.write("w/offline.txt", "written while ok was unreachable\n");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let out = desk.ok(".", &["fs", "status"]);
        if out.contains("checkpoint failed") || out.contains("ok unreachable") {
            assert!(out.contains("offline, ok is not reachable"), "{out}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never noticed:\n{out}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(desk.latest_checkpoint("w"), before);

    proxy.restore();
    desk.checkpoint_after("w", before);
    assert_eq!(
        std::fs::read_to_string(desk.home.path().join("w/unread.txt")).unwrap(),
        "never read before the outage\n"
    );
    let out = desk.ok("w", &["st"]);
    assert!(out.contains("A offline.txt"), "{out}");
    let out = desk.ok(".", &["fs", "status"]);
    assert!(
        !out.contains("checkpoint failed") && !out.contains("unreachable"),
        "{out}"
    );
    println!("offline_run: scenario done");
}

/// The journal (docs/okfs.md, "Tools"): `jj fs watch` reports each path written, and a
/// checkout, which file-system notifications cannot.
#[test]
fn watching() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("watching_run");
}

#[test]
fn watching_run() {
    if !ok_testscm::available() {
        return;
    }
    if !in_virtual_run() {
        return;
    }
    let name = format!("watch-{}", std::process::id());
    let alice = Person::new(ALICE, Mode::Virtual);
    alice.ok(".", &["create", &name, "repo"]);
    alice.write("repo/a.txt", "a\n");
    alice.ok("repo", &["commit", "-m", "a"]);

    let endpoint = format!("ok.endpoint={}", alice.endpoint);
    let mut watcher = alice
        .command(&[
            "--no-pager",
            "--color=never",
            "--config",
            &endpoint,
            "fs",
            "watch",
        ])
        .current_dir(alice.home.path().join("repo"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let stdout = watcher.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
            let Ok(line) = line else {
                return;
            };
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    let wait_for = |want: &str, mut poke: Box<dyn FnMut()>| {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        loop {
            poke();
            if let Ok(line) = rx.recv_timeout(Duration::from_millis(300)) {
                if line == want {
                    return;
                }
                seen.push(line);
            }
            assert!(
                Instant::now() < deadline,
                "no {want:?} from jj fs watch; saw {seen:?}"
            );
        }
    };
    // Written until the watcher, subscribing as it starts, reports it.
    let file = alice.home.path().join("repo/b.txt");
    wait_for(
        "changed b.txt",
        Box::new(move || {
            let _ = std::fs::write(&file, "b\n");
        }),
    );
    alice.ok("repo", &["new", "root()"]);
    wait_for("checkout", Box::new(|| {}));
    let _ = watcher.kill();
    let _ = watcher.wait();
    println!("watching_run: scenario done");
}

/// Redirections (docs/okfs.md, "Tools"): a path `.ok/redirections` declares is a symlink to
/// a directory on local disk, never tracked, kept across checkouts, and gone once undeclared.
#[test]
fn redirections() {
    if !ok_testscm::available() {
        return;
    }
    in_namespace("redirections_run");
}

#[test]
fn redirections_run() {
    if !ok_testscm::available() {
        return;
    }
    if !in_virtual_run() {
        return;
    }
    let name = format!("redirect-{}", std::process::id());
    let alice = Person::new(ALICE, Mode::Virtual);
    alice.ok(".", &["create", &name, "repo"]);
    alice.write(
        "repo/.ok/redirections",
        "# build outputs\nout\nweb/node_modules  # npm\n",
    );
    alice.write("repo/web/package.json", "{}\n");
    alice.ok("repo", &["commit", "-m", "declare redirections"]);
    alice.ok("repo", &["cl"]);
    // Another workspace that checks the commit out — from its CL, as work moves between
    // workspaces — has them.
    alice.ok(".", &["clone", &name, "w"]);
    alice.ok("w", &["cl", "get", "1"]);
    alice.ok("w", &["new", "cl/1@ok"]);
    let home = alice.home.path();
    for p in ["w/out", "w/web/node_modules"] {
        let link = std::fs::read_link(home.join(p)).unwrap_or_else(|e| panic!("{p}: {e}"));
        assert!(
            link.is_absolute() && !link.starts_with(home.join("w")),
            "{p} -> {}",
            link.display()
        );
        assert!(link.is_dir(), "{p}'s directory exists on local disk");
    }
    // What is written there is on local disk, and never in the working copy.
    std::fs::write(home.join("w/out/artifact.bin"), vec![1u8; 4096]).unwrap();
    assert!(
        std::fs::read_link(home.join("w/out"))
            .unwrap()
            .join("artifact.bin")
            .exists()
    );
    let tracked = alice.tracked("w");
    assert!(
        tracked
            .iter()
            .all(|t| !t.starts_with("out") && !t.contains("node_modules")),
        "{tracked:?}"
    );
    let out = alice.ok("w", &["st"]);
    assert!(
        !out.contains("out") && !out.contains("node_modules"),
        "{out}"
    );
    // Kept across a checkout; a redirection no longer declared goes.
    alice.write("w/.ok/redirections", "out\n");
    alice.ok("w", &["commit", "-m", "only out"]);
    alice.ok("w", &["new", "@-"]);
    assert!(
        std::fs::read_link(home.join("w/out"))
            .unwrap()
            .join("artifact.bin")
            .exists()
    );
    assert!(
        !home.join("w/web/node_modules").exists(),
        "an undeclared redirection goes"
    );
    assert!(home.join("w/web/package.json").exists());
    println!("redirections_run: scenario done");
}

/// `jj auth login` signs a machine in through ok identity — the browser's half, a form posted as
/// ada to a development identity — and `jj auth logout` signs it out again. The login once ran on
/// jj's own executor, which is no Tokio runtime, and panicked before it showed a code.
#[test]
fn auth_login_and_logout() {
    if !ok_testscm::available() {
        return;
    }
    let endpoint = ok_testscm::identity_endpoint();
    let home = tempfile::tempdir().unwrap();
    let jj = |args: &[&str]| {
        let mut cmd = Command::new(jj_bin());
        cmd.args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", home.path())
            .env("OK_CONFIG_DIR", home.path().join("ok"))
            .env("JJ_CONFIG", home.path().join("jj.toml"));
        cmd
    };
    let mut login = jj(&[
        "auth",
        "login",
        endpoint,
        "--no-browser",
        "--label",
        "ada's laptop",
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("run jj auth login");
    // The code, from the link it shows; the rest of what it says is read as it goes.
    let (lines, said) = std::sync::mpsc::channel::<String>();
    let stderr = login.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        use std::io::BufRead as _;
        for line in std::io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let mut shown = String::new();
    let code = loop {
        let line = said
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|e| panic!("jj auth login showed no code ({e}):\n{shown}"));
        shown.push_str(&line);
        shown.push('\n');
        if let Some((_, rest)) = line.split_once("/id/device?code=") {
            break rest.split_whitespace().next().unwrap().to_owned();
        }
    };
    let resp = ok_testscm::post_form_as(
        endpoint,
        "/id/device",
        "ada.human.example.com",
        &[("code", &code), ("decision", "approve")],
    );
    assert!(resp.contains("Approved"), "approving the code:\n{resp}");
    let out = login.wait_with_output().expect("wait for jj auth login");
    reader.join().unwrap();
    for line in said.try_iter() {
        shown.push_str(&line);
        shown.push('\n');
    }
    assert!(
        out.status.success()
            && shown.contains(&format!("Signed in to {endpoint} as ada.human.example.com")),
        "jj auth login: {}\n{shown}",
        out.status
    );

    let status = jj(&["auth", "status"]).output().unwrap();
    assert!(
        status.status.success()
            && String::from_utf8_lossy(&status.stdout).contains("as ada.human.example.com"),
        "jj auth status: {status:?}"
    );
    let logout = jj(&["auth", "logout"]).output().unwrap();
    assert!(
        logout.status.success() && String::from_utf8_lossy(&logout.stderr).contains("Signed out"),
        "jj auth logout: {logout:?}"
    );
    let after = jj(&["auth", "status"]).output().unwrap();
    assert!(
        !after.status.success(),
        "still signed in after logout: {after:?}"
    );
}
