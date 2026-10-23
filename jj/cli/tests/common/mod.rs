//! What cli.rs and cli_virtual.rs share: two people at their workstations, each with their own
//! home and, in a virtual run, their own okfs daemon; and the scenarios, which run in real
//! checkouts (cli.rs) and in virtual workspaces (cli_virtual.rs, in namespaces of its own).
#![allow(dead_code)]

use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use ok_store::pb;

/// The host's user namespace as /proc/self/ns/user names it (the kernel fixes its inode).
const HOST_USER_NAMESPACE: &str = "user:[4026531837]";

/// Whether this process runs in a user namespace of its own, where a virtual scenario mounts.
pub fn in_own_user_namespace() -> bool {
    std::fs::read_link("/proc/self/ns/user").is_ok_and(|ns| ns.as_os_str() != HOST_USER_NAMESPACE)
}

pub const ALICE: &str = "alice.human.example.com";
pub const BOB: &str = "bob.human.example.com";

/// The jj binary this workspace builds.
pub fn jj_bin() -> String {
    std::env::var("CARGO_BIN_EXE_jj").expect("CARGO_BIN_EXE_jj: run under cargo test")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Real,
    Virtual,
}

/// One person at a workstation: their own home (so their own jj configuration) and the
/// principal a loopback SCM knows them by; in a virtual run, their own okfs daemon.
pub struct Person {
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
                in_own_user_namespace(),
                "a virtual scenario runs in cli_virtual, which enters namespaces of its own first"
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

    /// Runs `jj args…` in `dir` as a personal token alone: no development principal names anyone,
    /// and `OK_TOKEN` holds `secret`. Clones are real.
    pub fn jj_token(&self, secret: &str, dir: &str, args: &[&str]) -> Output {
        let cwd = self.home.path().join(dir);
        std::fs::create_dir_all(&cwd).unwrap();
        let endpoint = format!("ok.endpoint={}", self.endpoint);
        let mut all = vec!["--no-pager", "--color=never", "--config", &endpoint];
        all.extend_from_slice(args);
        if matches!(
            args.first(),
            Some(&"clone") | Some(&"create") | Some(&"fork")
        ) {
            all.push("--real");
        }
        self.command(&all)
            .env_remove("OK_DEV_PRINCIPAL")
            .env("OK_TOKEN", secret)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .output()
            .expect("run jj")
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
        if real
            && matches!(
                args.first(),
                Some(&"clone") | Some(&"create") | Some(&"fork")
            )
        {
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

pub async fn create_upload_stack(mode: Mode) {
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
    let alice_id = ok_testscm::id(ALICE).await;
    control
        .transition(pb::TransitionRequest {
            repository_id: repo.id,
            number: 1,
            kind: pb::transition_request::Kind::RequestReview as i32,
            actor_id: alice_id.clone(),
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
            actor_id: String::new(),
        }],
        actor_id: alice_id.clone(),
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

/// Forks: bob forks alice's repository with `jj fork`, its clone holding what the upstream
/// held; once the upstream lands more, `jj sync --upstream` brings it to bob's fork and clone. A
/// repository that is no fork refuses `--upstream`.
pub async fn forks() {
    let name = format!("forked-{}", std::process::id());
    let alice = Person::new(ALICE, Mode::Real);
    let bob = Person::new(BOB, Mode::Real);
    alice.ok(".", &["create", &name, "repo"]);
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
    let alice_id = ok_testscm::id(ALICE).await;
    let mut head = repo.head.clone();
    // alice uploads a change as CL number, and the landing queue lands it.
    let mut land = async |number: i64, file: &str| {
        alice.ok("repo", &["new", "trunk()"]);
        alice.write(&format!("repo/{file}"), "upstream\n");
        alice.ok("repo", &["describe", "-m", &format!("add {file}")]);
        alice.ok("repo", &["cl", "--ready"]);
        control
            .transition(pb::TransitionRequest {
                repository_id: repo.id,
                number,
                kind: pb::transition_request::Kind::RequestReview as i32,
                actor_id: alice_id.clone(),
                reason: String::new(),
            })
            .await
            .ok();
        let landed = queue
            .land(pb::LandRequest {
                repository_id: repo.id,
                items: vec![pb::land_request::Item {
                    number,
                    patchset: 1,
                    candidate: Vec::new(),
                    actor_id: String::new(),
                }],
                actor_id: alice_id.clone(),
                expected_head: head.clone(),
                dry_run: false,
            })
            .await
            .unwrap()
            .into_inner();
        head = landed.head;
        alice.ok("repo", &["sync"]);
    };
    land(1, "a.txt").await;

    let fork = format!("bob/{name}");
    let out = bob.ok(".", &["fork", &name, &fork, "fork"]);
    assert!(
        out.contains(&format!("Forked {name} on ok as {fork}")),
        "{out}"
    );
    assert!(
        bob.exists("fork/a.txt"),
        "bob's fork holds the upstream's file"
    );
    land(2, "b.txt").await;
    let out = bob.ok("fork", &["sync", "--upstream"]);
    assert!(
        out.contains("The fork's main moved to its upstream's"),
        "{out}"
    );
    assert!(
        bob.exists("fork/b.txt"),
        "bob's fork has the upstream's later file after syncing"
    );
    let out = alice.fails("repo", &["sync", "--upstream"]);
    assert!(out.contains("no fork"), "{out}");
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn big_files(mode: Mode) {
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
    // Tracked anyway — the limit raised — it is refused as it is snapshotted, naming the file,
    // and nothing of it is staged: once it is gone, the workspace publishes again.
    let raised = ["--config", "snapshot.max-new-file-size=60MiB"];
    let out = alice.fails("repo", &[&raised[..], &["cl"]].concat());
    assert!(
        out.contains("big.bin is larger than 50 MiB, the largest file ok holds"),
        "{out}"
    );
    std::fs::remove_file(&big).unwrap();
    std::fs::write(alice.home.path().join("repo/small.bin"), b"small\n").unwrap();
    alice.ok("repo", &["describe", "-m", "small after all"]);
    let out = alice.ok("repo", &["cl"]);
    assert!(out.contains("CL 2 created"), "{out}");
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

/// A personal token in place of a credential: jj clones and reads the repository it names, as a
/// real checkout, and is told there is none of a repository it does not name.
pub async fn personal_tokens() {
    let alice = Person::new(ALICE, Mode::Real);
    let name = format!("token-{}", std::process::id());
    let other = format!("token-other-{}", std::process::id());
    alice.ok(".", &["create", &name, "repo"]);
    alice.ok(".", &["create", &other, "other"]);
    let secret = ok_testscm::create_token(ALICE, &[&name], "read").await;
    let out = alice.jj_token(&secret, ".", &["clone", &name, "viatoken"]);
    assert!(
        out.status.success(),
        "a clone through the token: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = alice.jj_token(
        &secret,
        "viatoken",
        &["log", "--no-graph", "-r", "@", "-T", "change_id"],
    );
    assert!(
        log.status.success(),
        "{}",
        String::from_utf8_lossy(&log.stderr)
    );
    let out = alice.jj_token(&secret, ".", &["clone", &other, "nottoken"]);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && said.contains(&format!("ok has no repository named \"{other}\"")),
        "a clone of a repository the token does not name: {said}"
    );
}

pub fn ignores(mode: Mode) {
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

    // In a conflicted commit — a merge of two changes that edited a.txt apart — the ignore files
    // apply as in any other: its tree has a term per side, and each side holds them alike.
    let change = |rev: &str| {
        let out = alice.jj("repo", &["log", "--no-graph", "-r", rev, "-T", "change_id"]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    alice.ok("repo", &["commit", "-m", "base"]);
    let base = change("@-");
    alice.write("repo/a.txt", "left\n");
    alice.ok("repo", &["commit", "-m", "left"]);
    let left = change("@-");
    alice.ok("repo", &["new", &base]);
    alice.write("repo/a.txt", "right\n");
    alice.ok("repo", &["commit", "-m", "right"]);
    let right = change("@-");
    alice.ok("repo", &["new", &left, &right]);
    let conflicts = alice.ok("repo", &["resolve", "--list"]);
    assert!(conflicts.contains("a.txt"), "{conflicts}");
    for path in [
        "repo/merged.log",
        "repo/merged.tmp",
        "repo/build/again.bin",
        "repo/merged.txt",
    ] {
        alice.write(path, "x\n");
    }
    let tracked = alice.tracked("repo");
    assert!(tracked.contains(&"merged.txt".to_owned()), "{tracked:?}");
    for ignored in ["merged.log", "merged.tmp", "build/again.bin"] {
        assert!(
            !tracked.contains(&ignored.to_owned()),
            "{ignored} in {tracked:?}"
        );
    }

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

    /// Waits until the workspace at `dir` has a checkpoint newer than `after` (0: none): its latest
    /// is another than `after`, as checkpoint IDs are random and the latest only ever moves on.
    fn checkpoint_after(&self, dir: &str, after: i64) -> i64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let n = self.latest_checkpoint(dir);
            if n != 0 && n != after {
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

/// A TCP proxy in front of the test SCM that a test cuts and restores, as a network outage would:
/// cut, it closes every connection and refuses new ones.
pub struct Proxy {
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

/// What only a virtual workspace does (docs/okfs.md): the daemon keeps every write in a
/// checkpoint without a jj command; another machine takes the workspace over at its latest
/// checkpoint, after which the first machine's mount refuses writes; a daemon that crashes is
/// started again with its mounts and written files intact; checkpoints are listed and restored;
/// and `jj fs forget` unmounts the workspace and drops it.
pub fn virtual_workspaces() {
    if !ok_testscm::available() {
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
    // Each line: the checkpoint's ID, when it was made (YYYY-MM-DD HH:MM:SS), and the machine.
    let listed: Vec<(i64, String)> = out
        .lines()
        .filter_map(|l| {
            let mut f = l.split_whitespace();
            let id = f.next()?.parse().ok()?;
            Some((id, format!("{} {}", f.next()?, f.next()?)))
        })
        .collect();
    let ids: Vec<i64> = listed.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&first) && ids.contains(&after_crash), "{out}");
    assert!(
        listed.windows(2).all(|w| w[0].1 >= w[1].1),
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
pub fn checkpoint_after_restart() {
    if !ok_testscm::available() {
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

/// Offline (docs/okfs.md, "Lifecycle and recovery"): with ok unreachable, what was read
/// before reads, what was never read fails, writes work, and `jj fs status` says what is wrong;
/// once ok is back, what was written meanwhile is checkpointed and the rest reads.
pub fn offline() {
    if !ok_testscm::available() {
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
pub fn watching() {
    if !ok_testscm::available() {
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
pub fn redirections() {
    if !ok_testscm::available() {
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
    // The paths it lists, not its change IDs, which may spell "out".
    let listed: Vec<&str> = out
        .lines()
        .filter_map(|l| l.split_once(' '))
        .filter(|(status, _)| status.len() == 1)
        .map(|(_, path)| path)
        .collect();
    assert!(
        listed
            .iter()
            .all(|p| !p.starts_with("out") && !p.contains("node_modules")),
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

pub const ADA: &str = "ada.human.example.com";

impl Person {
    /// A file in this person's home, as bytes: an error where it cannot be read.
    fn read(&self, path: &str) -> std::io::Result<Vec<u8>> {
        std::fs::read(self.home.path().join(path))
    }
}

/// Lands a CL through the protocol, as ok control and the landing queue do, on main's head.
async fn land_cl(name: &str, number: i64, actor: &str) {
    let repo = ok_testscm::repositories(actor)
        .await
        .get_repository(pb::GetRepositoryRequest {
            key: Some(pb::get_repository_request::Key::Name(name.to_owned())),
        })
        .await
        .unwrap()
        .into_inner();
    let actor_id = ok_testscm::id(actor).await;
    ok_testscm::review(ok_testscm::CONTROL)
        .await
        .transition(pb::TransitionRequest {
            repository_id: repo.id,
            number,
            kind: pb::transition_request::Kind::RequestReview as i32,
            actor_id: actor_id.clone(),
            reason: String::new(),
        })
        .await
        .unwrap();
    ok_testscm::landing(ok_testscm::AUTOMATION)
        .await
        .land(pb::LandRequest {
            repository_id: repo.id,
            items: vec![pb::land_request::Item {
                number,
                patchset: 1,
                candidate: Vec::new(),
                actor_id: String::new(),
            }],
            actor_id,
            expected_head: repo.head,
            dry_run: false,
        })
        .await
        .unwrap();
}

/// Secrets (docs/secrets.md): alice makes the repository keep them, under //secret, which
/// secret-readers (ada, alice) read and secret-writers (alice) write. In a real checkout a value
/// written to a secret's file is refused at the snapshot, and `jj secret write` keeps it, its
/// pointer in the working copy; in a virtual workspace the file is written, and the snapshot keeps
/// it. Either way the CL uploads, the SCM asking ok secrets, and lands. A reader reads the value
/// once per unseal — `jj secret cat`, or the file in a virtual workspace — and someone who is no
/// reader never; a pointer copied to another path is refused at upload; and a real checkout's
/// file holds the pointer.
pub async fn secrets(mode: Mode) {
    let name = format!("secrets-{}-{mode:?}", std::process::id()).to_lowercase();
    let alice = Person::new(ALICE, mode);
    let ada = Person::new(ADA, mode);
    let bob = Person::new(BOB, mode);

    alice.ok(".", &["create", &name, "repo"]);
    alice.write("repo/.ok/repository.yaml", "build: none\nsecrets: {}\n");
    alice.write(
        "repo/secret/acl.yaml",
        &format!(
            "readers: [{}]\nwriters: [{}]\n",
            ok_testscm::SECRET_READERS,
            ok_testscm::SECRET_WRITERS
        ),
    );
    alice.ok("repo", &["describe", "-m", "keep secrets"]);
    alice.ok("repo", &["cl"]);
    land_cl(&name, 1, ALICE).await;
    alice.ok("repo", &["sync"]);

    // alice writes the token.
    alice.ok("repo", &["new", "trunk()", "-m", "add the token"]);
    match mode {
        Mode::Real => {
            // A value in a secret's file never leaves the machine.
            alice.write("repo/secret/token", "hunter2");
            let out = alice.fails("repo", &["status"]);
            assert!(out.contains("holds a value, not a pointer"), "{out}");
            std::fs::remove_file(alice.home.path().join("repo/secret/token")).unwrap();
            alice.write("value", "hunter2");
            let value = alice.home.path().join("value").display().to_string();
            let out = alice.ok(
                "repo",
                &["secret", "write", "//secret/token", "--file", &value],
            );
            assert!(
                out.contains("Kept a new version of //secret/token"),
                "{out}"
            );
            let pointer = alice.read("repo/secret/token").unwrap();
            assert!(
                ok_secret::parse_pointer(&pointer).is_some(),
                "the file holds the pointer"
            );
        }
        Mode::Virtual => {
            alice.write("repo/secret/token", "hunter2");
            assert_eq!(alice.read("repo/secret/token").unwrap(), b"hunter2");
        }
    }
    let out = alice.ok("repo", &["cl"]);
    assert!(out.contains("CL 2 created"), "{out}");
    land_cl(&name, 2, ALICE).await;

    // ada, a reader, reads the value once per unseal.
    ada.ok(".", &["clone", &name, "repo"]);
    let out = ada.ok("repo", &["secret", "cat", "//secret/token", "--no-browser"]);
    assert_eq!(out.trim_end_matches('\n'), "hunter2");
    assert!(ada.ok("repo", &["secret", "status"]).contains("No unseals"));
    match mode {
        Mode::Real => {
            let held = ada.read("repo/secret/token").unwrap();
            assert!(
                ok_secret::parse_pointer(&held).is_some(),
                "a real checkout holds the pointer"
            );
        }
        Mode::Virtual => {
            let e = ada.read("repo/secret/token").unwrap_err();
            assert_eq!(
                e.raw_os_error(),
                Some(libc::EACCES),
                "a read without an unseal"
            );
            let out = ada.ok(
                "repo",
                &["secret", "unseal", "secret/token", "--no-browser"],
            );
            assert!(out.contains("Unsealed //secret/token"), "{out}");
            assert!(
                ada.ok("repo", &["secret", "status"])
                    .contains("//secret/token  expires in")
            );
            assert_eq!(ada.read("repo/secret/token").unwrap(), b"hunter2");
            let e = ada.read("repo/secret/token").unwrap_err();
            assert_eq!(
                e.raw_os_error(),
                Some(libc::EACCES),
                "an unseal serves one read"
            );
        }
    }
    // ada reads, and does not write.
    ada.write("value", "not hers");
    let value = ada.home.path().join("value").display().to_string();
    let out = ada.fails(
        "repo",
        &["secret", "write", "//secret/token", "--file", &value],
    );
    assert!(out.contains("you may not write secret/token"), "{out}");

    // bob is neither.
    bob.ok(".", &["clone", &name, "repo"]);
    let out = bob.fails("repo", &["secret", "cat", "//secret/token", "--no-browser"]);
    assert!(out.contains("you may not unseal secret/token"), "{out}");
    let out = bob.fails(
        "repo",
        &["secret", "unseal", "//secret/token", "--no-browser"],
    );
    assert!(out.contains("secret-readers.acl.example.com"), "{out}");

    // A pointer copied to another path names nothing there.
    alice.ok("repo", &["sync"]);
    alice.ok("repo", &["new", "trunk()", "-m", "copy the token"]);
    let pointer = match mode {
        Mode::Real => alice.read("repo/secret/token").unwrap(),
        Mode::Virtual => {
            let out = alice.ok("repo", &["file", "show", "-r", "trunk()", "secret/token"]);
            out.into_bytes()
        }
    };
    let pointer = String::from_utf8(pointer).unwrap();
    alice.write("repo/secret/copied", &pointer);
    let out = alice.fails("repo", &["cl"]);
    assert!(
        out.contains("names a version ok secrets does not keep"),
        "{out}"
    );
}
