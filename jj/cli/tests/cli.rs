//! ok's jj on the command line, end to end (docs/jj.md): `jj create`, `jj
//! clone`, `jj cl`, `jj cl get`, and `jj sync`, run as the built binary against an ok scm
//! the test runs in its sandbox (jj/testscm), as two people. ok control lands
//! through the protocol.
//!
//! Every scenario that a real checkout can run runs here in real checkouts (`--real`), and again
//! in virtual workspaces in cli_virtual.rs (docs/okfs.md), so the two being the same is a test.
//!
//! Linux only: its SCM runs where ok's server does.

#![cfg(target_os = "linux")]

mod common;

use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread")]
async fn create_upload_stack_on_someone_else_and_sync() {
    if !ok_testscm::available() {
        return;
    }
    create_upload_stack(Mode::Real).await;
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

/// A personal token signs jj in where there is no credential — a CI job: with `OK_TOKEN`, jj
/// clones and reads the repository the token names, and reaches no other.
#[tokio::test(flavor = "multi_thread")]
async fn personal_token() {
    if !ok_testscm::available() {
        return;
    }
    personal_tokens().await;
}

/// Secrets: kept by reference, read once per unseal (docs/secrets.md).
#[tokio::test(flavor = "multi_thread")]
async fn secrets_in_real_checkouts() {
    if !ok_testscm::available() {
        return;
    }
    secrets(Mode::Real).await;
}

/// Forks: `jj fork`, and `jj sync --upstream`.
#[tokio::test(flavor = "multi_thread")]
async fn fork() {
    if !ok_testscm::available() {
        return;
    }
    forks().await;
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
