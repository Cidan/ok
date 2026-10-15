//! A virtual workspace where the machine cannot mount one (docs/okfs.md): `jj` runs where
//! `/dev/fuse` does not exist (okfs_fuse::testns::without_fuse), so `jj create` and `jj clone`
//! without `--real` are refused — before ok is asked, naming why and `--real`. No SCM is needed:
//! the endpoint is never dialled.
//!
//! Linux only: it takes `/dev/fuse` away in a namespace of its own.

#![cfg(target_os = "linux")]

use std::process::Command;

#[test]
fn a_virtual_create_or_clone_is_refused_without_fuse() {
    let home = tempfile::tempdir().unwrap();
    for args in [["create", "nofuse", "repo"], ["clone", "nofuse", "repo"]] {
        let mut cmd = Command::new(
            std::env::var("CARGO_BIN_EXE_jj").expect("CARGO_BIN_EXE_jj: run under cargo test"),
        );
        okfs_fuse::testns::without_fuse(&mut cmd);
        let out = cmd
            .args([
                "--no-pager",
                "--color=never",
                "--config",
                "ok.endpoint=http://127.0.0.1:9",
            ])
            .args(args)
            .current_dir(home.path())
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("HOME", home.path())
            .env("OK_DEV_PRINCIPAL", "alice.human.example.com")
            .env("JJ_USER", "alice")
            .env("JJ_EMAIL", "alice@example.com")
            .output()
            .expect("run jj");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.status.success(), "jj {args:?}: {text}");
        assert!(
            text.contains(
                "this machine cannot mount a virtual workspace: /dev/fuse cannot be opened"
            ),
            "jj {args:?}: {text}"
        );
        assert!(text.contains("--real"), "jj {args:?}: {text}");
        assert!(
            !text.contains("Created repository"),
            "jj {args:?} asked ok first: {text}"
        );
        assert!(!home.path().join("repo").exists());
    }
}
