//! The scenarios of cli.rs again in virtual workspaces (docs/okfs.md), and what only a virtual
//! workspace does. The binary enters a user and mount namespace of its own first thing in its
//! `main` (the target binds `/dev/fuse`), where each person runs their own okfs daemon and every
//! `jj`, and the test's own file reads and writes, see its mounts — so it runs on a harness of its
//! own (okfs_fuse::testns::run).
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

#[cfg(target_os = "linux")]
mod common;

#[cfg(target_os = "linux")]
use common::*;

/// The scenarios of cli.rs that run in both kinds of workspace, async ones on a runtime of their own.
#[cfg(target_os = "linux")]
fn create_upload_stack_on_someone_else_and_sync() {
    if !ok_testscm::available() {
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(create_upload_stack(Mode::Virtual));
}

#[cfg(target_os = "linux")]
fn secrets_in_virtual_workspaces() {
    if !ok_testscm::available() {
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(secrets(Mode::Virtual));
}

#[cfg(target_os = "linux")]
fn files_up_to_fifty_mebibytes() {
    if !ok_testscm::available() {
        return;
    }
    big_files(Mode::Virtual);
}

#[cfg(target_os = "linux")]
fn ignore_files() {
    if !ok_testscm::available() {
        return;
    }
    ignores(Mode::Virtual);
}

#[cfg(target_os = "linux")]
fn main() {
    okfs_fuse::testns::enter().expect("enter a user and mount namespace of the test's own");
    okfs_fuse::testns::run(&[
        (
            "create_upload_stack_on_someone_else_and_sync",
            create_upload_stack_on_someone_else_and_sync,
        ),
        ("files_up_to_fifty_mebibytes", files_up_to_fifty_mebibytes),
        (
            "secrets_in_virtual_workspaces",
            secrets_in_virtual_workspaces,
        ),
        ("ignore_files", ignore_files),
        ("virtual_workspaces", virtual_workspaces),
        ("checkpoint_after_restart", checkpoint_after_restart),
        ("offline", offline),
        ("watching", watching),
        ("redirections", redirections),
    ]);
}

#[cfg(not(target_os = "linux"))]
fn main() {}
