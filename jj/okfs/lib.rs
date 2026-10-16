//! okfs (docs/okfs.md): virtual workspaces for ok's jj — a file system the jj binary serves, over
//! FUSE on Linux and NFS on macOS, presenting a commit's tree at once and reading each file from ok
//! on first access, with what is written kept in an overlay and, as checkpoints, in ok.

pub mod client;
pub mod control;
pub mod daemon;
pub mod fs;
pub mod model;
pub mod nfs;
#[cfg(target_os = "macos")]
pub mod nfsd;
pub mod persist;
pub mod snapshot;
pub mod systemd;
pub mod workingcopy;

/// What macOS writes beside a virtual workspace's files over NFS — AppleDouble files holding their
/// extended attributes (`._name`), the Finder's `.DS_Store`, and the names NFS gives a file
/// removed while still open (`.nfs.*`) — ignored under `base`'s rules: never tracked or
/// checkpointed, they stay where they are, as build outputs do. Elsewhere, `base`.
pub fn platform_ignores(
    base: &std::sync::Arc<jj_lib::gitignore::GitIgnoreFile>,
) -> std::sync::Arc<jj_lib::gitignore::GitIgnoreFile> {
    if !cfg!(target_os = "macos") {
        return base.clone();
    }
    base.chain(
        jj_lib::repo_path::RepoPath::root(),
        std::path::Path::new(""),
        b"._*\n.DS_Store\n.nfs.*\n",
    )
    .unwrap_or_else(|_| base.clone())
}

/// Why this machine cannot mount a virtual workspace, if it cannot: over FUSE on Linux, over NFS
/// on macOS.
pub fn can_mount() -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        nfsd::can_mount()
    }
    #[cfg(not(target_os = "macos"))]
    {
        okfs_fuse::can_mount()
    }
}

#[cfg(test)]
mod checkpoint_tests;
#[cfg(all(test, target_os = "linux"))]
mod fs_tests;
#[cfg(test)]
mod model_tests;
#[cfg(all(test, target_os = "macos"))]
mod nfs_kernel_tests;
#[cfg(test)]
mod nfs_tests;
#[cfg(test)]
mod testfix;
