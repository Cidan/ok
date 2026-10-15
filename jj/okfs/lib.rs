//! okfs (docs/okfs.md): virtual workspaces for ok's jj — a FUSE file system the jj binary
//! serves, presenting a commit's tree at once and reading each file from ok on first access,
//! with what is written kept in an overlay and, as checkpoints, in ok.

pub mod client;
pub mod control;
pub mod daemon;
pub mod fs;
pub mod model;
pub mod persist;
pub mod snapshot;
pub mod systemd;
pub mod workingcopy;

#[cfg(test)]
mod checkpoint_tests;
#[cfg(all(test, target_os = "linux"))]
mod fs_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod testfix;
