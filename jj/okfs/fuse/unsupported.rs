//! Mounting on a platform whose FUSE is not Linux's: it cannot. Virtual workspaces need Linux's
//! `/dev/fuse` (docs/okfs.md); elsewhere, every workspace is a real checkout.

use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;

use crate::MountOptions;

fn unsupported() -> String {
    format!(
        "virtual workspaces need Linux's FUSE, and this is {}",
        std::env::consts::OS
    )
}

/// Why this process cannot mount a FUSE file system: always, here.
pub fn can_mount() -> Result<(), String> {
    Err(unsupported())
}

/// Refuses: this platform mounts no FUSE file system.
pub fn mount(_: &Path, _: &MountOptions) -> io::Result<OwnedFd> {
    Err(io::Error::new(io::ErrorKind::Unsupported, unsupported()))
}

/// Refuses: nothing was mounted.
pub fn unmount(_: &Path) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, unsupported()))
}
