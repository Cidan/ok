//! Running a process in a user and mount namespace of its own, where it is root and may mount
//! FUSE file systems — how okfs's kernel tests mount without privilege (docs/building.md,
//! "Devices"), and how a process joins another's namespaces to see what it mounted.
//!
//! A multithreaded process cannot enter a new user namespace, so the namespaces are entered in
//! the forked child, before it executes its program.

use std::io;
use std::os::unix::process::CommandExt as _;
use std::process::Command;

/// Writes `data` to `path` with raw system calls (safe between fork and exec).
fn write_file(path: &std::ffi::CStr, data: &[u8]) -> io::Result<()> {
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = unsafe { libc::write(fd, data.as_ptr().cast(), data.len()) };
    let err = io::Error::last_os_error();
    unsafe { libc::close(fd) };
    if n != data.len() as isize {
        return Err(err);
    }
    Ok(())
}

/// Makes `cmd` run in a new user namespace, as root mapped to the caller, and a new mount
/// namespace it owns, with every mount private.
pub fn unshare(cmd: &mut Command) -> &mut Command {
    let uid_map = format!("0 {} 1", unsafe { libc::geteuid() }).into_bytes();
    let gid_map = format!("0 {} 1", unsafe { libc::getegid() }).into_bytes();
    unsafe {
        cmd.pre_exec(move || {
            if libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNS) != 0 {
                return Err(io::Error::last_os_error());
            }
            write_file(c"/proc/self/setgroups", b"deny")?;
            write_file(c"/proc/self/uid_map", &uid_map)?;
            write_file(c"/proc/self/gid_map", &gid_map)?;
            if libc::mount(
                std::ptr::null(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        })
    }
}

/// Makes `cmd` run where `/dev/fuse` does not exist: in namespaces of its own ([`unshare`]), with
/// an empty file system over `/dev` — a machine that cannot mount a virtual workspace.
pub fn without_fuse(cmd: &mut Command) -> &mut Command {
    unshare(cmd);
    unsafe {
        cmd.pre_exec(|| {
            if libc::mount(
                c"none".as_ptr(),
                c"/dev".as_ptr(),
                c"tmpfs".as_ptr(),
                0,
                std::ptr::null(),
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        })
    }
}

/// Makes `cmd` run in the user and mount namespaces of process `pid` (one [`unshare`] started),
/// so it sees what that process mounted.
pub fn join(cmd: &mut Command, pid: u32) -> &mut Command {
    let user = std::ffi::CString::new(format!("/proc/{pid}/ns/user")).unwrap();
    let mnt = std::ffi::CString::new(format!("/proc/{pid}/ns/mnt")).unwrap();
    unsafe {
        cmd.pre_exec(move || {
            for (path, kind) in [(&user, libc::CLONE_NEWUSER), (&mnt, libc::CLONE_NEWNS)] {
                let fd = libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC);
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                let rc = libc::setns(fd, kind);
                let err = io::Error::last_os_error();
                libc::close(fd);
                if rc != 0 {
                    return Err(err);
                }
            }
            Ok(())
        })
    }
}
