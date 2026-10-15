//! Mounting and unmounting: directly with mount(2) when the process holds `CAP_SYS_ADMIN` over its
//! mount namespace (root in a user namespace of its own, as okfs's kernel tests are), and
//! otherwise through `fusermount3`, the setuid helper FUSE ships, which mounts for an unprivileged
//! user and hands back the connection's descriptor over a socket.

use std::ffi::CString;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Command;
use std::process::Stdio;

use crate::MountOptions;

fn cstring(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(|_| io::Error::other("a path holds a NUL byte"))
}

/// Whether this process may mount directly: it holds CAP_SYS_ADMIN in its user namespace.
fn may_mount() -> bool {
    #[repr(C)]
    struct Header {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct Data {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    let mut h = Header {
        version: 0x2008_0522, // _LINUX_CAPABILITY_VERSION_3
        pid: 0,
    };
    let mut d = [Data::default(); 2];
    let rc = unsafe { libc::syscall(libc::SYS_capget, &mut h, d.as_mut_ptr()) };
    const CAP_SYS_ADMIN: u32 = 21;
    rc == 0 && d[0].effective & (1 << CAP_SYS_ADMIN) != 0
}

/// Whether this process can mount a FUSE file system here, and if not, why: `/dev/fuse` must open,
/// and either the process may mount or `fusermount3` must be on `PATH`.
pub fn can_mount() -> Result<(), String> {
    let fd = unsafe { libc::open(c"/dev/fuse".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(format!(
            "/dev/fuse cannot be opened: {}",
            io::Error::last_os_error()
        ));
    }
    unsafe { libc::close(fd) };
    if may_mount() {
        return Ok(());
    }
    let found = std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|d| d.join("fusermount3").is_file()));
    if found {
        Ok(())
    } else {
        Err("fusermount3 (FUSE 3's mount helper) is not installed".to_owned())
    }
}

/// Mounts a FUSE file system at `mountpoint` and returns the connection's device descriptor, on
/// which the kernel's INIT is waiting.
pub fn mount(mountpoint: &Path, opts: &MountOptions) -> io::Result<OwnedFd> {
    if may_mount() {
        mount_direct(mountpoint, opts)
    } else {
        mount_fusermount(mountpoint, opts)
    }
}

fn mount_direct(mountpoint: &Path, opts: &MountOptions) -> io::Result<OwnedFd> {
    let raw = unsafe { libc::open(c"/dev/fuse".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut data = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        fd.as_raw_fd(),
        crate::abi::S_IFDIR,
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() }
    );
    if opts.default_permissions {
        data.push_str(",default_permissions");
    }
    let source = CString::new(opts.fsname.as_str()).map_err(io::Error::other)?;
    let fstype = CString::new(format!("fuse.{}", opts.subtype)).map_err(io::Error::other)?;
    let target = cstring(mountpoint)?;
    let data = CString::new(data).map_err(io::Error::other)?;
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            libc::MS_NOSUID | libc::MS_NODEV | if opts.read_only { libc::MS_RDONLY } else { 0 },
            data.as_ptr().cast(),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

fn mount_fusermount(mountpoint: &Path, opts: &MountOptions) -> io::Result<OwnedFd> {
    let mut fds = [0i32; 2];
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let ours = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let theirs = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    let mut options = format!("fsname={},subtype={}", opts.fsname, opts.subtype);
    if opts.default_permissions {
        options.push_str(",default_permissions");
    }
    if opts.read_only {
        options.push_str(",ro");
    }
    let their_fd = theirs.as_raw_fd();
    let mut cmd = Command::new("fusermount3");
    cmd.arg("-o")
        .arg(&options)
        .arg("--")
        .arg(mountpoint)
        .env("_FUSE_COMMFD", their_fd.to_string())
        .stdin(Stdio::null());
    unsafe {
        cmd.pre_exec(move || {
            // The socket's end must survive exec in fusermount3.
            let flags = libc::fcntl(their_fd, libc::F_GETFD);
            if flags < 0 || libc::fcntl(their_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let output = cmd
        .output()
        .map_err(|e| io::Error::new(e.kind(), format!("running fusermount3: {e}")))?;
    drop(theirs);
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "fusermount3 could not mount {}: {}",
            mountpoint.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    receive_fd(&ours)
}

/// Receives the one descriptor fusermount3 sends (SCM_RIGHTS).
fn receive_fd(sock: &OwnedFd) -> io::Result<OwnedFd> {
    let mut byte = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<i32>() as u32) } as usize;
    let mut control = vec![0u8; space];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = space as _;
    loop {
        let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if n >= 0 {
            break;
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            return Err(err);
        }
    }
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null()
        || unsafe {
            (*cmsg).cmsg_level != libc::SOL_SOCKET || (*cmsg).cmsg_type != libc::SCM_RIGHTS
        }
    {
        return Err(io::Error::other("fusermount3 sent no descriptor"));
    }
    let fd = unsafe { std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<i32>()) };
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Unmounts `mountpoint`, lazily (detached: open files keep working until closed): directly when
/// the process may, through `fusermount3 -u -z` otherwise.
pub fn unmount(mountpoint: &Path) -> io::Result<()> {
    if may_mount() {
        let target = cstring(mountpoint)?;
        if unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) } != 0 {
            return Err(io::Error::last_os_error());
        }
        return Ok(());
    }
    let output = Command::new("fusermount3")
        .args(["-u", "-z", "--"])
        .arg(mountpoint)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| io::Error::new(e.kind(), format!("running fusermount3: {e}")))?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "fusermount3 could not unmount {}: {}",
            mountpoint.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}
