//! systemd's service protocol, as the daemon uses it: readiness, and its file descriptor store,
//! which keeps each mount's `/dev/fuse` descriptor across a restart (docs/okfs.md,
//! "Lifecycle and recovery").

use std::io;
use std::os::fd::AsRawFd as _;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;

/// Sends a notification to systemd, with descriptors; nothing outside systemd.
pub fn notify(state: &str, fds: &[&OwnedFd]) -> io::Result<()> {
    let Some(sock) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    let fd = OwnedFd::from(std::os::unix::net::UnixDatagram::unbound()?);
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    let path = sock.as_bytes();
    if path.len() >= addr.sun_path.len() {
        return Err(io::Error::other("NOTIFY_SOCKET is too long"));
    }
    for (i, b) in path.iter().enumerate() {
        addr.sun_path[i] = *b as libc::c_char;
    }
    // An abstract socket's name starts with a NUL.
    if path.first() == Some(&b'@') {
        addr.sun_path[0] = 0;
    }
    let addr_len = (std::mem::size_of::<libc::sa_family_t>() + path.len()) as libc::socklen_t;
    let mut iov = libc::iovec {
        iov_base: state.as_ptr() as *mut _,
        iov_len: state.len(),
    };
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_name = (&mut addr as *mut libc::sockaddr_un).cast();
    msg.msg_namelen = addr_len;
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    let raw: Vec<i32> = fds.iter().map(|f| f.as_raw_fd()).collect();
    let space = if raw.is_empty() {
        0
    } else {
        unsafe { libc::CMSG_SPACE((raw.len() * std::mem::size_of::<i32>()) as u32) as usize }
    };
    let mut control = vec![0u8; space];
    if !raw.is_empty() {
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        unsafe {
            let c = libc::CMSG_FIRSTHDR(&msg);
            (*c).cmsg_level = libc::SOL_SOCKET;
            (*c).cmsg_type = libc::SCM_RIGHTS;
            (*c).cmsg_len = libc::CMSG_LEN((raw.len() * std::mem::size_of::<i32>()) as u32) as _;
            std::ptr::copy_nonoverlapping(
                raw.as_ptr(),
                libc::CMSG_DATA(c).cast::<i32>(),
                raw.len(),
            );
        }
    }
    if unsafe { libc::sendmsg(fd.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Keeps a descriptor in systemd's store under a name.
pub fn store_fd(name: &str, fd: &OwnedFd) -> io::Result<()> {
    notify(&format!("FDSTORE=1\nFDNAME={name}\nFDPOLL=0"), &[fd])
}

/// Drops a stored descriptor.
pub fn remove_fd(name: &str) -> io::Result<()> {
    notify(&format!("FDSTOREREMOVE=1\nFDNAME={name}"), &[])
}

/// The descriptors systemd passed this process (its store's, after a restart), by name.
pub fn listen_fds() -> Vec<(String, OwnedFd)> {
    let pid_ok = std::env::var("LISTEN_PID")
        .ok()
        .and_then(|p| p.parse::<u32>().ok())
        .is_some_and(|p| p == std::process::id());
    if !pid_ok {
        return Vec::new();
    }
    let n: i32 = std::env::var("LISTEN_FDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let names: Vec<String> = std::env::var("LISTEN_FDNAMES")
        .unwrap_or_default()
        .split(':')
        .map(str::to_owned)
        .collect();
    // SAFETY: systemd hands over descriptors 3..3+n, which nothing else in the process owns.
    unsafe {
        std::env::remove_var("LISTEN_PID");
        std::env::remove_var("LISTEN_FDS");
        std::env::remove_var("LISTEN_FDNAMES");
    }
    (0..n)
        .map(|i| {
            let fd = 3 + i;
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
            (names.get(i as usize).cloned().unwrap_or_default(), unsafe {
                OwnedFd::from_raw_fd(fd)
            })
        })
        .collect()
}
