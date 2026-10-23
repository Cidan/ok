//! User and mount namespaces of a test's own, where it is root and may mount FUSE file systems
//! without privilege (docs/building.md, "Devices").
//!
//! The kernel lets only a single-threaded process enter a new user namespace. A test binary whose
//! scenarios mount enters them itself, first thing in its `main` ([`enter`]) — it is built with a
//! harness of its own ([`run`]), since libtest starts threads before any test runs — and every
//! program it runs inherits them. A test that runs another program somewhere it cannot mount puts
//! that program in namespaces of its own as it is executed ([`unshare`], [`without_fuse`]). No test
//! runs its own binary again to get them.

use std::io;
use std::io::Write as _;
use std::os::unix::process::CommandExt as _;
use std::process::Command;
use std::time::Instant;

/// Puts this process in a new user namespace, as root mapped to the caller, and a new mount
/// namespace that one owns, with every mount private. It refuses unless the process is still
/// single-threaded: call it first in `main`.
pub fn enter() -> io::Result<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let threads = status
        .lines()
        .find_map(|l| l.strip_prefix("Threads:"))
        .map(str::trim)
        .unwrap_or("?");
    if threads != "1" {
        return Err(io::Error::other(format!(
            "only a single-threaded process enters a new user namespace, and this one has {threads} threads: \
             call testns::enter first in main"
        )));
    }
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    if unsafe { libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNS) } != 0 {
        return Err(io::Error::last_os_error());
    }
    std::fs::write("/proc/self/setgroups", "deny")?;
    std::fs::write("/proc/self/uid_map", format!("0 {uid} 1"))?;
    std::fs::write("/proc/self/gid_map", format!("0 {gid} 1"))?;
    make_private()
}

/// Makes every mount in this process's mount namespace private.
fn make_private() -> io::Result<()> {
    if unsafe {
        libc::mount(
            std::ptr::null(),
            c"/".as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A test in a binary of its own harness: its name, and its body, which fails by panicking.
pub type Test = (&'static str, fn());

/// Runs a test binary's tests one at a time, in order, reporting as libtest does — `test <name>
/// ... ok` or `FAILED`, then a summary — and exits: 0 when every test passed, 101 otherwise. As
/// libtest's, arguments that are not flags select the tests whose names hold them (`--exact`: whose
/// names are them), `--skip <name>` leaves those out, and `--list` lists the tests. Other flags
/// (`--nocapture`, `--test-threads`) are accepted and change nothing: output is never captured.
pub fn run(tests: &[Test]) -> ! {
    let mut filters = Vec::new();
    let mut skips = Vec::new();
    let (mut exact, mut list) = (false, false);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--exact" => exact = true,
            "--list" => list = true,
            "--skip" => skips.extend(args.next()),
            "--test-threads" | "--color" | "--format" | "--logfile" | "-Z" => {
                args.next();
            }
            a if a.starts_with('-') => {}
            _ => filters.push(arg),
        }
    }
    let matches = |name: &str, pattern: &str| {
        if exact {
            name == pattern
        } else {
            name.contains(pattern)
        }
    };
    let chosen: Vec<&Test> = tests
        .iter()
        .filter(|(name, _)| filters.is_empty() || filters.iter().any(|f| matches(name, f)))
        .filter(|(name, _)| !skips.iter().any(|s| matches(name, s)))
        .collect();
    if list {
        for (name, _) in &chosen {
            println!("{name}: test");
        }
        std::process::exit(0);
    }
    let started = Instant::now();
    println!("\nrunning {} tests", chosen.len());
    let mut failed = Vec::new();
    for (name, body) in &chosen {
        print!("test {name} ... ");
        let _ = io::stdout().flush();
        if std::panic::catch_unwind(body).is_ok() {
            println!("ok");
        } else {
            println!("FAILED");
            failed.push(*name);
        }
    }
    if !failed.is_empty() {
        println!("\nfailures:");
        for name in &failed {
            println!("    {name}");
        }
    }
    println!(
        "\ntest result: {}. {} passed; {} failed; 0 ignored; 0 measured; {} filtered out; finished in {:.2}s\n",
        if failed.is_empty() { "ok" } else { "FAILED" },
        chosen.len() - failed.len(),
        failed.len(),
        tests.len() - chosen.len(),
        started.elapsed().as_secs_f64()
    );
    std::process::exit(if failed.is_empty() { 0 } else { 101 })
}

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

/// Makes `cmd`, another program, run in a new user namespace, as root mapped to the caller, and a
/// new mount namespace it owns, with every mount private: entered as it is executed.
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
            make_private()
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
