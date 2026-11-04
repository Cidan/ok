//! The `git` front end (docs/git.md, "How it runs"): this binary run by the name `git`. In an ok
//! workspace, and for a clone, it runs ok's git commands — it executes itself as `jj git-compat`,
//! since jj's runner reads its arguments from the process — and everywhere else it runs the git
//! that is on the PATH after it, so a `git` that is ok's leaves every other repository to git.

use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::ExitCode;

/// Names the git this front end hands everything outside ok to, instead of the next on the PATH.
pub const REAL_GIT_ENV: &str = "OK_REAL_GIT";

/// Set on the git this front end runs, to the path it ran: a front end that finds itself started
/// by that path was taken for git, and says so rather than running itself forever.
const RAN_ENV: &str = "OK_GIT_RAN";

/// Commands that never need a repository, or that make one, and so are always git's: in an ok
/// workspace too.
const ALWAYS_GIT: &[&str] = &[
    "init",
    "version",
    "hash-object",
    "var",
    "check-ref-format",
    "stripspace",
    "interpret-trailers",
    "column",
    "credential",
    "credential-cache",
    "credential-store",
    "mailinfo",
    "mailsplit",
    "web--browse",
];

/// Global options before the command: what git reads, in the order it reads them.
#[derive(Debug, Default)]
pub struct Globals {
    /// `-C` directories, in order.
    pub dirs: Vec<PathBuf>,
    /// `-c name=value`, in order.
    pub config: Vec<String>,
    pub no_pager: bool,
    pub literal_pathspecs: bool,
    /// An option that addresses a git repository directly (`--git-dir`, `--work-tree`, `--bare`):
    /// the command is git's.
    pub addresses_git: bool,
    /// An option only git answers (`--version`, `--help`, `--exec-path`, …).
    pub git_only: bool,
    /// Every option, as given, but `-C`: what the in-process half parses again.
    pub forwarded: Vec<String>,
}

/// Splits the global options off `args`, returning them and the command with its arguments.
pub fn split_globals(args: &[String]) -> (Globals, Vec<String>) {
    let mut g = Globals::default();
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        let value = |i: &mut usize| -> Option<String> {
            *i += 1;
            args.get(*i).cloned()
        };
        match a {
            "-C" => {
                if let Some(d) = value(&mut i)
                    && !d.is_empty()
                {
                    g.dirs.push(PathBuf::from(d));
                }
            }
            "-c" => {
                if let Some(c) = value(&mut i) {
                    g.forwarded.extend(["-c".to_owned(), c.clone()]);
                    g.config.push(c);
                }
            }
            "--no-pager" | "-P" => {
                g.no_pager = true;
                g.forwarded.push(a.to_owned());
            }
            "-p"
            | "--paginate"
            | "--no-replace-objects"
            | "--no-optional-locks"
            | "--no-lazy-fetch"
            | "--no-advice"
            | "--glob-pathspecs"
            | "--noglob-pathspecs"
            | "--icase-pathspecs" => g.forwarded.push(a.to_owned()),
            "--literal-pathspecs" => {
                g.literal_pathspecs = true;
                g.forwarded.push(a.to_owned());
            }
            "--git-dir" | "--work-tree" | "--namespace" | "--super-prefix" => {
                g.addresses_git = true;
                i += 1;
            }
            "--bare" => g.addresses_git = true,
            "--version" | "-v" | "--help" | "-h" | "--exec-path" | "--html-path" | "--man-path"
            | "--info-path" => g.git_only = true,
            _ if a.starts_with("-C") && a.len() > 2 => g.dirs.push(PathBuf::from(&a[2..])),
            _ if a.starts_with("-c") && a.len() > 2 => {
                g.forwarded.push(a.to_owned());
                g.config.push(a[2..].to_owned());
            }
            _ if a.starts_with("--git-dir=")
                || a.starts_with("--work-tree=")
                || a.starts_with("--namespace=")
                || a.starts_with("--super-prefix=") =>
            {
                g.addresses_git = true
            }
            _ if a.starts_with("--exec-path=") || a.starts_with("--list-cmds=") => {
                g.git_only = true
            }
            _ if a.starts_with("--config-env=") => g.forwarded.push(a.to_owned()),
            _ if a.starts_with('-') => g.git_only = true,
            _ => break,
        }
        i += 1;
    }
    (g, args[i.min(args.len())..].to_vec())
}

/// What a directory is in: the nearest `.git` or `.jj` above it decides.
#[derive(Debug, PartialEq, Eq)]
pub enum Place {
    /// An ok workspace, at this root.
    Ok(PathBuf),
    /// A git repository, a jj repository that is not ok's, or nothing.
    Elsewhere,
}

/// Finds what `dir` is in. A `.git` beside a `.jj` is a colocated jj repository, whose backend is
/// git's: git's.
pub fn place(dir: &Path) -> Place {
    for d in dir.ancestors() {
        if d.join(".git").exists() {
            return Place::Elsewhere;
        }
        let jj = d.join(".jj");
        if jj.is_dir() {
            return if is_ok_repo(&jj) {
                Place::Ok(d.to_owned())
            } else {
                Place::Elsewhere
            };
        }
    }
    Place::Elsewhere
}

/// Whether the jj workspace whose `.jj` is `jj_dir` has ok's stores: its repository (`repo`, a
/// directory, or a file naming one) has a store of type `ok`.
fn is_ok_repo(jj_dir: &Path) -> bool {
    let repo = jj_dir.join("repo");
    let repo = if repo.is_file() {
        match std::fs::read_to_string(&repo) {
            Ok(p) => jj_dir.join(p.trim_end()),
            Err(_) => return false,
        }
    } else {
        repo
    };
    std::fs::read_to_string(repo.join("store").join("type"))
        .is_ok_and(|t| t.trim() == ok_store::NAME)
}

/// The git everything outside ok goes to: `OK_REAL_GIT`, else the first `git` on the PATH that is
/// not this binary.
pub fn real_git() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(REAL_GIT_ENV).filter(|p| !p.is_empty()) {
        return Some(PathBuf::from(p));
    }
    let me = std::env::current_exe()
        .ok()
        .and_then(|p| std::fs::canonicalize(p).ok());
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join(if cfg!(windows) { "git.exe" } else { "git" }))
        .filter(|c| is_executable(c))
        .find(|c| std::fs::canonicalize(c).ok() != me)
}

fn is_executable(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// Whether this process was started by the name `git`.
fn started_as_git() -> bool {
    std::env::args_os()
        .next()
        .and_then(|a0| {
            Path::new(&a0)
                .file_stem()
                .map(|s| s.eq_ignore_ascii_case("git"))
        })
        .unwrap_or(false)
}

/// The front end: `None` unless this process was started as `git`; otherwise what it exits with,
/// having run git, or itself as ok's git.
pub fn main() -> Option<ExitCode> {
    if !started_as_git() {
        return None;
    }
    if let (Some(ran), Ok(me)) = (std::env::var_os(RAN_ENV), std::env::current_exe())
        && std::fs::canonicalize(&me).ok() == std::fs::canonicalize(&ran).ok()
    {
        eprintln!(
            "fatal: {} is ok's jj, not git; name git with {REAL_GIT_ENV}",
            me.display()
        );
        return Some(ExitCode::from(128));
    }
    let raw: Vec<OsString> = std::env::args_os().skip(1).collect();
    let Some(args) = raw
        .iter()
        .map(|a| a.to_str().map(str::to_owned))
        .collect::<Option<Vec<String>>>()
    else {
        return Some(run_git(&raw, None));
    };
    let (globals, rest) = split_globals(&args);
    let cmd = rest.first().map(String::as_str);
    if globals.addresses_git
        || globals.git_only
        || cmd.is_none()
        || std::env::var_os("GIT_DIR").is_some()
    {
        return Some(run_git(&raw, None));
    }
    let mut cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("fatal: unable to read the current directory: {e}");
            return Some(ExitCode::from(128));
        }
    };
    for d in &globals.dirs {
        cwd = cwd.join(d);
    }
    let cmd = cmd.unwrap();
    let ours = match place(&cwd) {
        _ if ALWAYS_GIT.contains(&cmd) => false,
        // `git help` and `git help <command>` are git's, but for `git help ok`.
        _ if cmd == "help" => rest.get(1).is_some_and(|a| a == "ok"),
        Place::Ok(root) => {
            if is_repoless(cmd, &rest[1..]) {
                return Some(run_git(&repoless_args(&args), Some(&root)));
            }
            true
        }
        // A clone decides once it can read ok's configuration: an ok repository's, or git's.
        Place::Elsewhere => cmd == "clone",
    };
    if !ours {
        return Some(run_git(&raw, None));
    }
    if !globals.dirs.is_empty()
        && let Err(e) = std::env::set_current_dir(&cwd)
    {
        eprintln!("fatal: cannot change to '{}': {e}", cwd.display());
        return Some(ExitCode::from(128));
    }
    Some(run_ours(&globals, &rest))
}

/// Commands git runs without a repository, which in an ok workspace mean the same as anywhere:
/// `git apply` and `git diff --no-index`.
fn is_repoless(cmd: &str, args: &[String]) -> bool {
    match cmd {
        "apply" => true,
        "diff" => args.iter().any(|a| a == "--no-index"),
        _ => false,
    }
}

/// A repoless command's arguments for git: after `apply`, `--index` and `--cached` dropped, since
/// the index is the working tree, which a patch applies to; `--3way` too, which needs git's
/// objects.
fn repoless_args(args: &[String]) -> Vec<OsString> {
    let mut applying = false;
    let mut out = Vec::new();
    for a in args {
        if applying && matches!(a.as_str(), "--index" | "--cached" | "--3way" | "-3") {
            continue;
        }
        applying |= a == "apply";
        out.push(OsString::from(a));
    }
    out
}

/// Runs this binary as `jj git-compat`, with jj's own options from git's.
fn run_ours(globals: &Globals, rest: &[String]) -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("fatal: cannot find ok's jj: {e}");
            return ExitCode::from(128);
        }
    };
    let mut cmd = Command::new(&exe);
    // git says what it did in its own words; jj's reports of the working copy moving would only
    // be noise. A clone keeps them: they say where the workspace is and what it is called.
    if rest.first().map(String::as_str) != Some("clone") {
        cmd.arg("--quiet");
    }
    let pager = std::env::var("GIT_PAGER").ok();
    if globals.no_pager || pager.as_deref().is_some_and(|p| p.is_empty() || p == "cat") {
        cmd.arg("--no-pager");
    } else if let Some(p) = pager {
        cmd.args(["--config", &format!("ui.pager={}", toml_string(&p))]);
    }
    let mut color = None;
    let mut editor = std::env::var("GIT_EDITOR").ok();
    for c in &globals.config {
        if let Some(c) = color_setting(c) {
            color = Some(c);
        }
        if let Some((k, v)) = c.split_once('=')
            && k.eq_ignore_ascii_case("core.editor")
            && editor.is_none()
        {
            editor = Some(v.to_owned());
        }
    }
    // A command's own `--color` and `--no-color` decide its colors, as git's do.
    for a in rest.iter().skip(1).take_while(|a| *a != "--") {
        match a.as_str() {
            "--color" => color = Some("always"),
            "--no-color" => color = Some("never"),
            _ => {
                if let Some(v) = a.strip_prefix("--color=") {
                    color = color_setting(&format!("color.ui={v}")).or(color);
                }
            }
        }
    }
    if let Some(c) = color {
        cmd.arg(format!("--color={c}"));
    }
    if let Some(e) = editor.filter(|e| !e.is_empty()) {
        cmd.args(["--config", &format!("ui.editor={}", toml_string(&e))]);
    }
    cmd.arg(super::SUBCOMMAND).arg("--");
    cmd.args(&globals.forwarded);
    cmd.args(rest);
    exec(cmd, "ok's jj")
}

/// `color.ui` (or `color.diff`, …) as jj's `--color`.
fn color_setting(c: &str) -> Option<&'static str> {
    let (k, v) = c.split_once('=').unwrap_or((c, "true"));
    let k = k.to_ascii_lowercase();
    if k != "color.ui" && !k.starts_with("color.") {
        return None;
    }
    match v.to_ascii_lowercase().as_str() {
        "always" => Some("always"),
        "never" | "false" | "off" | "no" | "0" => Some("never"),
        "auto" | "true" | "on" | "yes" | "1" => Some("auto"),
        _ => None,
    }
}

fn toml_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Runs git with `args`. In an ok workspace (`within`), git is kept from finding a repository
/// around it.
pub fn run_git(args: &[OsString], within: Option<&Path>) -> ExitCode {
    let Some(git) = real_git() else {
        eprintln!(
            "fatal: there is no git on the PATH but ok's; outside ok workspaces, git commands need one (or {REAL_GIT_ENV})"
        );
        return ExitCode::from(128);
    };
    let mut cmd = Command::new(&git);
    cmd.args(args).env(RAN_ENV, &git);
    if let Some(root) = within.and_then(Path::parent) {
        cmd.env("GIT_CEILING_DIRECTORIES", root);
    }
    exec(cmd, &git.display().to_string())
}

#[cfg(unix)]
fn exec(mut cmd: Command, what: &str) -> ExitCode {
    use std::os::unix::process::CommandExt as _;
    let e = cmd.exec();
    eprintln!("fatal: cannot run {what}: {e}");
    ExitCode::from(128)
}

#[cfg(not(unix))]
fn exec(mut cmd: Command, what: &str) -> ExitCode {
    match cmd.status() {
        Ok(s) => ExitCode::from(s.code().unwrap_or(128).clamp(0, 255) as u8),
        Err(e) => {
            eprintln!("fatal: cannot run {what}: {e}");
            ExitCode::from(128)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn globals() {
        let (g, rest) = split_globals(&strings(&[
            "-C",
            "sub",
            "-c",
            "color.ui=never",
            "--no-pager",
            "-Cother",
            "status",
            "-s",
        ]));
        assert_eq!(g.dirs, vec![PathBuf::from("sub"), PathBuf::from("other")]);
        assert_eq!(g.config, vec!["color.ui=never".to_owned()]);
        assert!(g.no_pager && !g.addresses_git && !g.git_only);
        assert_eq!(
            g.forwarded,
            strings(&["-c", "color.ui=never", "--no-pager"])
        );
        assert_eq!(rest, strings(&["status", "-s"]));

        let (g, rest) = split_globals(&strings(&["--git-dir=/x/.git", "log"]));
        assert!(g.addresses_git);
        assert_eq!(rest, strings(&["log"]));
        let (g, rest) = split_globals(&strings(&["--version"]));
        assert!(g.git_only && rest.is_empty());
    }

    #[test]
    fn apply_onto_the_working_tree() {
        assert_eq!(
            repoless_args(&strings(&["-c", "x=y", "apply", "--index", "-3", "p.diff"])),
            vec![
                OsString::from("-c"),
                OsString::from("x=y"),
                OsString::from("apply"),
                OsString::from("p.diff")
            ]
        );
    }

    #[test]
    fn colors() {
        assert_eq!(color_setting("color.ui=always"), Some("always"));
        assert_eq!(color_setting("color.diff=false"), Some("never"));
        assert_eq!(color_setting("core.pager=less"), None);
    }

    #[test]
    fn places() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(ws.join(".jj/repo/store")).unwrap();
        std::fs::write(ws.join(".jj/repo/store/type"), "ok").unwrap();
        std::fs::create_dir_all(ws.join("sub/vendored/.git")).unwrap();
        assert_eq!(place(&ws.join("sub")), Place::Ok(ws.clone()));
        assert_eq!(place(&ws.join("sub/vendored")), Place::Elsewhere);
        // A virtual workspace's `.jj` names its repository in a file.
        let virt = tmp.path().join("virt");
        std::fs::create_dir_all(virt.join(".jj")).unwrap();
        std::fs::write(
            virt.join(".jj/repo"),
            ws.join(".jj/repo").display().to_string(),
        )
        .unwrap();
        assert_eq!(place(&virt), Place::Ok(virt.clone()));
        let other = tmp.path().join("other");
        std::fs::create_dir_all(other.join(".jj/repo/store")).unwrap();
        std::fs::write(other.join(".jj/repo/store/type"), "git").unwrap();
        assert_eq!(place(&other), Place::Elsewhere);
    }
}
