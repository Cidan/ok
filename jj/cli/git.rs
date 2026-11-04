//! git's commands in ok workspaces (docs/git.md): this binary, run as `git`, answers git's
//! commands over ok's jj — `git clone` makes a virtual workspace, `git commit` a change, `git push`
//! uploads ChangeLists, `git pull` syncs — and hands everything outside ok to git itself.
//!
//! `front` is the half that runs first, by the name `git`; the rest runs as the hidden `jj
//! git-compat`, the front end executing this binary again with jj's runner.

pub mod front;

mod change;
mod fmt;
mod history;
mod inspect;
mod refs;
mod remote;
mod repo;

use std::io::Write as _;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use jj_cli::cli_util::CommandHelper;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;

/// The hidden subcommand the front end runs.
pub const SUBCOMMAND: &str = "git-compat";

/// Run git's command in an ok workspace (what `git` does there; not run by hand)
#[derive(clap::Args, Clone, Debug)]
pub struct GitCompatArgs {
    /// git's global options, its command, and the command's arguments
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// The exit status a git command chose, which jj's runner, knowing only success and its own
/// errors, cannot carry: git's are 1 for "no" (a difference, a missing key), 128 for fatal errors,
/// and 129 for usage.
static EXIT: AtomicI32 = AtomicI32::new(-1);

/// The status the git command that ran chose, if one ran.
pub fn exit_code() -> Option<u8> {
    let c = EXIT.load(Ordering::SeqCst);
    (c >= 0).then_some(c as u8)
}

/// What a git command returns: its exit status.
pub type Code = Result<u8, CommandError>;

/// The context every command has: jj's, and git's global options.
pub struct Cx<'a> {
    pub command: &'a CommandHelper,
    pub globals: front::Globals,
}

impl Cx<'_> {
    /// A `-c name=value` given for this command, by its name (case-insensitive in the section and
    /// key, as git compares them).
    pub fn config(&self, name: &str) -> Option<String> {
        self.globals.config.iter().rev().find_map(|c| {
            let (k, v) = c.split_once('=').unwrap_or((c, "true"));
            k.eq_ignore_ascii_case(name).then(|| v.to_owned())
        })
    }

    fn config_bool(&self, name: &str) -> Option<bool> {
        self.config(name).map(|v| {
            !matches!(
                v.to_ascii_lowercase().as_str(),
                "false" | "no" | "off" | "0" | ""
            )
        })
    }

    /// Whether paths with bytes above ASCII are quoted (`core.quotePath`, on by default).
    pub fn quote_high(&self) -> bool {
        self.config_bool("core.quotepath").unwrap_or(true)
    }

    /// The workspace, its working copy snapshotted first: for what reads or changes the working
    /// tree.
    pub async fn ws(&self, ui: &Ui) -> Result<WorkspaceCommandHelper, CommandError> {
        self.command.workspace_helper(ui).await
    }

    /// The workspace without a snapshot: for what reads only commits.
    pub async fn ws_read(&self, ui: &Ui) -> Result<WorkspaceCommandHelper, CommandError> {
        self.command.workspace_helper_no_snapshot(ui).await
    }

    /// Git's pathspecs, against the workspace.
    pub fn pathspec(
        &self,
        ws: &WorkspaceCommandHelper,
        specs: &[String],
    ) -> Result<repo::Pathspec, repo::BadPathspec> {
        repo::Pathspec::parse(
            ws.workspace_root(),
            self.command.cwd(),
            specs,
            self.globals.literal_pathspecs
                || std::env::var_os("GIT_LITERAL_PATHSPECS").is_some_and(|v| v == "1"),
        )
    }

    pub async fn resolver<'w>(
        &self,
        ws: &'w WorkspaceCommandHelper,
    ) -> Result<repo::Resolver<'w>, CommandError> {
        repo::Resolver::new(self.command, ws).await
    }
}

/// `fatal: <message>`, git's exit status 128.
pub fn fatal(ui: &Ui, message: impl std::fmt::Display) -> Code {
    writeln!(ui.stderr(), "fatal: {message}")?;
    Ok(128)
}

/// `error: <message>`, with the status given.
pub fn error(ui: &Ui, message: impl std::fmt::Display, code: u8) -> Code {
    writeln!(ui.stderr(), "error: {message}")?;
    Ok(code)
}

/// `hint: <message>`, a line of it each.
pub fn hint(ui: &Ui, message: impl std::fmt::Display) -> Result<(), CommandError> {
    for line in message.to_string().lines() {
        writeln!(ui.stderr(), "hint: {line}")?;
    }
    Ok(())
}

/// A revision that did not resolve, as git says it.
pub fn rev_error(ui: &Ui, spec: &str, e: repo::RevError) -> Code {
    match e.message(spec) {
        Some(m) => {
            fatal(ui, &m)?;
            if let repo::RevError::Unknown | repo::RevError::Unborn = e {
                writeln!(
                    ui.stderr(),
                    "Use '--' to separate paths from revisions, like this:\n'git <command> [<revision>...] -- [<file>...]'"
                )?;
            }
            Ok(128)
        }
        None => match e {
            repo::RevError::Command(e) => Err(e),
            _ => unreachable!(),
        },
    }
}

/// Parses a command's arguments, or says why not: help to stdout (status 0), a usage error to
/// stderr (status 129), as git does.
pub fn parse<T: clap::Parser>(ui: &Ui, name: &str, args: &[String]) -> Result<T, u8> {
    let bin = format!("git {name}");
    match T::try_parse_from(std::iter::once(bin).chain(args.iter().cloned())) {
        Ok(t) => Ok(t),
        Err(e) => {
            use clap::error::ErrorKind;
            let text = e.render().to_string();
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                let _ = write!(ui.stdout(), "{text}");
                Err(0)
            } else {
                let _ = write!(ui.stderr(), "{text}");
                Err(129)
            }
        }
    }
}

/// Parses, or returns the status parsing chose.
macro_rules! parse_or_return {
    ($ui:expr, $name:expr, $args:expr) => {
        match $crate::git::parse($ui, $name, $args) {
            Ok(a) => a,
            Err(code) => return Ok(code),
        }
    };
}
pub(crate) use parse_or_return;

pub async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    args: GitCompatArgs,
) -> Result<(), CommandError> {
    let (globals, rest) = front::split_globals(&args.args);
    let cx = Cx { command, globals };
    let code = match rest.split_first() {
        Some((name, args)) => dispatch(ui, &cx, name, args).await?,
        None => fatal(ui, "no command")?,
    };
    EXIT.store(code as i32, Ordering::SeqCst);
    Ok(())
}

/// Commands git has that ok answers without doing anything: what they keep up does not exist
/// here, or ok keeps it up itself.
const NOTHING_TO_DO: &[&str] = &[
    "gc",
    "prune",
    "repack",
    "maintenance",
    "pack-refs",
    "prune-packed",
    "fsck",
    "count-objects",
    "commit-graph",
    "multi-pack-index",
    "rerere",
];

async fn dispatch(ui: &mut Ui, cx: &Cx<'_>, name: &str, args: &[String]) -> Code {
    match name {
        "clone" => remote::clone(ui, cx, args).await,
        "push" => remote::push(ui, cx, args).await,
        "pull" => remote::pull(ui, cx, args).await,
        "fetch" => remote::fetch(ui, cx, args).await,
        "remote" => remote::remote(ui, cx, args).await,

        "status" => inspect::status(ui, cx, args).await,
        "diff" => inspect::diff(ui, cx, args).await,
        "diff-index" => inspect::diff_index(ui, cx, args).await,
        "diff-tree" => inspect::diff_tree(ui, cx, args).await,
        "diff-files" => inspect::diff_files(ui, cx, args).await,
        "log" | "whatchanged" => inspect::log(ui, cx, name, args).await,
        "show" => inspect::show(ui, cx, args).await,
        "rev-list" => inspect::rev_list(ui, cx, args).await,
        "blame" | "annotate" => inspect::blame(ui, cx, args).await,
        "grep" => inspect::grep(ui, cx, args).await,
        "ls-files" => inspect::ls_files(ui, cx, args).await,
        "ls-tree" => inspect::ls_tree(ui, cx, args).await,
        "cat-file" => inspect::cat_file(ui, cx, args).await,
        "check-ignore" => inspect::check_ignore(ui, cx, args).await,
        "shortlog" => inspect::shortlog(ui, cx, args).await,

        "rev-parse" => refs::rev_parse(ui, cx, args).await,
        "branch" => refs::branch(ui, cx, args).await,
        "tag" => refs::tag(ui, cx, args).await,
        "show-ref" => refs::show_ref(ui, cx, args).await,
        "for-each-ref" => refs::for_each_ref(ui, cx, args).await,
        "symbolic-ref" => refs::symbolic_ref(ui, cx, args).await,
        "merge-base" => refs::merge_base(ui, cx, args).await,
        "describe" => refs::describe(ui, cx, args).await,
        "name-rev" => refs::name_rev(ui, cx, args).await,
        "config" => refs::config(ui, cx, args).await,
        "update-index" => refs::update_index(ui, cx, args).await,
        "submodule" => refs::submodule(ui, cx, args).await,
        "worktree" => refs::worktree(ui, cx, args).await,
        "help" => refs::help(ui, cx, args).await,

        "add" | "stage" => change::add(ui, cx, args).await,
        "rm" => change::rm(ui, cx, args).await,
        "mv" => change::mv(ui, cx, args).await,
        "restore" => change::restore(ui, cx, args).await,
        "commit" => change::commit(ui, cx, args).await,
        "reset" => change::reset(ui, cx, args).await,
        "checkout" => change::checkout(ui, cx, args).await,
        "switch" => change::switch(ui, cx, args).await,
        "stash" => change::stash(ui, cx, args).await,
        "clean" => change::clean(ui, cx, args).await,

        "merge" => history::merge(ui, cx, args).await,
        "rebase" => history::rebase(ui, cx, args).await,
        "cherry-pick" => history::cherry_pick(ui, cx, args).await,
        "revert" => history::revert(ui, cx, args).await,

        _ if NOTHING_TO_DO.contains(&name) => Ok(0),
        "reflog" => {
            fatal(ui, "ok keeps no reflog")?;
            hint(
                ui,
                "`jj op log` lists every operation on this workspace, and `jj undo` undoes the last",
            )?;
            Ok(128)
        }
        "bisect" => unsupported(ui, name, "`jj bisect run` bisects with a command"),
        "am" | "format-patch" | "send-email" | "request-pull" => unsupported(
            ui,
            name,
            "changes move between people on ok as ChangeLists: `git push` uploads them",
        ),
        "filter-branch" | "filter-repo" | "replace" | "notes" | "fast-export" | "fast-import"
        | "bundle" | "archive" | "svn" | "p4" | "lfs" | "sparse-checkout" | "subtree" => {
            unsupported(ui, name, "")
        }
        "update-ref" | "write-tree" | "commit-tree" | "read-tree" | "mktree" | "mktag"
        | "pack-objects" | "unpack-objects" | "index-pack" | "send-pack" | "receive-pack"
        | "upload-pack" | "fetch-pack" | "checkout-index" | "merge-file" | "merge-tree" => {
            unsupported(
                ui,
                name,
                "ok keeps no git objects: its plumbing has nothing to write",
            )
        }
        _ => {
            fatal(
                ui,
                format!("'{name}' is not a git command ok answers. See 'git help ok'."),
            )?;
            Ok(1)
        }
    }
}

fn unsupported(ui: &Ui, name: &str, why: &str) -> Code {
    fatal(
        ui,
        format!("'git {name}' is not available in ok workspaces"),
    )?;
    if !why.is_empty() {
        hint(ui, why)?;
    }
    Ok(128)
}
