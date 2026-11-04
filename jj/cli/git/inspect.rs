//! git's commands that read: `status`, `diff` (and `diff-index`, `diff-tree`, `diff-files`),
//! `log`, `show`, `rev-list`, `shortlog`, `blame`, `grep`, `ls-files`, `ls-tree`, `cat-file`, and
//! `check-ignore`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::IsTerminal as _;
use std::io::Write as _;

use futures::StreamExt as _;
use futures::TryStreamExt as _;
use jj_cli::cli_util::WorkspaceCommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::diff_util::ColorWordsDiffOptions;
use jj_cli::diff_util::DiffFormat;
use jj_cli::diff_util::DiffRenderer;
use jj_cli::diff_util::DiffStatOptions;
use jj_cli::diff_util::DiffStats;
use jj_cli::diff_util::LineDiffOptions;
use jj_cli::diff_util::UnifiedDiffOptions;
use jj_cli::formatter::Formatter;
use jj_cli::formatter::FormatterExt as _;
use jj_cli::ui::Ui;
use jj_lib::backend::CommitId;
use jj_lib::backend::MergedTreeValue;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::copies::CopiesTreeDiffEntry;
use jj_lib::copies::CopyOperation;
use jj_lib::copies::CopyRecords;
use jj_lib::diff_presentation::LineCompareMode;
use jj_lib::matchers::FilesMatcher;
use jj_lib::matchers::Matcher;
use jj_lib::merge::Diff;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::RevsetExpression;
use jj_lib::ui_path::RepoPathUiConverter;

use super::Code;
use super::Cx;
use super::fatal;
use super::fmt;
use super::parse_or_return;
use super::repo;
use super::repo::Head;
use super::repo::Object;
use super::rev_error;

/// The diff options git's diffing commands share.
#[derive(clap::Args, Clone, Debug, Default)]
pub struct DiffFlags {
    /// Show the patch (the default for `diff`)
    #[arg(short = 'p', short_alias = 'u', long = "patch")]
    pub patch: bool,
    /// Show no patch
    #[arg(short = 's', long = "no-patch")]
    pub no_patch: bool,
    /// A histogram of the changes
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub stat: Option<String>,
    #[arg(long)]
    pub numstat: bool,
    #[arg(long)]
    pub shortstat: bool,
    #[arg(long)]
    pub name_only: bool,
    #[arg(long)]
    pub name_status: bool,
    #[arg(long)]
    pub summary: bool,
    #[arg(long)]
    pub compact_summary: bool,
    #[arg(short = 'U', long = "unified")]
    pub unified: Option<usize>,
    #[arg(short = 'W', long = "function-context")]
    pub function_context: bool,
    #[arg(short = 'w', long = "ignore-all-space")]
    pub ignore_all_space: bool,
    #[arg(short = 'b', long = "ignore-space-change")]
    pub ignore_space_change: bool,
    #[arg(long = "ignore-blank-lines")]
    pub ignore_blank_lines: bool,
    #[arg(long = "ignore-space-at-eol")]
    pub ignore_space_at_eol: bool,
    #[arg(long = "ignore-cr-at-eol")]
    pub ignore_cr_at_eol: bool,
    #[arg(short = 'M', long = "find-renames", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub find_renames: Option<String>,
    #[arg(short = 'C', long = "find-copies", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub find_copies: Option<String>,
    #[arg(long = "find-copies-harder")]
    pub find_copies_harder: bool,
    #[arg(long = "no-renames")]
    pub no_renames: bool,
    #[arg(long = "diff-filter")]
    pub diff_filter: Option<String>,
    #[arg(long = "no-prefix")]
    pub no_prefix: bool,
    #[arg(long = "default-prefix")]
    pub default_prefix: bool,
    #[arg(long = "src-prefix")]
    pub src_prefix: Option<String>,
    #[arg(long = "dst-prefix")]
    pub dst_prefix: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "always")]
    pub color: Option<String>,
    #[arg(long = "no-color")]
    pub no_color: bool,
    #[arg(long = "color-words", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub color_words: Option<String>,
    #[arg(long = "word-diff", num_args = 0..=1, require_equals = true, default_missing_value = "plain")]
    pub word_diff: Option<String>,
    #[arg(long = "ext-diff")]
    pub ext_diff: bool,
    #[arg(long = "no-ext-diff")]
    pub no_ext_diff: bool,
    #[arg(long = "textconv")]
    pub textconv: bool,
    #[arg(long = "no-textconv")]
    pub no_textconv: bool,
    #[arg(long)]
    pub minimal: bool,
    #[arg(long)]
    pub patience: bool,
    #[arg(long)]
    pub histogram: bool,
    #[arg(long = "diff-algorithm")]
    pub diff_algorithm: Option<String>,
    #[arg(long = "indent-heuristic")]
    pub indent_heuristic: bool,
    #[arg(long = "no-indent-heuristic")]
    pub no_indent_heuristic: bool,
    #[arg(long = "ignore-submodules", num_args = 0..=1, require_equals = true, default_missing_value = "all")]
    pub ignore_submodules: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "short")]
    pub submodule: Option<String>,
    #[arg(long)]
    pub binary: bool,
    #[arg(short = 'a', long)]
    pub text: bool,
    #[arg(long = "full-index")]
    pub full_index: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub abbrev: Option<String>,
    #[arg(long = "no-abbrev")]
    pub no_abbrev: bool,
    #[arg(long = "stat-width")]
    pub stat_width: Option<String>,
    #[arg(long = "stat-name-width")]
    pub stat_name_width: Option<String>,
    #[arg(long = "stat-count")]
    pub stat_count: Option<String>,
    #[arg(long = "stat-graph-width")]
    pub stat_graph_width: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub relative: Option<String>,
    #[arg(long = "no-relative")]
    pub no_relative: bool,
    #[arg(short = 'R', id = "swap_sides")]
    pub swap: bool,
    #[arg(long)]
    pub raw: bool,
    #[arg(long)]
    pub check: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    pub dirstat: Option<String>,
    #[arg(long = "exit-code")]
    pub exit_code: bool,
    #[arg(long = "ita-invisible-in-index")]
    pub ita_invisible_in_index: bool,
    #[arg(long = "ita-visible-in-index")]
    pub ita_visible_in_index: bool,
    #[arg(long = "output-indicator-new")]
    pub output_indicator_new: Option<String>,
    #[arg(long = "output-indicator-old")]
    pub output_indicator_old: Option<String>,
    #[arg(long = "output-indicator-context")]
    pub output_indicator_context: Option<String>,
}

/// What a diff shows, from git's flags.
#[derive(Clone, Debug)]
pub struct DiffShow {
    pub patch: bool,
    pub words: bool,
    pub stat: bool,
    pub numstat: bool,
    pub shortstat: bool,
    pub name_only: bool,
    pub name_status: bool,
    pub summary: bool,
    pub context: usize,
    pub prefix: bool,
    pub compare: LineCompareMode,
    pub filter: Option<String>,
    pub reverse: bool,
    pub relative: bool,
    pub z: bool,
}

impl DiffShow {
    /// The formats the flags ask for; `default_patch` when they ask for none.
    pub fn from_flags(f: &DiffFlags, default_patch: bool, z: bool) -> Result<Self, String> {
        if f.raw {
            return Err("--raw shows git's object IDs, which ok does not keep".to_owned());
        }
        if f.check {
            return Err("--check is not available in ok workspaces".to_owned());
        }
        if f.dirstat.is_some() {
            return Err("--dirstat is not available in ok workspaces".to_owned());
        }
        let words = f.color_words.is_some() || f.word_diff.is_some();
        let any_short = f.stat.is_some()
            || f.numstat
            || f.shortstat
            || f.name_only
            || f.name_status
            || f.summary
            || f.compact_summary;
        let patch = !f.no_patch && (f.patch || (default_patch && !any_short && !words));
        Ok(Self {
            patch: patch && !words,
            words: words && !f.no_patch,
            stat: f.stat.is_some() || f.compact_summary,
            numstat: f.numstat,
            shortstat: f.shortstat,
            name_only: f.name_only,
            name_status: f.name_status,
            summary: f.summary || f.compact_summary,
            context: if f.function_context {
                usize::MAX / 2
            } else {
                f.unified.unwrap_or(3)
            },
            prefix: !f.no_prefix,
            compare: if f.ignore_all_space {
                LineCompareMode::IgnoreAllSpace
            } else if f.ignore_space_change {
                LineCompareMode::IgnoreSpaceChange
            } else {
                LineCompareMode::Exact
            },
            filter: f.diff_filter.clone(),
            reverse: f.swap,
            relative: f.relative.is_some() && !f.no_relative,
            z,
        })
    }

    pub fn shows_anything(&self) -> bool {
        self.patch
            || self.words
            || self.stat
            || self.numstat
            || self.shortstat
            || self.name_only
            || self.name_status
            || self.summary
    }
}

/// A changed path: its status letter (`A`, `D`, `M`, `R`, `C`, `T`, `U`), where it was and is, and
/// its values before and after.
#[derive(Clone, Debug)]
pub struct Entry {
    pub status: char,
    pub source: RepoPathBuf,
    pub target: RepoPathBuf,
    pub before: MergedTreeValue,
    pub after: MergedTreeValue,
}

impl Entry {
    pub fn is_copy(&self) -> bool {
        matches!(self.status, 'R' | 'C')
    }
}

/// Git's mode for a tree value: `100644`, `100755`, `120000`, `040000`, or `000000` absent.
pub fn mode(value: &MergedTreeValue) -> &'static str {
    match value.as_resolved() {
        Some(Some(TreeValue::File {
            executable: false, ..
        })) => "100644",
        Some(Some(TreeValue::File {
            executable: true, ..
        })) => "100755",
        Some(Some(TreeValue::Symlink(_))) => "120000",
        Some(Some(TreeValue::Tree(_))) => "040000",
        Some(Some(TreeValue::GitSubmodule(_))) => "160000",
        Some(None) => "000000",
        None => "100644",
    }
}

/// A value's object ID, or zeros where there is none (absent, or a conflict).
pub fn object_hex(value: &MergedTreeValue, width: usize) -> String {
    match value.as_resolved() {
        Some(Some(TreeValue::File { id, .. })) => id.hex(),
        Some(Some(TreeValue::Symlink(id))) => id.hex(),
        Some(Some(TreeValue::Tree(id))) => id.hex(),
        Some(Some(TreeValue::GitSubmodule(id))) => id.hex(),
        _ => "0".repeat(width),
    }
}

/// How long ok's object IDs are written: BLAKE3-256, 64 hex digits.
pub const HEX_LEN: usize = 64;

/// The changed paths from one tree to another, renames included, in path order.
pub async fn entries(
    from: &MergedTree,
    to: &MergedTree,
    matcher: &dyn Matcher,
    copies: &CopyRecords,
) -> Result<Vec<Entry>, CommandError> {
    let mut stream = from.diff_stream_with_copies(to, matcher, copies);
    let mut out = Vec::new();
    while let Some(CopiesTreeDiffEntry { path, values }) = stream.next().await {
        let values = values?;
        let status = if let Some(op) = path.copy_operation() {
            match op {
                CopyOperation::Rename => 'R',
                CopyOperation::Copy => 'C',
            }
        } else if !values.after.is_resolved() {
            'U'
        } else {
            match (values.before.is_present(), values.after.is_present()) {
                (false, true) => 'A',
                (true, false) => 'D',
                _ => {
                    let kind = |v: &MergedTreeValue| match v.as_resolved() {
                        Some(Some(TreeValue::Symlink(_))) => 1,
                        Some(Some(TreeValue::GitSubmodule(_))) => 2,
                        _ => 0,
                    };
                    if kind(&values.before) != kind(&values.after) {
                        'T'
                    } else {
                        'M'
                    }
                }
            }
        };
        out.push(Entry {
            status,
            source: path.source().to_owned(),
            target: path.target().to_owned(),
            before: values.before,
            after: values.after,
        });
    }
    Ok(out)
}

/// Whether an entry passes `--diff-filter`: upper-case letters keep those statuses, lower-case
/// ones drop them.
fn passes_filter(filter: &str, status: char) -> bool {
    let includes: Vec<char> = filter.chars().filter(char::is_ascii_uppercase).collect();
    let excludes: Vec<char> = filter
        .chars()
        .filter(char::is_ascii_lowercase)
        .map(|c| c.to_ascii_uppercase())
        .collect();
    (includes.is_empty() || includes.contains(&status)) && !excludes.contains(&status)
}

/// `a/{b => c}/d`: a rename shown as git shows one, its common prefix and suffix outside braces.
pub fn pretty_rename(from: &str, to: &str) -> String {
    let fb = from.as_bytes();
    let tb = to.as_bytes();
    let mut pfx = 0;
    for i in 0..fb.len().min(tb.len()) {
        if fb[i] != tb[i] {
            break;
        }
        if fb[i] == b'/' {
            pfx = i + 1;
        }
    }
    let mut sfx = 0;
    let max_sfx = fb.len().min(tb.len()) - pfx;
    for i in 1..=max_sfx {
        if fb[fb.len() - i] != tb[tb.len() - i] {
            break;
        }
        if fb[fb.len() - i] == b'/' {
            sfx = i;
        }
    }
    if pfx == 0 && sfx == 0 {
        return format!("{from} => {to}");
    }
    let a = &from[pfx..from.len() - sfx];
    let b = &to[pfx..to.len() - sfx];
    format!(
        "{}{{{} => {}}}{}",
        &from[..pfx],
        a,
        b,
        &from[from.len() - sfx..]
    )
}

/// A converter showing every path from the root: diffs name paths from there, wherever they run.
pub fn root_converter(ws: &WorkspaceCommandHelper) -> RepoPathUiConverter {
    RepoPathUiConverter::Fs {
        cwd: ws.workspace_root().to_owned(),
        base: ws.workspace_root().to_owned(),
    }
}

/// A converter showing paths from the current directory, as git's status and listings do.
pub fn cwd_converter(cx: &Cx<'_>, ws: &WorkspaceCommandHelper) -> RepoPathUiConverter {
    RepoPathUiConverter::Fs {
        cwd: cx.command.cwd().to_owned(),
        base: ws.workspace_root().to_owned(),
    }
}

/// Shows the diff from one tree to another as `show` asks; returns whether there was any.
#[allow(clippy::too_many_arguments)]
pub async fn show_diff(
    ui: &Ui,
    cx: &Cx<'_>,
    ws: &WorkspaceCommandHelper,
    repo: &dyn Repo,
    formatter: &mut dyn Formatter,
    trees: (&MergedTree, &MergedTree),
    copies: &CopyRecords,
    matcher: &dyn Matcher,
    show: &DiffShow,
) -> Result<bool, CommandError> {
    let (from, to) = if show.reverse {
        (trees.1, trees.0)
    } else {
        (trees.0, trees.1)
    };
    let mut all = entries(from, to, matcher, copies).await?;
    if let Some(f) = &show.filter {
        all.retain(|e| passes_filter(f, e.status));
    }
    if all.is_empty() {
        return Ok(false);
    }
    let paths: Vec<RepoPathBuf> = all
        .iter()
        .flat_map(|e| [e.source.clone(), e.target.clone()])
        .collect();
    let only = FilesMatcher::new(paths);
    let cwd_prefix = ws.workspace_root().join("").clone();
    let _ = cwd_prefix;
    let converter = if show.relative {
        RepoPathUiConverter::Fs {
            cwd: cx.command.cwd().to_owned(),
            base: ws.workspace_root().to_owned(),
        }
    } else {
        root_converter(ws)
    };
    let shown = |p: &RepoPath| -> String {
        if show.relative {
            converter.format_file_path(p)
        } else {
            p.as_internal_file_string().to_owned()
        }
    };
    let quote = |s: &str| {
        if show.z {
            s.to_owned()
        } else {
            fmt::quote_path(s, cx.quote_high())
        }
    };
    let term = if show.z { '\0' } else { '\n' };
    let marker = ws.env().conflict_marker_style();
    let needs_stats = show.stat || show.numstat || show.shortstat;
    let stats = if needs_stats {
        let options = DiffStatOptions {
            line_diff: LineDiffOptions {
                compare_mode: show.compare,
            },
            max_bar_width: None,
        };
        Some(
            DiffStats::calculate(
                repo.store(),
                from.diff_stream_with_copies(to, &only, copies),
                &options,
                marker,
            )
            .await?,
        )
    } else {
        None
    };
    if show.stat
        && let Some(s) = &stats
    {
        let rows: Vec<StatRow> = s
            .entries()
            .iter()
            .map(|e| StatRow {
                name: match e.path.to_diff() {
                    Some(d) => pretty_rename(&shown(d.before), &shown(d.after)),
                    None => quote(&shown(e.path.target())),
                },
                change: e.added_removed,
            })
            .collect();
        write!(formatter, "{}", git_stat(&rows, 80))?;
        writeln!(
            formatter,
            "{}",
            fmt::shortstat(
                s.entries().len(),
                s.count_total_added(),
                s.count_total_removed()
            )
        )?;
    } else if show.shortstat
        && let Some(s) = &stats
    {
        writeln!(
            formatter,
            "{}",
            fmt::shortstat(
                s.entries().len(),
                s.count_total_added(),
                s.count_total_removed()
            )
        )?;
    }
    if show.numstat
        && let Some(s) = &stats
    {
        for e in s.entries() {
            let (a, r) = match e.added_removed {
                Some((a, r)) => (a.to_string(), r.to_string()),
                None => ("-".to_owned(), "-".to_owned()),
            };
            let path = match e.path.to_diff() {
                Some(d) if show.z => {
                    write!(
                        formatter,
                        "{a}\t{r}\t\0{}\0{}\0",
                        shown(d.before),
                        shown(d.after)
                    )?;
                    continue;
                }
                Some(d) => pretty_rename(&shown(d.before), &shown(d.after)),
                None => quote(&shown(e.path.target())),
            };
            write!(formatter, "{a}\t{r}\t{path}{term}")?;
        }
    }
    if show.name_only {
        for e in &all {
            write!(formatter, "{}{term}", quote(&shown(&e.target)))?;
        }
    }
    if show.name_status {
        for e in &all {
            if e.is_copy() {
                if show.z {
                    write!(
                        formatter,
                        "{}100\0{}\0{}\0",
                        e.status,
                        shown(&e.source),
                        shown(&e.target)
                    )?;
                } else {
                    writeln!(
                        formatter,
                        "{}100\t{}\t{}",
                        e.status,
                        quote(&shown(&e.source)),
                        quote(&shown(&e.target))
                    )?;
                }
            } else if show.z {
                write!(formatter, "{}\0{}\0", e.status, shown(&e.target))?;
            } else {
                writeln!(formatter, "{}\t{}", e.status, quote(&shown(&e.target)))?;
            }
        }
    }
    if show.summary {
        for e in &all {
            match e.status {
                'A' => writeln!(
                    formatter,
                    " create mode {} {}",
                    mode(&e.after),
                    quote(&shown(&e.target))
                )?,
                'D' => writeln!(
                    formatter,
                    " delete mode {} {}",
                    mode(&e.before),
                    quote(&shown(&e.target))
                )?,
                'R' => writeln!(
                    formatter,
                    " rename {} (100%)",
                    pretty_rename(&shown(&e.source), &shown(&e.target))
                )?,
                'C' => writeln!(
                    formatter,
                    " copy {} (100%)",
                    pretty_rename(&shown(&e.source), &shown(&e.target))
                )?,
                _ if mode(&e.before) != mode(&e.after) => writeln!(
                    formatter,
                    " mode change {} => {} {}",
                    mode(&e.before),
                    mode(&e.after),
                    quote(&shown(&e.target))
                )?,
                _ => {}
            }
        }
    }
    if show.patch || show.words {
        if show.stat || show.numstat || show.shortstat || show.summary {
            writeln!(formatter)?;
        }
        let format = if show.words {
            let mut options = ColorWordsDiffOptions::from_settings(cx.command.settings())
                .map_err(jj_cli::command_error::config_error)?;
            options.context = show.context;
            options.line_diff = LineDiffOptions {
                compare_mode: show.compare,
            };
            DiffFormat::ColorWords(Box::new(options))
        } else {
            DiffFormat::Git(Box::new(UnifiedDiffOptions {
                context: show.context,
                show_path_prefix: show.prefix,
                line_diff: LineDiffOptions {
                    compare_mode: show.compare,
                },
            }))
        };
        let renderer = DiffRenderer::new(repo, &converter, marker, vec![format]);
        renderer
            .show_diff(
                ui,
                formatter,
                Diff::new(from, to),
                &only,
                copies,
                ui.term_width(),
            )
            .await?;
    }
    Ok(true)
}

/// A file's line of `--stat`: its name, and lines added and removed (none when binary).
pub struct StatRow {
    pub name: String,
    pub change: Option<(usize, usize)>,
}

/// `--stat` as git draws it within `width` columns: ` name | count +++--`, names padded to the
/// longest, the graph scaled down (git's `scale_linear`) when the largest change would not fit,
/// and names cut from the left (`...`) when even a small graph would not.
pub fn git_stat(rows: &[StatRow], width: usize) -> String {
    let counts: Vec<String> = rows
        .iter()
        .map(|r| match r.change {
            Some((a, d)) => (a + d).to_string(),
            None => "Bin".to_owned(),
        })
        .collect();
    let max_change = rows
        .iter()
        .filter_map(|r| r.change.map(|(a, d)| a + d))
        .max()
        .unwrap_or(0);
    let number_width = counts.iter().map(String::len).max().unwrap_or(1);
    let mut name_width = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    // " " name " | " count " " graph
    let fixed = |name_width: usize| name_width + number_width + 5;
    let mut graph_width = max_change;
    if fixed(name_width) + graph_width > width {
        graph_width = width
            .saturating_sub(fixed(name_width))
            .max(6.min(max_change));
        if fixed(name_width) + graph_width > width {
            name_width = width.saturating_sub(number_width + 5 + graph_width).max(4);
        }
    }
    let scale = |n: usize| -> usize {
        if graph_width >= max_change || n == 0 {
            n
        } else {
            1 + (n * (graph_width - 1)) / max_change
        }
    };
    let mut out = String::new();
    for (r, count) in rows.iter().zip(&counts) {
        let chars = r.name.chars().count();
        let name = if chars > name_width {
            let keep: String = r.name.chars().skip(chars - (name_width - 3)).collect();
            format!("...{keep}")
        } else {
            r.name.clone()
        };
        let pad = name_width.saturating_sub(name.chars().count());
        let graph = match r.change {
            Some((a, d)) => format!("{}{}", "+".repeat(scale(a)), "-".repeat(scale(d))),
            None => String::new(),
        };
        let line = format!(
            " {name}{} | {count:>number_width$} {graph}",
            " ".repeat(pad)
        );
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

/// The trees a commit's diff is from and to, and its copy records.
pub async fn commit_trees(
    repo: &dyn Repo,
    commit: &Commit,
    matcher: &dyn Matcher,
) -> Result<(MergedTree, MergedTree, CopyRecords), CommandError> {
    let from = commit.parent_tree(repo).await?;
    let copies = repo::copy_records(repo, commit.parent_ids(), commit.id(), matcher).await?;
    Ok((from, commit.tree(), copies))
}

// ---------------------------------------------------------------------------------------------
// status

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct StatusArgs {
    #[arg(short = 's', long = "short")]
    short: bool,
    #[arg(short = 'b', long = "branch")]
    branch: bool,
    #[arg(long = "no-branch")]
    no_branch: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "v1")]
    porcelain: Option<String>,
    #[arg(long)]
    long: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long = "untracked-files", num_args = 0..=1, require_equals = true, default_missing_value = "all")]
    untracked_files: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "traditional")]
    ignored: Option<String>,
    #[arg(long = "ignore-submodules", num_args = 0..=1, require_equals = true, default_missing_value = "all")]
    ignore_submodules: Option<String>,
    #[arg(long = "show-stash")]
    show_stash: bool,
    #[arg(long = "ahead-behind")]
    ahead_behind: bool,
    #[arg(long = "no-ahead-behind")]
    no_ahead_behind: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    column: Option<String>,
    #[arg(long = "no-column")]
    no_column: bool,
    #[arg(long)]
    renames: bool,
    #[arg(long = "no-renames")]
    no_renames: bool,
    #[arg(long = "find-renames", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    find_renames: Option<String>,
    #[arg(long = "no-optional-locks")]
    no_optional_locks: bool,
    pathspec: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `-u<mode>` and `-unormal`-style options git attaches to a short flag, which clap cannot read:
/// they say how to show untracked files, and an ok workspace has none.
fn drop_untracked_flags(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut after_dashes = false;
    for a in args {
        if a == "--" {
            after_dashes = true;
        }
        if !after_dashes && a.starts_with("-u") && !a.starts_with("--") {
            continue;
        }
        out.push(a.clone());
    }
    out
}

/// Where HEAD is against `origin/main`: the upstream's name and the counts ahead and behind, if
/// there is an upstream.
pub async fn tracking(
    repo: &dyn Repo,
    head: &Commit,
    unborn: bool,
) -> Result<Option<(usize, usize)>, CommandError> {
    let Some(up) = repo::origin_main(repo) else {
        return Ok(None);
    };
    if up == *repo.store().root_commit_id() {
        return Ok(None);
    }
    if unborn {
        return Ok(Some((0, repo::count_range(repo, head.id(), &up).await?)));
    }
    let ahead = repo::count_range(repo, &up, head.id()).await?;
    let behind = repo::count_range(repo, head.id(), &up).await?;
    Ok(Some((ahead, behind)))
}

fn commits(n: usize) -> String {
    if n == 1 {
        "1 commit".to_owned()
    } else {
        format!("{n} commits")
    }
}

pub async fn status(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let args = drop_untracked_flags(args);
    let a: StatusArgs = parse_or_return!(ui, "status", &args);
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let head = repo::current(repo, &root, st.head.id());
    let mut specs = a.pathspec.clone();
    specs.extend(a.rest.iter().cloned());
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let from = st.wc.parent_tree(repo).await?;
    let copies = if a.no_renames {
        CopyRecords::default()
    } else {
        repo::copy_records(repo, st.wc.parent_ids(), st.wc.id(), &matcher).await?
    };
    let mut changed = entries(&from, &st.wc.tree(), &matcher, &copies).await?;
    let seen: HashSet<RepoPathBuf> = changed.iter().map(|e| e.target.clone()).collect();
    for (path, value) in st.wc.tree().conflicts_matching(&matcher) {
        if !seen.contains(&path) {
            let value = value?;
            changed.push(Entry {
                status: 'U',
                source: path.clone(),
                target: path,
                before: value.clone(),
                after: value,
            });
        }
    }
    changed.sort_by(|a, b| a.target.cmp(&b.target));
    let track = if a.no_ahead_behind {
        None
    } else {
        tracking(repo, &st.head, st.unborn).await?
    };
    let porcelain = a.porcelain.as_deref();
    let z = a.z;
    let short_form = a.short || porcelain.is_some() || (z && !a.long);
    let term = if z { '\0' } else { '\n' };
    let quote_high = cx.quote_high();
    let converter = cwd_converter(cx, &ws);
    let rel = |p: &RepoPath| -> String {
        if porcelain.is_some() {
            p.as_internal_file_string().to_owned()
        } else {
            converter.format_file_path(p)
        }
    };
    let q = |s: String| {
        if z {
            s
        } else {
            fmt::quote_path(&s, quote_high)
        }
    };
    let short_head = repo::short(repo, st.head.id()).await?;
    let mut out = ui.stdout_formatter();
    let out = out.as_mut();
    if porcelain == Some("v2") || porcelain == Some("2") {
        if a.branch {
            let oid = if st.unborn {
                "(initial)".to_owned()
            } else {
                st.head.id().hex()
            };
            write!(out, "# branch.oid {oid}{term}")?;
            write!(
                out,
                "# branch.head {}{term}",
                head.branch().unwrap_or("(detached)")
            )?;
            if let Some((ahead, behind)) = track
                && head.branch().is_some()
            {
                write!(
                    out,
                    "# branch.upstream {}/{}{term}",
                    repo::ORIGIN,
                    repo::MAIN
                )?;
                write!(out, "# branch.ab +{ahead} -{behind}{term}")?;
            }
            if a.show_stash {
                let n = super::change::stashes(repo).await?.len();
                if n > 0 {
                    write!(out, "# stash {n}{term}")?;
                }
            }
        }
        for e in &changed {
            let (mh, mi) = (mode(&e.before), mode(&e.after));
            let (hh, hi) = (
                object_hex(&e.before, HEX_LEN),
                object_hex(&e.after, HEX_LEN),
            );
            match e.status {
                'U' => write!(
                    out,
                    "u UU N... {mi} {mi} {mi} {mi} {hi} {hi} {hi} {}{term}",
                    q(rel(&e.target))
                )?,
                'R' | 'C' => {
                    let sep = if z { '\0' } else { '\t' };
                    write!(
                        out,
                        "2 {}. N... {mh} {mi} {mi} {hh} {hi} {}100 {}{sep}{}{term}",
                        e.status,
                        e.status,
                        q(rel(&e.target)),
                        q(rel(&e.source))
                    )?
                }
                s => write!(
                    out,
                    "1 {s}. N... {mh} {mi} {mi} {hh} {hi} {}{term}",
                    q(rel(&e.target))
                )?,
            }
        }
        return Ok(0);
    }
    if short_form {
        if (a.branch && !a.no_branch) || (a.short && a.branch) {
            let mut line = String::from("## ");
            match (&head, st.unborn) {
                (Head::Branch(b), true) => line.push_str(&format!("No commits yet on {b}")),
                (Head::Branch(b), false) => line.push_str(b),
                (Head::Detached, _) => line.push_str("HEAD (no branch)"),
            }
            if let (Some((ahead, behind)), Head::Branch(_)) = (track, &head) {
                line.push_str(&format!("...{}/{}", repo::ORIGIN, repo::MAIN));
                match (ahead, behind) {
                    (0, 0) => {}
                    (a, 0) => line.push_str(&format!(" [ahead {a}]")),
                    (0, b) => line.push_str(&format!(" [behind {b}]")),
                    (a, b) => line.push_str(&format!(" [ahead {a}, behind {b}]")),
                }
            }
            write!(out, "{line}{term}")?;
        }
        for e in &changed {
            let xy = match e.status {
                'U' => "UU".to_owned(),
                s => format!("{s} "),
            };
            if e.is_copy() {
                if z {
                    write!(out, "{xy} {}\0{}\0", rel(&e.target), rel(&e.source))?;
                } else {
                    let line = format!("{xy} {} -> {}", q(rel(&e.source)), q(rel(&e.target)));
                    if porcelain.is_none() {
                        writeln!(out.labeled("added"), "{line}")?;
                    } else {
                        writeln!(out, "{line}")?;
                    }
                }
            } else {
                let line = format!("{xy} {}", q(rel(&e.target)));
                if porcelain.is_none() && !z {
                    let label = if e.status == 'U' { "conflict" } else { "added" };
                    writeln!(out.labeled(label), "{line}")?;
                } else {
                    write!(out, "{line}{term}")?;
                }
            }
        }
        return Ok(0);
    }

    // The long format.
    match (&head, st.unborn) {
        (Head::Branch(b), _) => writeln!(out, "On branch {b}")?,
        (Head::Detached, _) => writeln!(out, "HEAD detached at {short_head}")?,
    }
    if st.unborn {
        writeln!(out, "\nNo commits yet")?;
    }
    if let (Some((ahead, behind)), Head::Branch(_)) = (track, &head) {
        let up = format!("{}/{}", repo::ORIGIN, repo::MAIN);
        match (ahead, behind) {
            (0, 0) => writeln!(out, "Your branch is up to date with '{up}'.")?,
            (n, 0) => {
                writeln!(out, "Your branch is ahead of '{up}' by {}.", commits(n))?;
                writeln!(
                    out,
                    "  (use \"git push\" to upload your local commits as ChangeLists)"
                )?;
            }
            (0, n) => {
                writeln!(
                    out,
                    "Your branch is behind '{up}' by {}, and can be fast-forwarded.",
                    commits(n)
                )?;
                writeln!(out, "  (use \"git pull\" to update your local branch)")?;
            }
            (a, b) => {
                writeln!(out, "Your branch and '{up}' have diverged,")?;
                writeln!(
                    out,
                    "and have {a} and {b} different commits each, respectively."
                )?;
                writeln!(
                    out,
                    "  (use \"git pull\" to rebase your commits onto '{up}')"
                )?;
            }
        }
        writeln!(out)?;
    } else if st.unborn {
        writeln!(out)?;
    }
    let staged: Vec<&Entry> = changed.iter().filter(|e| e.status != 'U').collect();
    let unmerged: Vec<&Entry> = changed.iter().filter(|e| e.status == 'U').collect();
    if !staged.is_empty() {
        writeln!(out, "Changes to be committed:")?;
        writeln!(
            out,
            "  (ok tracks every file: \"git commit\" commits them all)"
        )?;
        writeln!(out, "  (use \"git restore <file>...\" to discard changes)")?;
        for e in &staged {
            let what = match e.status {
                'A' => "new file:   ",
                'D' => "deleted:    ",
                'R' => "renamed:    ",
                'C' => "copied:     ",
                'T' => "typechange: ",
                _ => "modified:   ",
            };
            let path = if e.is_copy() {
                format!("{} -> {}", q(rel(&e.source)), q(rel(&e.target)))
            } else {
                q(rel(&e.target))
            };
            writeln!(out.labeled("added"), "\t{what}{path}")?;
        }
        writeln!(out)?;
    }
    if !unmerged.is_empty() {
        writeln!(out, "Unmerged paths:")?;
        writeln!(
            out,
            "  (fix conflicts in the working tree, then run \"git commit\")"
        )?;
        for e in &unmerged {
            writeln!(
                out.labeled("conflict"),
                "\tboth modified:   {}",
                q(rel(&e.target))
            )?;
        }
        writeln!(out)?;
    }
    if a.show_stash {
        let n = super::change::stashes(repo).await?.len();
        if n > 0 {
            writeln!(
                out,
                "Your stash currently has {n} entr{}",
                if n == 1 { "y" } else { "ies" }
            )?;
        }
    }
    if changed.is_empty() {
        if st.unborn {
            writeln!(
                out,
                "nothing to commit (create/copy files and run \"git commit\")"
            )?;
        } else {
            writeln!(out, "nothing to commit, working tree clean")?;
        }
    }
    if a.verbose > 0 && !staged.is_empty() {
        let show = DiffShow::from_flags(&DiffFlags::default(), true, false).unwrap();
        show_diff(
            ui,
            cx,
            &ws,
            repo,
            out,
            (&from, &st.wc.tree()),
            &copies,
            &matcher,
            &show,
        )
        .await?;
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// diff

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct DiffArgs {
    #[arg(long, visible_alias = "staged")]
    cached: bool,
    #[arg(long = "merge-base")]
    merge_base: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(long)]
    quiet: bool,
    #[arg(long = "no-index")]
    no_index: bool,
    #[command(flatten)]
    flags: DiffFlags,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// Splits arguments before `--` into revisions and paths as git does: what resolves is a
/// revision, until the first that does not; the rest must be paths in the working tree.
async fn split_revs_paths(
    r: &repo::Resolver<'_>,
    root: &std::path::Path,
    cwd: &std::path::Path,
    args: &[String],
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut revs = Vec::new();
    let mut paths = Vec::new();
    for a in args {
        if !paths.is_empty() {
            paths.push(a.clone());
            continue;
        }
        let is_rev = if let Some((x, y)) = a.split_once("...").or_else(|| a.split_once("..")) {
            let x = if x.is_empty() { "HEAD" } else { x };
            let y = if y.is_empty() { "HEAD" } else { y };
            r.commit_of(x).await.is_ok() && r.commit_of(y).await.is_ok()
        } else {
            r.object(a).await.is_ok()
        };
        if is_rev {
            revs.push(a.clone());
        } else if cwd.join(a).exists() || a.starts_with(':') || a.contains(['*', '?', '[']) {
            paths.push(a.clone());
        } else {
            let _ = root;
            return Err(format!(
                "ambiguous argument '{a}': unknown revision or path not in the working tree.\nUse '--' to separate paths from revisions, like this:\n'git <command> [<revision>...] -- [<file>...]'"
            ));
        }
    }
    Ok((revs, paths))
}

pub async fn diff(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: DiffArgs = parse_or_return!(ui, "diff", args);
    let mut show = match DiffShow::from_flags(&a.flags, true, a.z) {
        Ok(s) => s,
        Err(e) => return fatal(ui, e),
    };
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let (revs, mut paths) =
        match split_revs_paths(&r, ws.workspace_root(), cx.command.cwd(), &a.args).await {
            Ok(x) => x,
            Err(e) => return fatal(ui, e),
        };
    paths.extend(a.paths.iter().cloned());
    let matcher = match cx.pathspec(&ws, &paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let commit = async |s: &str| r.commit_of(if s.is_empty() { "HEAD" } else { s }).await;
    let (from, to, from_ids, to_id): (MergedTree, MergedTree, Vec<CommitId>, CommitId) =
        match revs.as_slice() {
            [] => (
                st.wc.parent_tree(repo).await?,
                st.wc.tree(),
                st.wc.parent_ids().to_vec(),
                st.wc.id().clone(),
            ),
            [one] if one.contains("..") => {
                let symmetric = one.contains("...");
                let (x, y) = if symmetric {
                    one.split_once("...").unwrap()
                } else {
                    one.split_once("..").unwrap()
                };
                let x = match commit(x).await {
                    Ok(c) => c,
                    Err(e) => return rev_error(ui, x, e),
                };
                let y = match commit(y).await {
                    Ok(c) => c,
                    Err(e) => return rev_error(ui, y, e),
                };
                let x = if symmetric {
                    match merge_base(repo, &[x.id().clone(), y.id().clone()]).await? {
                        Some(b) => b,
                        None => return fatal(ui, format!("{one}: no merge base")),
                    }
                } else {
                    x
                };
                (x.tree(), y.tree(), vec![x.id().clone()], y.id().clone())
            }
            [one] => {
                let x = match commit(one).await {
                    Ok(c) => c,
                    Err(e) => return rev_error(ui, one, e),
                };
                let x = if a.merge_base {
                    let head = match commit("HEAD").await {
                        Ok(c) => c,
                        Err(e) => return rev_error(ui, "HEAD", e),
                    };
                    match merge_base(repo, &[x.id().clone(), head.id().clone()]).await? {
                        Some(b) => b,
                        None => return fatal(ui, format!("{one}: no merge base")),
                    }
                } else {
                    x
                };
                (
                    x.tree(),
                    st.wc.tree(),
                    vec![x.id().clone()],
                    st.wc.id().clone(),
                )
            }
            [x, y] => {
                let x = match commit(x).await {
                    Ok(c) => c,
                    Err(e) => return rev_error(ui, x, e),
                };
                let y = match commit(y).await {
                    Ok(c) => c,
                    Err(e) => return rev_error(ui, y, e),
                };
                let x = if a.merge_base {
                    match merge_base(repo, &[x.id().clone(), y.id().clone()]).await? {
                        Some(b) => b,
                        None => return fatal(ui, "no merge base"),
                    }
                } else {
                    x
                };
                (x.tree(), y.tree(), vec![x.id().clone()], y.id().clone())
            }
            _ => return fatal(ui, "ok shows no combined diffs of more than two commits"),
        };
    let copies = if a.flags.no_renames {
        CopyRecords::default()
    } else {
        repo::copy_records(repo, &from_ids, &to_id, &matcher).await?
    };
    if a.quiet {
        show = DiffShow {
            patch: false,
            words: false,
            stat: false,
            numstat: false,
            shortstat: false,
            name_only: false,
            name_status: false,
            summary: false,
            ..show
        };
    }
    let mut ui_paged = false;
    if show.patch || show.words || show.stat {
        ui.request_pager();
        ui_paged = true;
    }
    let _ = ui_paged;
    let mut out = ui.stdout_formatter();
    let any = show_diff(
        ui,
        cx,
        &ws,
        repo,
        out.as_mut(),
        (&from, &to),
        &copies,
        &matcher,
        &show,
    )
    .await?;
    Ok(if (a.flags.exit_code || a.quiet) && any {
        1
    } else {
        0
    })
}

/// The best common ancestor of commits, if they have one.
pub async fn merge_base(repo: &dyn Repo, ids: &[CommitId]) -> Result<Option<Commit>, CommandError> {
    let Some((first, rest)) = ids.split_first() else {
        return Ok(None);
    };
    let mut bases = vec![first.clone()];
    for id in rest {
        bases = repo
            .index()
            .common_ancestors(&bases, std::slice::from_ref(id))
            .await?;
        if bases.is_empty() {
            return Ok(None);
        }
    }
    bases.sort();
    match bases.first() {
        Some(b) if b != repo.store().root_commit_id() => {
            Ok(Some(repo.store().get_commit_async(b).await?))
        }
        _ => Ok(None),
    }
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct DiffIndexArgs {
    #[arg(long)]
    cached: bool,
    #[arg(long)]
    quiet: bool,
    #[arg(short = 'm')]
    m: bool,
    #[arg(short = 'r')]
    r: bool,
    #[arg(short = 'z')]
    z: bool,
    #[command(flatten)]
    flags: DiffFlags,
    tree: String,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git diff-index <tree-ish>`: the tree against the working tree (the index is the working
/// tree), as name-status unless the flags say otherwise.
pub async fn diff_index(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: DiffIndexArgs = parse_or_return!(ui, "diff-index", args);
    let mut show = match DiffShow::from_flags(&a.flags, false, a.z) {
        Ok(s) => s,
        Err(e) => return fatal(ui, e),
    };
    if !show.shows_anything() && !a.quiet {
        show.name_status = true;
    }
    if a.quiet {
        show = DiffShow::from_flags(
            &DiffFlags {
                no_patch: true,
                ..DiffFlags::default()
            },
            false,
            a.z,
        )
        .unwrap();
    }
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let base = match r.commit_of(&a.tree).await {
        Ok(c) => c,
        Err(e) => return rev_error(ui, &a.tree, e),
    };
    let mut specs = a.paths.clone();
    specs.extend(a.rest.iter().cloned());
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let copies = repo::copy_records(repo, &[base.id().clone()], st.wc.id(), &matcher).await?;
    let mut out = ui.stdout_formatter();
    let any = show_diff(
        ui,
        cx,
        &ws,
        repo,
        out.as_mut(),
        (&base.tree(), &st.wc.tree()),
        &copies,
        &matcher,
        &show,
    )
    .await?;
    Ok(if (a.flags.exit_code || a.quiet) && any {
        1
    } else {
        0
    })
}

/// `git diff-files`: the index against the working tree, which are one in an ok workspace —
/// never a difference.
pub async fn diff_files(ui: &mut Ui, _cx: &Cx<'_>, args: &[String]) -> Code {
    let _ = ui;
    let _ = args;
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct DiffTreeArgs {
    #[arg(short = 'r')]
    r: bool,
    #[arg(short = 't')]
    t: bool,
    #[arg(long = "no-commit-id")]
    no_commit_id: bool,
    #[arg(long = "root")]
    root: bool,
    #[arg(short = 'm')]
    m: bool,
    #[arg(long = "cc")]
    cc: bool,
    #[arg(short = 'c')]
    c: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(long)]
    quiet: bool,
    #[arg(long)]
    stdin: bool,
    #[arg(short = 'v')]
    v: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "medium")]
    pretty: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[command(flatten)]
    flags: DiffFlags,
    trees: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// `git diff-tree <commit>` (against its parents) or `<a> <b>`, as name-status unless the flags
/// say otherwise; with one commit, its ID first unless `--no-commit-id`.
pub async fn diff_tree(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: DiffTreeArgs = parse_or_return!(ui, "diff-tree", args);
    let mut show = match DiffShow::from_flags(&a.flags, false, a.z) {
        Ok(s) => s,
        Err(e) => return fatal(ui, e),
    };
    if !show.shows_anything() {
        show.name_status = true;
    }
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    let matcher = match cx.pathspec(&ws, &a.paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let mut out = ui.stdout_formatter();
    let term = if a.z { '\0' } else { '\n' };
    let any;
    match a.trees.as_slice() {
        [one] => {
            let c = match r.commit_of(one).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, one, e),
            };
            let (from, to, copies) = commit_trees(repo, &c, &matcher).await?;
            let mut buf: Vec<u8> = Vec::new();
            {
                let mut f = ui.new_formatter(&mut buf);
                any = show_diff(
                    ui,
                    cx,
                    &ws,
                    repo,
                    f.as_mut(),
                    (&from, &to),
                    &copies,
                    &matcher,
                    &show,
                )
                .await?;
            }
            if any && !a.quiet {
                if !a.no_commit_id {
                    write!(out, "{}{term}", c.id().hex())?;
                }
                out.write_all(&buf)?;
            }
        }
        [x, y] => {
            let x = match r.commit_of(x).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, x, e),
            };
            let y = match r.commit_of(y).await {
                Ok(c) => c,
                Err(e) => return rev_error(ui, y, e),
            };
            let copies = repo::copy_records(repo, &[x.id().clone()], y.id(), &matcher).await?;
            if a.quiet {
                any = !entries(&x.tree(), &y.tree(), &matcher, &copies)
                    .await?
                    .is_empty();
            } else {
                any = show_diff(
                    ui,
                    cx,
                    &ws,
                    repo,
                    out.as_mut(),
                    (&x.tree(), &y.tree()),
                    &copies,
                    &matcher,
                    &show,
                )
                .await?;
            }
        }
        _ => return fatal(ui, "git diff-tree takes one commit, or two trees"),
    }
    Ok(if (a.flags.exit_code || a.quiet) && any {
        1
    } else {
        0
    })
}

// ---------------------------------------------------------------------------------------------
// log, show, rev-list

/// git's commit formats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pretty {
    Oneline,
    Short,
    Medium,
    Full,
    Fuller,
    Reference,
    Raw,
    /// A format string; `terminator` for `tformat:` (a newline after each), else a separator.
    Format {
        format: String,
        terminator: bool,
    },
}

impl Pretty {
    pub fn parse(s: &str) -> Self {
        match s {
            "oneline" => Pretty::Oneline,
            "short" => Pretty::Short,
            "medium" | "" => Pretty::Medium,
            "full" => Pretty::Full,
            "fuller" => Pretty::Fuller,
            "reference" => Pretty::Reference,
            "raw" => Pretty::Raw,
            "email" | "mboxrd" => Pretty::Medium,
            _ => {
                if let Some(f) = s.strip_prefix("format:") {
                    Pretty::Format {
                        format: f.to_owned(),
                        terminator: false,
                    }
                } else if let Some(f) = s.strip_prefix("tformat:") {
                    Pretty::Format {
                        format: f.to_owned(),
                        terminator: true,
                    }
                } else {
                    Pretty::Format {
                        format: s.to_owned(),
                        terminator: true,
                    }
                }
            }
        }
    }
}

/// Every ref by the commit it names, as `--decorate` shows them: `HEAD -> branch`, `tag: …`,
/// branches, and `origin/…`.
pub fn decorations(
    repo: &dyn Repo,
    head: &Head,
    head_id: Option<&CommitId>,
    full: bool,
) -> HashMap<CommitId, Vec<String>> {
    let mut map: HashMap<CommitId, Vec<String>> = HashMap::new();
    let (bh, rh, th) = if full {
        ("refs/heads/", "refs/remotes/", "tag: refs/tags/")
    } else {
        ("", "", "tag: ")
    };
    let current = head.branch().map(str::to_owned);
    if let Some(id) = head_id {
        let label = match &current {
            Some(b) if repo::branch_target(repo, b).as_ref() == Some(id) => {
                format!("HEAD -> {bh}{b}")
            }
            _ => "HEAD".to_owned(),
        };
        map.entry(id.clone()).or_default().push(label);
    }
    for (name, id) in repo::tags(repo) {
        if let Some(id) = id {
            map.entry(id).or_default().push(format!("{th}{name}"));
        }
    }
    for (name, id) in repo::branches(repo) {
        if Some(&name) == current.as_ref() && repo::branch_target(repo, &name).as_ref() == head_id {
            continue;
        }
        if let Some(id) = id {
            map.entry(id).or_default().push(format!("{bh}{name}"));
        }
    }
    for (name, id) in repo::remote_branches(repo) {
        if id == *repo.store().root_commit_id() {
            continue;
        }
        map.entry(id)
            .or_default()
            .push(format!("{rh}{}/{name}", repo::ORIGIN));
    }
    map
}

/// One commit in a format.
#[allow(clippy::too_many_arguments)]
pub async fn render_commit(
    repo: &dyn Repo,
    c: &Commit,
    pretty: &Pretty,
    abbrev: bool,
    deco: &[String],
    date_mode: &fmt::DateMode,
) -> Result<String, CommandError> {
    let short = repo::short(repo, c.id()).await?;
    let id = if abbrev { short.clone() } else { c.id().hex() };
    let mut parents_short = Vec::new();
    for p in c.parent_ids() {
        if p != repo.store().root_commit_id() {
            parents_short.push(repo::short(repo, p).await?);
        }
    }
    let deco_text = if deco.is_empty() {
        String::new()
    } else {
        format!(" ({})", deco.join(", "))
    };
    let merge_line = if c.parent_ids().len() > 1 {
        format!("Merge: {}\n", parents_short.join(" "))
    } else {
        String::new()
    };
    let author = c.author();
    let committer = c.committer();
    let tree_hex = c
        .tree_ids()
        .as_resolved()
        .map(|t| t.hex())
        .unwrap_or_else(|| "0".repeat(HEX_LEN));
    Ok(match pretty {
        Pretty::Oneline => format!("{id}{deco_text} {}\n", fmt::subject(c.description())),
        Pretty::Short => format!(
            "commit {id}{deco_text}\n{merge_line}Author: {}\n\n{}",
            fmt::ident(author),
            short_message(c.description())
        ),
        Pretty::Medium => format!(
            "commit {id}{deco_text}\n{merge_line}Author: {}\nDate:   {}\n\n{}",
            fmt::ident(author),
            fmt::date(&author.timestamp, date_mode),
            fmt::indented(c.description())
        ),
        Pretty::Full => format!(
            "commit {id}{deco_text}\n{merge_line}Author: {}\nCommit: {}\n\n{}",
            fmt::ident(author),
            fmt::ident(committer),
            fmt::indented(c.description())
        ),
        Pretty::Fuller => format!(
            "commit {id}{deco_text}\n{merge_line}Author:     {}\nAuthorDate: {}\nCommit:     {}\nCommitDate: {}\n\n{}",
            fmt::ident(author),
            fmt::date(&author.timestamp, date_mode),
            fmt::ident(committer),
            fmt::date(&committer.timestamp, date_mode),
            fmt::indented(c.description())
        ),
        Pretty::Reference => format!(
            "{short} ({}, {})\n",
            fmt::subject(c.description()),
            fmt::date(&author.timestamp, &fmt::DateMode::Short { local: false })
        ),
        Pretty::Raw => {
            let mut s = format!("commit {}\ntree {tree_hex}\n", c.id().hex());
            for p in c.parent_ids() {
                if p != repo.store().root_commit_id() {
                    s.push_str(&format!("parent {}\n", p.hex()));
                }
            }
            s.push_str(&format!(
                "author {} {} {}\ncommitter {} {} {}\n\n{}",
                fmt::ident(author),
                fmt::unix(&author.timestamp),
                fmt::zone(&author.timestamp),
                fmt::ident(committer),
                fmt::unix(&committer.timestamp),
                fmt::zone(&committer.timestamp),
                fmt::indented(c.description())
            ));
            s
        }
        Pretty::Format { format, .. } => fmt::expand(
            format,
            c,
            &fmt::Extra {
                short: &short,
                parents_short: &parents_short,
                tree: tree_hex,
                decorations: deco,
                date_mode,
            },
        ),
    })
}

fn short_message(description: &str) -> String {
    format!("    {}\n", fmt::subject(description))
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct LogArgs {
    #[arg(short = 'n', long = "max-count")]
    max_count: Option<usize>,
    #[arg(long)]
    skip: Option<usize>,
    #[arg(long)]
    oneline: bool,
    #[arg(long, visible_alias = "pretty", num_args = 0..=1, require_equals = true, default_missing_value = "medium")]
    format: Option<String>,
    #[arg(long = "abbrev-commit")]
    abbrev_commit: bool,
    #[arg(long = "no-abbrev-commit")]
    no_abbrev_commit: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "short")]
    decorate: Option<String>,
    #[arg(long = "no-decorate")]
    no_decorate: bool,
    #[arg(long = "decorate-refs")]
    decorate_refs: Vec<String>,
    #[arg(long = "decorate-refs-exclude")]
    decorate_refs_exclude: Vec<String>,
    #[arg(long)]
    graph: bool,
    #[arg(long)]
    author: Vec<String>,
    #[arg(long)]
    committer: Vec<String>,
    #[arg(long)]
    grep: Vec<String>,
    #[arg(long = "all-match")]
    all_match: bool,
    #[arg(long = "invert-grep")]
    invert_grep: bool,
    #[arg(short = 'i', long = "regexp-ignore-case")]
    ignore_case: bool,
    #[arg(short = 'E', long = "extended-regexp")]
    extended: bool,
    #[arg(short = 'F', long = "fixed-strings")]
    fixed: bool,
    #[arg(long, visible_alias = "after")]
    since: Option<String>,
    #[arg(long, visible_alias = "before")]
    until: Option<String>,
    #[arg(long)]
    reverse: bool,
    #[arg(long = "first-parent")]
    first_parent: bool,
    #[arg(long)]
    merges: bool,
    #[arg(long = "no-merges")]
    no_merges: bool,
    #[arg(long = "min-parents")]
    min_parents: Option<usize>,
    #[arg(long = "max-parents")]
    max_parents: Option<usize>,
    #[arg(long)]
    all: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    branches: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    tags: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    remotes: Option<String>,
    #[arg(long)]
    date: Option<String>,
    #[arg(long = "relative-date")]
    relative_date: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(long)]
    follow: bool,
    #[arg(long = "no-follow")]
    no_follow: bool,
    #[arg(long = "topo-order")]
    topo_order: bool,
    #[arg(long = "date-order")]
    date_order: bool,
    #[arg(long = "author-date-order")]
    author_date_order: bool,
    #[arg(long = "full-history")]
    full_history: bool,
    #[arg(long = "simplify-merges")]
    simplify_merges: bool,
    #[arg(long = "no-walk", num_args = 0..=1, require_equals = true, default_missing_value = "sorted")]
    no_walk: Option<String>,
    #[arg(long = "use-mailmap")]
    use_mailmap: bool,
    #[arg(long = "no-mailmap")]
    no_mailmap: bool,
    #[arg(long = "show-signature")]
    show_signature: bool,
    #[arg(long = "show-notes", num_args = 0..=1, require_equals = true)]
    show_notes: Option<String>,
    #[arg(long = "no-notes")]
    no_notes: bool,
    #[arg(long)]
    source: bool,
    #[arg(long = "left-right")]
    left_right: bool,
    #[arg(long)]
    boundary: bool,
    #[arg(short = 'q', long = "quiet")]
    quiet: bool,
    #[arg(long = "log-size")]
    log_size: bool,
    #[command(flatten)]
    flags: DiffFlags,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// `-<n>` as `--max-count=<n>`, which clap cannot read.
fn numeric_counts(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut after_dashes = false;
    for a in args {
        if a == "--" {
            after_dashes = true;
        }
        if !after_dashes
            && a.len() > 1
            && a.starts_with('-')
            && a[1..].bytes().all(|b| b.is_ascii_digit())
        {
            out.push(format!("--max-count={}", &a[1..]));
        } else {
            out.push(a.clone());
        }
    }
    out
}

/// A `--since`/`--until` date: an ISO date or time, a Unix time (`@<n>`), or `<n> <unit>s ago`.
pub fn approxidate(s: &str) -> Option<i64> {
    let s = s.trim();
    let now = chrono::Utc::now().timestamp();
    if let Some(n) = s.strip_prefix('@') {
        return n.parse().ok();
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    let words: Vec<&str> = s
        .split(|c: char| c.is_whitespace() || c == '.')
        .filter(|w| !w.is_empty())
        .collect();
    match words.as_slice() {
        ["now"] => return Some(now),
        ["yesterday"] => return Some(now - 86400),
        [n, unit, "ago"] | [n, unit] => {
            if let Ok(n) = n.parse::<i64>() {
                let unit = unit.trim_end_matches('s');
                let secs = match unit {
                    "second" | "sec" => 1,
                    "minute" | "min" => 60,
                    "hour" => 3600,
                    "day" => 86400,
                    "week" => 7 * 86400,
                    "month" => 30 * 86400,
                    "year" => 365 * 86400,
                    _ => return None,
                };
                return Some(now - n * secs);
            }
        }
        _ => {}
    }
    for f in [
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%z",
    ] {
        if let Ok(d) = chrono::DateTime::parse_from_str(s, f) {
            return Some(d.timestamp());
        }
    }
    for f in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(s, f) {
            return d
                .and_local_timezone(chrono::Local)
                .single()
                .map(|d| d.timestamp());
        }
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d
            .and_hms_opt(0, 0, 0)?
            .and_local_timezone(chrono::Local)
            .single()
            .map(|d| d.timestamp());
    }
    None
}

/// A pattern of `--grep`, `--author`, `--committer`.
fn pattern(p: &str, icase: bool, fixed: bool) -> Result<regex::Regex, String> {
    let p = if fixed {
        regex::escape(p)
    } else {
        p.to_owned()
    };
    regex::RegexBuilder::new(&p)
        .case_insensitive(icase)
        .build()
        .map_err(|e| e.to_string())
}

pub async fn log(ui: &mut Ui, cx: &Cx<'_>, name: &str, args: &[String]) -> Code {
    let args = numeric_counts(args);
    let a: LogArgs = parse_or_return!(ui, name, &args);
    let show = match DiffShow::from_flags(&a.flags, name == "whatchanged", a.z) {
        Ok(mut s) => {
            if name == "whatchanged" && !a.flags.patch {
                s.patch = false;
                s.name_status = true;
            }
            s
        }
        Err(e) => return fatal(ui, e),
    };
    let to_tty = std::io::stdout().is_terminal();
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let head = repo::current(repo, &root, st.head.id());
    let r = cx.resolver(&ws).await?;
    let (revs, mut paths) = match split_revs_paths(&r, &root, cx.command.cwd(), &a.args).await {
        Ok(x) => x,
        Err(e) => return fatal(ui, e),
    };
    paths.extend(a.paths.iter().cloned());
    let matcher = match cx.pathspec(&ws, &paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let mut walk = match repo::walk(&r, &revs, false).await {
        Ok(w) => w,
        Err((spec, e)) => return rev_error(ui, &spec, e),
    };
    let mut refs_given = false;
    if a.all || a.branches.is_some() {
        walk.include
            .extend(repo::branches(repo).into_iter().filter_map(|(_, id)| id));
        refs_given = true;
    }
    if a.all || a.tags.is_some() {
        walk.include
            .extend(repo::tags(repo).into_iter().filter_map(|(_, id)| id));
        refs_given = true;
    }
    if a.all || a.remotes.is_some() {
        walk.include
            .extend(repo::remote_branches(repo).into_iter().map(|(_, id)| id));
        refs_given = true;
    }
    if a.all && !st.unborn {
        walk.include.push(st.head.id().clone());
    }
    if walk.include.is_empty() && !refs_given {
        if st.unborn {
            return fatal(
                ui,
                format!(
                    "your current branch '{}' does not have any commits yet",
                    head.branch().unwrap_or("HEAD")
                ),
            );
        }
        walk.include.push(st.head.id().clone());
    }
    let expression = if a.first_parent {
        let mut set = RevsetExpression::commits(walk.include.clone()).first_ancestors();
        if !walk.exclude.is_empty() {
            set = set.minus(&RevsetExpression::commits(walk.exclude.clone()).ancestors());
        }
        set.minus(&RevsetExpression::root())
    } else {
        walk.expression(repo)
    };
    let icase = a.ignore_case;
    let greps: Vec<regex::Regex> = match a.grep.iter().map(|p| pattern(p, icase, a.fixed)).collect()
    {
        Ok(v) => v,
        Err(e) => return fatal(ui, format!("invalid --grep pattern: {e}")),
    };
    let authors: Vec<regex::Regex> = match a
        .author
        .iter()
        .map(|p| pattern(p, icase, a.fixed))
        .collect()
    {
        Ok(v) => v,
        Err(e) => return fatal(ui, format!("invalid --author pattern: {e}")),
    };
    let committers: Vec<regex::Regex> = match a
        .committer
        .iter()
        .map(|p| pattern(p, icase, a.fixed))
        .collect()
    {
        Ok(v) => v,
        Err(e) => return fatal(ui, format!("invalid --committer pattern: {e}")),
    };
    let since = a.since.as_deref().map(approxidate);
    let until = a.until.as_deref().map(approxidate);
    if since == Some(None) || until == Some(None) {
        return fatal(ui, "a --since or --until date ok cannot read");
    }
    let (since, until) = (since.flatten(), until.flatten());
    let pretty = if a.oneline {
        Pretty::Oneline
    } else {
        a.format
            .as_deref()
            .map(Pretty::parse)
            .unwrap_or(Pretty::Medium)
    };
    let abbrev = (a.oneline || a.abbrev_commit) && !a.no_abbrev_commit;
    let date_mode = if a.relative_date {
        fmt::DateMode::Relative
    } else {
        match a.date.as_deref() {
            Some(d) => match fmt::DateMode::parse(d) {
                Some(m) => m,
                None => return fatal(ui, format!("unknown date format {d}")),
            },
            None => fmt::DateMode::Default { local: false },
        }
    };
    let decorate = match (a.no_decorate, a.decorate.as_deref()) {
        (true, _) => None,
        (_, Some("no")) => None,
        (_, Some("full")) => Some(true),
        (_, Some(_)) => Some(false),
        (_, None) => {
            let auto = cx.config("log.decorate");
            match auto.as_deref() {
                Some("full") => Some(true),
                Some("short") | Some("true") => Some(false),
                Some("no") | Some("false") => None,
                _ if to_tty => Some(false),
                _ => None,
            }
        }
    };
    let deco_map = match decorate {
        Some(full) => decorations(repo, &head, (!st.unborn).then(|| st.head.id()), full),
        None => HashMap::new(),
    };
    let mut ids: Vec<CommitId> = Vec::new();
    {
        let mut stream = expression
            .evaluate(repo)
            .map_err(jj_cli::command_error::internal_error)?
            .stream();
        let mut skipped = 0;
        let max = a.max_count.unwrap_or(usize::MAX);
        while let Some(id) = stream.next().await {
            let id = id.map_err(jj_cli::command_error::internal_error)?;
            if !a.reverse && ids.len() >= max {
                break;
            }
            let c = repo.store().get_commit_async(&id).await?;
            let parents = c
                .parent_ids()
                .iter()
                .filter(|p| *p != repo.store().root_commit_id())
                .count();
            if a.merges && parents < 2 {
                continue;
            }
            if a.no_merges && parents > 1 {
                continue;
            }
            if a.min_parents.is_some_and(|m| parents < m)
                || a.max_parents.is_some_and(|m| parents > m)
            {
                continue;
            }
            let t = fmt::unix(&c.committer().timestamp);
            if since.is_some_and(|s| t < s) || until.is_some_and(|u| t > u) {
                continue;
            }
            if !authors.is_empty() && !authors.iter().any(|p| p.is_match(&fmt::ident(c.author()))) {
                continue;
            }
            if !committers.is_empty()
                && !committers
                    .iter()
                    .any(|p| p.is_match(&fmt::ident(c.committer())))
            {
                continue;
            }
            if !greps.is_empty() {
                let hit = if a.all_match {
                    greps.iter().all(|p| p.is_match(c.description()))
                } else {
                    greps.iter().any(|p| p.is_match(c.description()))
                };
                if hit == a.invert_grep {
                    continue;
                }
            }
            if !matcher.is_everything() && !repo::touches(repo, &c, &matcher).await? {
                continue;
            }
            if let Some(n) = a.skip
                && skipped < n
            {
                skipped += 1;
                continue;
            }
            ids.push(id);
        }
    }
    if a.reverse {
        ids.reverse();
        if let Some(m) = a.max_count {
            ids.truncate(m);
        }
    }
    if a.graph && a.reverse {
        return fatal(
            ui,
            "options '--reverse' and '--graph' cannot be used together",
        );
    }
    let sep = if a.z { "\0" } else { "\n" };
    let multi_line = matches!(
        pretty,
        Pretty::Short | Pretty::Medium | Pretty::Full | Pretty::Fuller | Pretty::Raw
    );
    // Each commit's entry: its text in the format, then its diff if one was asked for.
    let mut entries: Vec<(CommitId, Vec<u8>)> = Vec::with_capacity(ids.len());
    for id in &ids {
        let c = repo.store().get_commit_async(id).await?;
        let deco = deco_map.get(id).cloned().unwrap_or_default();
        let text = render_commit(repo, &c, &pretty, abbrev, &deco, &date_mode).await?;
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut f = ui.new_formatter(&mut buf);
            match text.find(' ').filter(|_| text.starts_with("commit ")) {
                Some(end) => {
                    write!(f.labeled("commit_id"), "{}", &text[..end])?;
                    write!(f, "{}", &text[end..])?;
                }
                None => write!(f, "{text}")?,
            }
            if let Pretty::Format {
                terminator: true, ..
            } = &pretty
            {
                write!(f, "{sep}")?;
            }
            let merge_hidden = c.parent_ids().len() > 1 && !a.flags.patch;
            if show.shows_anything() && !a.quiet && !merge_hidden {
                let (from, to, copies) = commit_trees(repo, &c, &matcher).await?;
                let mut diff: Vec<u8> = Vec::new();
                {
                    let mut d = ui.new_formatter(&mut diff);
                    show_diff(
                        ui,
                        cx,
                        &ws,
                        repo,
                        d.as_mut(),
                        (&from, &to),
                        &copies,
                        &matcher,
                        &show,
                    )
                    .await?;
                }
                if !diff.is_empty() {
                    if multi_line {
                        writeln!(f)?;
                    }
                    f.write_all(&diff)?;
                }
            }
        }
        entries.push((id.clone(), buf));
    }
    ui.request_pager();
    let mut out = ui.stdout_formatter();
    let out = out.as_mut();
    if a.graph {
        // git's graph, drawn by jj's renderer in its ASCII style: edges between the commits
        // shown, an elided stretch as an indirect edge.
        let by_id: HashMap<CommitId, Vec<u8>> = entries.into_iter().collect();
        let set = RevsetExpression::commits(ids.clone())
            .evaluate(repo)
            .map_err(jj_cli::command_error::internal_error)?;
        let mut stream = set.stream_graph();
        let mut drawn: Vec<u8> = Vec::new();
        {
            let mut graph =
                jj_cli::graphlog::get_graphlog(jj_cli::graphlog::GraphStyle::Ascii, &mut drawn);
            while let Some(node) = stream.next().await {
                let (id, edges) = node.map_err(jj_cli::command_error::internal_error)?;
                let mut text = String::from_utf8_lossy(&by_id[&id]).into_owned();
                if multi_line && !text.ends_with("\n\n") {
                    text.push('\n');
                }
                graph.add_node(&id, &edges, "*", &text)?;
            }
        }
        out.write_all(&drawn)?;
        return Ok(0);
    }
    for (i, (_, buf)) in entries.iter().enumerate() {
        if i > 0 {
            match &pretty {
                Pretty::Format {
                    terminator: false, ..
                } => write!(out, "{sep}")?,
                _ if multi_line => write!(out, "{sep}")?,
                _ => {}
            }
        }
        out.write_all(buf)?;
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct ShowArgs {
    #[arg(long)]
    oneline: bool,
    #[arg(long, visible_alias = "pretty", num_args = 0..=1, require_equals = true, default_missing_value = "medium")]
    format: Option<String>,
    #[arg(long = "abbrev-commit")]
    abbrev_commit: bool,
    #[arg(long = "no-abbrev-commit")]
    no_abbrev_commit: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "short")]
    decorate: Option<String>,
    #[arg(long = "no-decorate")]
    no_decorate: bool,
    #[arg(long)]
    date: Option<String>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(long = "show-signature")]
    show_signature: bool,
    #[arg(short = 'm')]
    m: bool,
    #[arg(long = "cc")]
    cc: bool,
    #[arg(long = "first-parent")]
    first_parent: bool,
    #[arg(long = "expand-tabs", num_args = 0..=1, require_equals = true)]
    expand_tabs: Option<String>,
    #[arg(long = "no-expand-tabs")]
    no_expand_tabs: bool,
    #[command(flatten)]
    flags: DiffFlags,
    objects: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

pub async fn show(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: ShowArgs = parse_or_return!(ui, "show", args);
    let mut show = match DiffShow::from_flags(&a.flags, true, a.z) {
        Ok(s) => s,
        Err(e) => return fatal(ui, e),
    };
    if a.quiet {
        show.patch = false;
        show.words = false;
    }
    let to_tty = std::io::stdout().is_terminal();
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let root = ws.workspace_root().to_owned();
    let head = repo::current(repo, &root, st.head.id());
    let r = cx.resolver(&ws).await?;
    let matcher = match cx.pathspec(&ws, &a.paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let objects = if a.objects.is_empty() {
        vec!["HEAD".to_owned()]
    } else {
        a.objects.clone()
    };
    let pretty = if a.oneline {
        Pretty::Oneline
    } else {
        a.format
            .as_deref()
            .map(Pretty::parse)
            .unwrap_or(Pretty::Medium)
    };
    let abbrev = (a.oneline || a.abbrev_commit) && !a.no_abbrev_commit;
    let date_mode = match a.date.as_deref() {
        Some(d) => match fmt::DateMode::parse(d) {
            Some(m) => m,
            None => return fatal(ui, format!("unknown date format {d}")),
        },
        None => fmt::DateMode::Default { local: false },
    };
    let deco_map = if !a.no_decorate && (a.decorate.is_some() || to_tty) {
        decorations(
            repo,
            &head,
            (!st.unborn).then(|| st.head.id()),
            a.decorate.as_deref() == Some("full"),
        )
    } else {
        HashMap::new()
    };
    let mut resolved = Vec::new();
    for o in &objects {
        match r.object(o).await {
            Ok(obj) => resolved.push((o.clone(), obj)),
            Err(e) => return rev_error(ui, o, e),
        }
    }
    ui.request_pager();
    let mut out = ui.stdout_formatter();
    let out = out.as_mut();
    for (i, (name, obj)) in resolved.iter().enumerate() {
        match obj {
            Object::Commit(c) => {
                if i > 0 {
                    writeln!(out)?;
                }
                let deco = deco_map.get(c.id()).cloned().unwrap_or_default();
                let text = render_commit(repo, c, &pretty, abbrev, &deco, &date_mode).await?;
                if text.starts_with("commit ") {
                    let end = text.find(' ').unwrap_or(text.len());
                    let line_end = text.find('\n').unwrap_or(text.len());
                    write!(out.labeled("commit_id"), "{}", &text[..end])?;
                    write!(out.labeled("commit_id"), "{}", &text[end..line_end])?;
                    write!(out, "{}", &text[line_end..])?;
                } else {
                    write!(out, "{text}")?;
                }
                if let Pretty::Format {
                    terminator: true, ..
                } = &pretty
                {
                    writeln!(out)?;
                }
                if show.shows_anything() {
                    let (from, to, copies) = commit_trees(repo, c, &matcher).await?;
                    let mut buf: Vec<u8> = Vec::new();
                    {
                        let mut f = ui.new_formatter(&mut buf);
                        show_diff(
                            ui,
                            cx,
                            &ws,
                            repo,
                            f.as_mut(),
                            (&from, &to),
                            &copies,
                            &matcher,
                            &show,
                        )
                        .await?;
                    }
                    if !buf.is_empty() {
                        if !matches!(
                            pretty,
                            Pretty::Oneline | Pretty::Format { .. } | Pretty::Reference
                        ) {
                            writeln!(out)?;
                        }
                        out.write_all(&buf)?;
                    }
                }
            }
            Object::Tree { path, root, .. } => {
                writeln!(out, "tree {name}\n")?;
                for (entry, is_dir) in list_dir(root, path).await? {
                    writeln!(out, "{entry}{}", if is_dir { "/" } else { "" })?;
                }
            }
            Object::File { path, value, .. } => {
                let bytes = read_value(repo, path, value).await?;
                out.write_all(&bytes)?;
            }
        }
    }
    Ok(0)
}

/// A directory's entries, one level: names, and whether each is a directory.
pub async fn list_dir(
    root: &MergedTree,
    dir: &RepoPath,
) -> Result<Vec<(String, bool)>, CommandError> {
    let mut seen: Vec<(String, bool)> = Vec::new();
    let mut names = HashSet::new();
    let prefix = jj_lib::matchers::PrefixMatcher::new([dir.to_owned()]);
    for (path, _) in root.entries_matching(&prefix) {
        let rel = if dir.is_root() {
            path.as_internal_file_string().to_owned()
        } else {
            path.as_internal_file_string()
                .strip_prefix(dir.as_internal_file_string())
                .and_then(|r| r.strip_prefix('/'))
                .unwrap_or("")
                .to_owned()
        };
        let (name, is_dir) = match rel.split_once('/') {
            Some((d, _)) => (d.to_owned(), true),
            None => (rel, false),
        };
        if !name.is_empty() && names.insert(name.clone()) {
            seen.push((name, is_dir));
        }
    }
    Ok(seen)
}

/// A file's contents at a value: a conflict's with its markers.
pub async fn read_value(
    repo: &dyn Repo,
    path: &RepoPath,
    value: &MergedTreeValue,
) -> Result<Vec<u8>, CommandError> {
    use jj_lib::conflicts::MaterializedTreeValue;
    use jj_lib::conflicts::materialize_tree_value;
    let labels = jj_lib::conflict_labels::ConflictLabels::unlabeled();
    let m = materialize_tree_value(repo.store(), path, value.clone(), &labels).await?;
    Ok(match m {
        MaterializedTreeValue::Absent | MaterializedTreeValue::AccessDenied(_) => Vec::new(),
        MaterializedTreeValue::File(mut f) => f.read_all(path).await?,
        MaterializedTreeValue::Symlink { target, .. } => target.into_bytes(),
        MaterializedTreeValue::FileConflict(f) => {
            let options = jj_lib::conflicts::ConflictMaterializeOptions {
                marker_style: jj_lib::conflicts::ConflictMarkerStyle::Git,
                marker_len: None,
                merge: repo.store().merge_options().clone(),
            };
            jj_lib::conflicts::materialize_merge_result_to_bytes(&f.contents, &f.labels, &options)
                .into()
        }
        MaterializedTreeValue::OtherConflict { .. } => {
            b"(a conflict of a file with something else)\n".to_vec()
        }
        MaterializedTreeValue::GitSubmodule(id) => id.hex().into_bytes(),
        MaterializedTreeValue::Tree(id) => id.hex().into_bytes(),
    })
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct RevListArgs {
    #[arg(short = 'n', long = "max-count")]
    max_count: Option<usize>,
    #[arg(long)]
    skip: Option<usize>,
    #[arg(long)]
    count: bool,
    #[arg(long)]
    reverse: bool,
    #[arg(long = "first-parent")]
    first_parent: bool,
    #[arg(long = "left-right")]
    left_right: bool,
    #[arg(long = "left-only")]
    left_only: bool,
    #[arg(long = "right-only")]
    right_only: bool,
    #[arg(long = "cherry-pick")]
    cherry_pick: bool,
    #[arg(long = "cherry-mark")]
    cherry_mark: bool,
    #[arg(long)]
    merges: bool,
    #[arg(long = "no-merges")]
    no_merges: bool,
    #[arg(long)]
    all: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    branches: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    tags: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "*")]
    remotes: Option<String>,
    #[arg(long, visible_alias = "after")]
    since: Option<String>,
    #[arg(long, visible_alias = "before")]
    until: Option<String>,
    #[arg(long = "abbrev-commit")]
    abbrev_commit: bool,
    #[arg(long)]
    parents: bool,
    #[arg(long = "topo-order")]
    topo_order: bool,
    #[arg(long = "date-order")]
    date_order: bool,
    #[arg(long = "objects")]
    objects: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "medium")]
    format: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "medium")]
    pretty: Option<String>,
    #[arg(long)]
    quiet: bool,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

pub async fn rev_list(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let args = numeric_counts(args);
    let a: RevListArgs = parse_or_return!(ui, "rev-list", &args);
    if a.objects {
        return fatal(ui, "ok keeps no git objects to list");
    }
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    let matcher = match cx.pathspec(&ws, &a.paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let mut walk = match repo::walk(&r, &a.args, false).await {
        Ok(w) => w,
        Err((spec, e)) => return rev_error(ui, &spec, e),
    };
    if a.all || a.branches.is_some() {
        walk.include
            .extend(repo::branches(repo).into_iter().filter_map(|(_, id)| id));
    }
    if a.all || a.tags.is_some() {
        walk.include
            .extend(repo::tags(repo).into_iter().filter_map(|(_, id)| id));
    }
    if a.all || a.remotes.is_some() {
        walk.include
            .extend(repo::remote_branches(repo).into_iter().map(|(_, id)| id));
    }
    if walk.include.is_empty() {
        return Ok(0);
    }
    let left_side: Option<HashSet<CommitId>> = match &walk.symmetric {
        Some((left, right)) if a.left_right || a.left_only || a.right_only || a.count => {
            let left_set = RevsetExpression::commits(vec![left.clone()])
                .ancestors()
                .minus(&RevsetExpression::commits(vec![right.clone()]).ancestors());
            let ids: Vec<CommitId> = left_set
                .evaluate(repo)
                .map_err(jj_cli::command_error::internal_error)?
                .stream()
                .try_collect()
                .await
                .map_err(jj_cli::command_error::internal_error)?;
            Some(ids.into_iter().collect())
        }
        _ => None,
    };
    let expression = if a.first_parent {
        RevsetExpression::commits(walk.include.clone())
            .first_ancestors()
            .minus(&RevsetExpression::commits(walk.exclude.clone()).ancestors())
            .minus(&RevsetExpression::root())
    } else {
        walk.expression(repo)
    };
    let since = a.since.as_deref().and_then(approxidate);
    let until = a.until.as_deref().and_then(approxidate);
    let mut ids = Vec::new();
    let mut stream = expression
        .evaluate(repo)
        .map_err(jj_cli::command_error::internal_error)?
        .stream();
    let mut skipped = 0;
    while let Some(id) = stream.next().await {
        let id = id.map_err(jj_cli::command_error::internal_error)?;
        if !a.reverse && a.max_count.is_some_and(|m| ids.len() >= m) {
            break;
        }
        let needs_commit = a.merges
            || a.no_merges
            || since.is_some()
            || until.is_some()
            || !matcher.is_everything();
        if needs_commit {
            let c = repo.store().get_commit_async(&id).await?;
            let parents = c.parent_ids().len();
            if (a.merges && parents < 2) || (a.no_merges && parents > 1) {
                continue;
            }
            let t = fmt::unix(&c.committer().timestamp);
            if since.is_some_and(|s| t < s) || until.is_some_and(|u| t > u) {
                continue;
            }
            if !matcher.is_everything() && !repo::touches(repo, &c, &matcher).await? {
                continue;
            }
        }
        if let Some(side) = &left_side {
            let left = side.contains(&id);
            if (a.left_only && !left) || (a.right_only && left) {
                continue;
            }
        }
        if let Some(n) = a.skip
            && skipped < n
        {
            skipped += 1;
            continue;
        }
        ids.push(id);
    }
    if a.reverse {
        ids.reverse();
        if let Some(m) = a.max_count {
            ids.truncate(m);
        }
    }
    let mut out = ui.stdout();
    if a.count {
        match (&left_side, a.left_right) {
            (Some(side), true) => {
                let left = ids.iter().filter(|id| side.contains(id)).count();
                writeln!(out, "{left}\t{}", ids.len() - left)?;
            }
            _ => writeln!(out, "{}", ids.len())?,
        }
        return Ok(0);
    }
    let pretty = a
        .format
        .as_deref()
        .or(a.pretty.as_deref())
        .map(Pretty::parse);
    for id in &ids {
        let mark = match (&left_side, a.left_right) {
            (Some(side), true) => {
                if side.contains(id) {
                    "<"
                } else {
                    ">"
                }
            }
            _ => "",
        };
        let shown = if a.abbrev_commit {
            repo::short(repo, id).await?
        } else {
            id.hex()
        };
        let mut line = format!("{mark}{shown}");
        if a.parents {
            let c = repo.store().get_commit_async(id).await?;
            for p in c.parent_ids() {
                if p != repo.store().root_commit_id() {
                    line.push(' ');
                    line.push_str(&p.hex());
                }
            }
        }
        match &pretty {
            Some(p) => {
                let c = repo.store().get_commit_async(id).await?;
                writeln!(out, "commit {line}")?;
                let text = render_commit(
                    repo,
                    &c,
                    p,
                    a.abbrev_commit,
                    &[],
                    &fmt::DateMode::Default { local: false },
                )
                .await?;
                let body = match p {
                    Pretty::Format { .. } | Pretty::Oneline => text,
                    _ => text
                        .split_once('\n')
                        .map(|(_, rest)| rest.to_owned())
                        .unwrap_or_default(),
                };
                write!(out, "{body}")?;
                if let Pretty::Format {
                    terminator: true, ..
                } = p
                {
                    writeln!(out)?;
                }
            }
            None => writeln!(out, "{line}")?,
        }
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct ShortlogArgs {
    #[arg(short = 'n', long)]
    numbered: bool,
    #[arg(short = 's', long)]
    summary: bool,
    #[arg(short = 'e', long)]
    email: bool,
    #[arg(short = 'c', long)]
    committer: bool,
    #[arg(long = "no-merges")]
    no_merges: bool,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

pub async fn shortlog(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: ShortlogArgs = parse_or_return!(ui, "shortlog", args);
    let ws = cx.ws_read(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    let walk = match repo::walk(&r, &a.args, true).await {
        Ok(w) => w,
        Err((spec, e)) => return rev_error(ui, &spec, e),
    };
    let matcher = match cx.pathspec(&ws, &a.paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let mut by: Vec<(String, Vec<String>)> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut stream = walk
        .expression(repo)
        .evaluate(repo)
        .map_err(jj_cli::command_error::internal_error)?
        .stream();
    while let Some(id) = stream.next().await {
        let id = id.map_err(jj_cli::command_error::internal_error)?;
        let c = repo.store().get_commit_async(&id).await?;
        if a.no_merges && c.parent_ids().len() > 1 {
            continue;
        }
        if !matcher.is_everything() && !repo::touches(repo, &c, &matcher).await? {
            continue;
        }
        let sig = if a.committer {
            c.committer()
        } else {
            c.author()
        };
        let who = if a.email {
            fmt::ident(sig)
        } else {
            sig.name.clone()
        };
        let i = *index.entry(who.clone()).or_insert_with(|| {
            by.push((who, Vec::new()));
            by.len() - 1
        });
        by[i].1.push(fmt::subject(c.description()));
    }
    if a.numbered {
        by.sort_by(|x, y| y.1.len().cmp(&x.1.len()).then(x.0.cmp(&y.0)));
    } else {
        by.sort_by(|x, y| x.0.cmp(&y.0));
    }
    let mut out = ui.stdout();
    for (who, subjects) in by {
        if a.summary {
            writeln!(out, "{:>6}\t{who}", subjects.len())?;
        } else {
            writeln!(out, "{who} ({}):", subjects.len())?;
            for s in subjects.iter().rev() {
                writeln!(out, "      {s}")?;
            }
            writeln!(out)?;
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------------------------
// ls-files, ls-tree, cat-file, check-ignore

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct LsFilesArgs {
    #[arg(short = 'z')]
    z: bool,
    #[arg(short = 'c', long)]
    cached: bool,
    #[arg(short = 'm', long)]
    modified: bool,
    #[arg(short = 'd', long)]
    deleted: bool,
    #[arg(short = 'o', long)]
    others: bool,
    #[arg(short = 'i', long)]
    ignored: bool,
    #[arg(short = 's', long)]
    stage: bool,
    #[arg(short = 'u', long)]
    unmerged: bool,
    #[arg(short = 'k', long)]
    killed: bool,
    #[arg(short = 't')]
    t: bool,
    #[arg(long = "exclude-standard")]
    exclude_standard: bool,
    #[arg(long = "error-unmatch")]
    error_unmatch: bool,
    #[arg(long = "full-name")]
    full_name: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(long = "deduplicate")]
    deduplicate: bool,
    #[arg(long = "directory")]
    directory: bool,
    #[arg(long = "no-empty-directory")]
    no_empty_directory: bool,
    #[arg(short = 'x', long = "exclude")]
    exclude: Vec<String>,
    #[arg(short = 'X', long = "exclude-from")]
    exclude_from: Vec<String>,
    #[arg(long = "exclude-per-directory")]
    exclude_per_directory: Option<String>,
    #[arg(long = "with-tree")]
    with_tree: Option<String>,
    #[arg(long = "abbrev", num_args = 0..=1, require_equals = true)]
    abbrev: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[arg(long = "eol")]
    eol: bool,
    #[arg(long = "sparse")]
    sparse: bool,
    paths: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git ls-files`: the working copy's files (git's index), under the current directory and shown
/// from it unless `--full-name`. Nothing is untracked, so `--others` lists nothing.
pub async fn ls_files(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: LsFilesArgs = parse_or_return!(ui, "ls-files", args);
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let mut specs = a.paths.clone();
    specs.extend(a.rest.iter().cloned());
    let given = !specs.is_empty();
    if specs.is_empty() {
        specs.push(".".to_owned());
    }
    let matcher = match cx.pathspec(&ws, &specs) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let term = if a.z { '\0' } else { '\n' };
    let converter = cwd_converter(cx, &ws);
    let quote_high = cx.quote_high();
    let mut listed: Vec<(RepoPathBuf, MergedTreeValue)> = Vec::new();
    if a.modified {
        let from = st.wc.parent_tree(repo).await?;
        for e in entries(&from, &st.wc.tree(), &matcher, &CopyRecords::default()).await? {
            if e.after.is_present() {
                listed.push((e.target, e.after));
            }
        }
    } else if a.unmerged {
        for (p, v) in st.wc.tree().conflicts_matching(&matcher) {
            listed.push((p, v?));
        }
    } else if a.others || a.ignored || a.deleted || a.killed {
        // Nothing is untracked or deleted from the index: they are one with the working tree.
    } else {
        listed = repo::files(&st.wc.tree(), &matcher).await?;
    }
    if a.error_unmatch && given {
        for p in matcher.literal_paths() {
            if !listed.iter().any(|(l, _)| l.starts_with(&p)) {
                writeln!(
                    ui.stderr(),
                    "error: pathspec '{}' did not match any file(s) known to git",
                    converter.format_file_path(&p)
                )?;
                writeln!(ui.stderr(), "Did you forget to 'git add'?")?;
                return Ok(1);
            }
        }
    }
    let mut out = ui.stdout();
    for (path, value) in &listed {
        let shown = if a.full_name {
            path.as_internal_file_string().to_owned()
        } else {
            converter.format_file_path(path)
        };
        let shown = if a.z {
            shown
        } else {
            fmt::quote_path(&shown, quote_high)
        };
        let tag = if a.t { "H " } else { "" };
        if a.stage {
            write!(
                out,
                "{tag}{} {} 0\t{shown}{term}",
                mode(value),
                object_hex(value, HEX_LEN)
            )?;
        } else {
            write!(out, "{tag}{shown}{term}")?;
        }
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct LsTreeArgs {
    #[arg(short = 'r')]
    r: bool,
    #[arg(short = 'd')]
    d: bool,
    #[arg(short = 't')]
    t: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(short = 'l', long = "long")]
    long: bool,
    #[arg(long = "name-only", visible_alias = "name-status")]
    name_only: bool,
    #[arg(long = "object-only")]
    object_only: bool,
    #[arg(long = "full-name")]
    full_name: bool,
    #[arg(long = "full-tree")]
    full_tree: bool,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    abbrev: Option<String>,
    #[arg(long)]
    format: Option<String>,
    tree: String,
    paths: Vec<String>,
}

pub async fn ls_tree(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: LsTreeArgs = parse_or_return!(ui, "ls-tree", args);
    let ws = cx.ws_read(ui).await?;
    let r = cx.resolver(&ws).await?;
    let (root, base) = match r.object(&a.tree).await {
        Ok(Object::Commit(c)) => (c.tree(), RepoPathBuf::root()),
        Ok(Object::Tree { root, path, .. }) => (root, path),
        Ok(Object::File { .. }) => return fatal(ui, "not a tree object"),
        Err(e) => return rev_error(ui, &a.tree, e),
    };
    let term = if a.z { '\0' } else { '\n' };
    let quote_high = cx.quote_high();
    let mut out = ui.stdout();
    let cwd_prefix = if a.full_tree {
        String::new()
    } else {
        cx.command
            .cwd()
            .strip_prefix(ws.workspace_root())
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default()
    };
    let mut specs = Vec::new();
    for p in &a.paths {
        specs.push(if cwd_prefix.is_empty() {
            p.clone()
        } else {
            format!("{cwd_prefix}/{p}")
        });
    }
    let dir = if base.is_root() && !cwd_prefix.is_empty() && specs.is_empty() {
        RepoPathBuf::from_internal_string(cwd_prefix.clone())
            .unwrap_or_else(|_| RepoPathBuf::root())
    } else {
        base.clone()
    };
    let mut emit = |path: &str, kind: &str, value: &MergedTreeValue| -> std::io::Result<()> {
        let rel = path
            .strip_prefix(base.as_internal_file_string())
            .map(|r| r.trim_start_matches('/'))
            .unwrap_or(path);
        let shown = if a.full_name || base.is_root() {
            path.to_owned()
        } else {
            rel.to_owned()
        };
        let shown = if a.z {
            shown
        } else {
            fmt::quote_path(&shown, quote_high)
        };
        if a.name_only {
            write!(out, "{shown}{term}")
        } else if a.object_only {
            write!(out, "{}{term}", object_hex(value, HEX_LEN))
        } else {
            write!(
                out,
                "{} {kind} {}\t{shown}{term}",
                mode(value),
                object_hex(value, HEX_LEN)
            )
        }
    };
    if a.r {
        let prefix = jj_lib::matchers::PrefixMatcher::new([dir.clone()]);
        for (path, value) in root.entries_matching(&prefix) {
            let value = value?;
            let p = path.as_internal_file_string();
            if !specs.is_empty()
                && !specs
                    .iter()
                    .any(|s| p == s || p.starts_with(&format!("{s}/")))
            {
                continue;
            }
            emit(p, "blob", &value)?;
        }
    } else {
        for (name, is_dir) in list_dir(&root, &dir).await? {
            let full = if dir.is_root() {
                name.clone()
            } else {
                format!("{}/{name}", dir.as_internal_file_string())
            };
            if !specs.is_empty()
                && !specs
                    .iter()
                    .any(|s| full == *s || s.starts_with(&format!("{full}/")))
            {
                continue;
            }
            let path = RepoPathBuf::from_internal_string(full.clone())
                .map_err(jj_cli::command_error::internal_error)?;
            let value = root.path_value(&path).await?;
            if a.d && !is_dir {
                continue;
            }
            emit(&full, if is_dir { "tree" } else { "blob" }, &value)?;
        }
    }
    Ok(0)
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CatFileArgs {
    #[arg(short = 't')]
    t: bool,
    #[arg(short = 's')]
    s: bool,
    #[arg(short = 'e')]
    e: bool,
    #[arg(short = 'p')]
    p: bool,
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "")]
    batch: Option<String>,
    #[arg(long = "batch-check", num_args = 0..=1, require_equals = true, default_missing_value = "")]
    batch_check: Option<String>,
    #[arg(long = "batch-command")]
    batch_command: bool,
    #[arg(long = "textconv")]
    textconv: bool,
    #[arg(long = "filters")]
    filters: bool,
    #[arg(long = "allow-unknown-type")]
    allow_unknown_type: bool,
    #[arg(long = "buffer")]
    buffer: bool,
    #[arg(short = 'Z')]
    nul: bool,
    args: Vec<String>,
}

/// An object as `cat-file` sees it: its type, its contents.
async fn object_bytes(
    repo: &dyn Repo,
    obj: &Object,
) -> Result<(&'static str, Vec<u8>), CommandError> {
    Ok(match obj {
        Object::Commit(c) => ("commit", commit_text(repo, c).into_bytes()),
        Object::Tree { root, path, .. } => {
            let mut s = String::new();
            for (name, is_dir) in list_dir(root, path).await? {
                let full = if path.is_root() {
                    name.clone()
                } else {
                    format!("{}/{name}", path.as_internal_file_string())
                };
                let p = RepoPathBuf::from_internal_string(full)
                    .map_err(jj_cli::command_error::internal_error)?;
                let v = root.path_value(&p).await?;
                s.push_str(&format!(
                    "{} {} {}\t{name}\n",
                    mode(&v),
                    if is_dir { "tree" } else { "blob" },
                    object_hex(&v, HEX_LEN)
                ));
            }
            ("tree", s.into_bytes())
        }
        Object::File { path, value, .. } => ("blob", read_value(repo, path, value).await?),
    })
}

/// A commit as git's `cat-file -p` prints one, with jj's `change-id` header as jj's git backend
/// writes it.
pub fn commit_text(repo: &dyn Repo, c: &Commit) -> String {
    let tree_hex = c
        .tree_ids()
        .as_resolved()
        .map(|t| t.hex())
        .unwrap_or_else(|| "0".repeat(HEX_LEN));
    let mut s = format!("tree {tree_hex}\n");
    for p in c.parent_ids() {
        if p != repo.store().root_commit_id() {
            s.push_str(&format!("parent {}\n", p.hex()));
        }
    }
    let (a, m) = (c.author(), c.committer());
    s.push_str(&format!(
        "author {} {} {}\ncommitter {} {} {}\nchange-id {}\n\n{}",
        fmt::ident(a),
        fmt::unix(&a.timestamp),
        fmt::zone(&a.timestamp),
        fmt::ident(m),
        fmt::unix(&m.timestamp),
        fmt::zone(&m.timestamp),
        c.change_id().reverse_hex(),
        fmt::raw_message(c.description())
    ));
    s
}

pub async fn cat_file(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: CatFileArgs = parse_or_return!(ui, "cat-file", args);
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let r = cx.resolver(&ws).await?;
    if a.batch.is_some() || a.batch_check.is_some() {
        let contents = a.batch.is_some();
        let format = a
            .batch
            .clone()
            .or(a.batch_check.clone())
            .unwrap_or_default();
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
        let mut out = ui.stdout();
        let sep = if a.nul { '\0' } else { '\n' };
        for name in input.split(sep).filter(|l| !l.is_empty()) {
            match r.object(name).await {
                Ok(obj) => {
                    let (kind, bytes) = object_bytes(repo, &obj).await?;
                    let id = object_id_hex(&obj);
                    let line = if format.is_empty() {
                        format!("{id} {kind} {}", bytes.len())
                    } else {
                        format
                            .replace("%(objectname)", &id)
                            .replace("%(objecttype)", kind)
                            .replace("%(objectsize)", &bytes.len().to_string())
                            .replace("%(rest)", "")
                    };
                    writeln!(out, "{line}")?;
                    if contents {
                        out.write_all(&bytes)?;
                        writeln!(out)?;
                    }
                }
                Err(_) => writeln!(out, "{name} missing")?,
            }
        }
        return Ok(0);
    }
    let (kind_arg, name) = match (a.t || a.s || a.e || a.p, a.args.as_slice()) {
        (true, [name]) => (None, name.clone()),
        (false, [kind, name]) => (Some(kind.clone()), name.clone()),
        _ => {
            writeln!(
                ui.stderr(),
                "usage: git cat-file (-t | -s | -e | -p | <type>) <object>"
            )?;
            return Ok(129);
        }
    };
    let obj = match r.object(&name).await {
        Ok(o) => o,
        Err(e) => {
            if a.e {
                return Ok(1);
            }
            return match e {
                repo::RevError::Command(e) => Err(e),
                _ => fatal(ui, format!("Not a valid object name {name}")),
            };
        }
    };
    if a.e {
        return Ok(0);
    }
    let (kind, bytes) = object_bytes(repo, &obj).await?;
    let mut out = ui.stdout();
    if a.t {
        writeln!(out, "{kind}")?;
    } else if a.s {
        writeln!(out, "{}", bytes.len())?;
    } else if let Some(want) = kind_arg {
        if want != kind {
            return fatal(ui, format!("git cat-file {name}: bad file"));
        }
        out.write_all(&bytes)?;
    } else {
        out.write_all(&bytes)?;
    }
    Ok(0)
}

/// An object's ID as ok knows it: a commit's, a file's, or a resolved tree's.
pub fn object_id_hex(obj: &Object) -> String {
    match obj {
        Object::Commit(c) => c.id().hex(),
        Object::File { value, .. } => object_hex(value, HEX_LEN),
        Object::Tree { root, path, .. } => {
            if path.is_root() {
                root.tree_ids()
                    .as_resolved()
                    .map(|t| t.hex())
                    .unwrap_or_else(|| "0".repeat(HEX_LEN))
            } else {
                "0".repeat(HEX_LEN)
            }
        }
    }
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct CheckIgnoreArgs {
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    #[arg(short = 'n', long = "non-matching")]
    non_matching: bool,
    #[arg(long)]
    stdin: bool,
    #[arg(short = 'z')]
    z: bool,
    #[arg(long = "no-index")]
    no_index: bool,
    paths: Vec<String>,
}

/// `git check-ignore`: which paths the ignore files (`.ignore` and `.gitignore`) name.
pub async fn check_ignore(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: CheckIgnoreArgs = parse_or_return!(ui, "check-ignore", args);
    let ws = cx.ws_read(ui).await?;
    let root = ws.workspace_root().to_owned();
    let mut paths = a.paths.clone();
    if a.stdin {
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
        let sep = if a.z { '\0' } else { '\n' };
        paths.extend(
            input
                .split(sep)
                .filter(|l| !l.is_empty())
                .map(str::to_owned),
        );
    }
    if paths.is_empty() {
        return fatal(ui, "no path specified");
    }
    let base = ws
        .base_ignores()
        .map_err(jj_cli::command_error::internal_error)?;
    let ignores = ok_working_copy::ignores(&root, &base, &jj_lib::matchers::NothingMatcher)
        .map_err(jj_cli::command_error::internal_error)?;
    let term = if a.z { '\0' } else { '\n' };
    let mut any = false;
    let mut out = ui.stdout();
    for p in &paths {
        let repo_path = match ws.parse_file_path(p) {
            Ok(rp) => rp,
            Err(_) => return fatal(ui, format!("{p}: '{p}' is outside repository")),
        };
        let is_dir = root.join(repo_path.as_internal_file_string()).is_dir();
        let ignored = ignore_matches(&ignores, &repo_path, is_dir);
        if ignored {
            any = true;
        }
        if a.quiet {
            continue;
        }
        if ignored || a.non_matching {
            if a.verbose {
                write!(out, "{}::\t{p}{term}", if ignored { ".ignore" } else { "" })?;
            } else if ignored {
                write!(out, "{p}{term}")?;
            }
        }
    }
    Ok(if any { 0 } else { 1 })
}

/// Whether the ignore rules name a path, or a directory above it.
fn ignore_matches(
    ignores: &jj_lib::gitignore::GitIgnoreFile,
    path: &RepoPath,
    is_dir: bool,
) -> bool {
    let mut dir = path.parent();
    while let Some(d) = dir {
        if !d.is_root() && ignores.matches_dir(d) {
            return true;
        }
        dir = d.parent();
    }
    if is_dir {
        ignores.matches_dir(path)
    } else {
        ignores.matches_file(path)
    }
}

// ---------------------------------------------------------------------------------------------
// blame, grep

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true)]
struct BlameArgs {
    #[arg(short = 'L')]
    lines: Vec<String>,
    #[arg(short = 's')]
    s: bool,
    #[arg(short = 'l')]
    l: bool,
    #[arg(short = 't')]
    t: bool,
    #[arg(short = 'e', long = "show-email")]
    show_email: bool,
    #[arg(short = 'f', long = "show-name")]
    show_name: bool,
    #[arg(short = 'n', long = "show-number")]
    show_number: bool,
    #[arg(short = 'p', long)]
    porcelain: bool,
    #[arg(long = "line-porcelain")]
    line_porcelain: bool,
    #[arg(long)]
    incremental: bool,
    #[arg(short = 'w')]
    w: bool,
    #[arg(short = 'M', num_args = 0..=1, require_equals = true, default_missing_value = "")]
    m: Option<String>,
    #[arg(short = 'C', num_args = 0..=1, require_equals = true, default_missing_value = "")]
    c: Option<String>,
    #[arg(long)]
    root: bool,
    #[arg(long)]
    date: Option<String>,
    #[arg(long = "abbrev", num_args = 0..=1, require_equals = true)]
    abbrev: Option<String>,
    #[arg(long = "color-lines")]
    color_lines: bool,
    #[arg(long = "color-by-age")]
    color_by_age: bool,
    #[arg(long = "ignore-rev")]
    ignore_rev: Vec<String>,
    #[arg(long = "ignore-revs-file")]
    ignore_revs_file: Vec<String>,
    #[arg(long)]
    contents: Option<String>,
    args: Vec<String>,
    #[arg(last = true)]
    rest: Vec<String>,
}

/// `git blame [<rev>] [--] <file>`: who last changed each line, from jj's annotation. Without a
/// revision, the working copy's file: lines the working change made are "Not Committed Yet".
pub async fn blame(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: BlameArgs = parse_or_return!(ui, "blame", args);
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let mut positional = a.args.clone();
    positional.extend(a.rest.iter().cloned());
    let (rev, file) = match positional.as_slice() {
        [file] => (None, file.clone()),
        [rev, file] => (Some(rev.clone()), file.clone()),
        _ => {
            writeln!(
                ui.stderr(),
                "usage: git blame [<options>] [<rev>] [--] <file>"
            )?;
            return Ok(129);
        }
    };
    let start = match &rev {
        Some(rv) => match r.commit_of(rv).await {
            Ok(c) => c,
            Err(e) => return rev_error(ui, rv, e),
        },
        None => st.wc.clone(),
    };
    let path = match ws.parse_file_path(&file) {
        Ok(p) => p,
        Err(_) => return fatal(ui, format!("no such path {file}")),
    };
    if start.tree().path_value(&path).await?.is_absent() {
        return fatal(
            ui,
            format!(
                "no such path '{file}' in {}",
                rev.as_deref().unwrap_or("HEAD")
            ),
        );
    }
    let mut annotator = jj_lib::annotate::FileAnnotator::from_commit(&start, &path).await?;
    let domain: std::sync::Arc<jj_lib::revset::ResolvedRevsetExpression> = RevsetExpression::all();
    annotator
        .compute(repo, &domain)
        .await
        .map_err(jj_cli::command_error::internal_error)?;
    let annotation = annotator.to_annotation();
    let lines: Vec<_> = annotation.line_origins().collect();
    let total = lines.len();
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for l in &a.lines {
        match parse_line_range(l, total) {
            Some(r) => ranges.push(r),
            None => return fatal(ui, format!("invalid -L argument '{l}'")),
        }
    }
    let date_mode = match a.date.as_deref() {
        Some(d) => fmt::DateMode::parse(d).unwrap_or(fmt::DateMode::Iso { local: false }),
        None => fmt::DateMode::Iso { local: false },
    };
    let uncommitted = st.wc.id().clone();
    let mut commits: HashMap<CommitId, Commit> = HashMap::new();
    let mut shorts: HashMap<CommitId, String> = HashMap::new();
    for (origin, _) in &lines {
        let id = match origin {
            Ok(o) | Err(o) => &o.commit_id,
        };
        if !commits.contains_key(id) {
            let c = repo.store().get_commit_async(id).await?;
            let s = if *id == uncommitted {
                "0".repeat(8)
            } else {
                repo::short(repo, id).await?
            };
            shorts.insert(id.clone(), s);
            commits.insert(id.clone(), c);
        }
    }
    let width = shorts.values().map(String::len).max().unwrap_or(8);
    let author_width = commits
        .values()
        .map(|c| {
            if *c.id() == uncommitted {
                "Not Committed Yet".len()
            } else {
                c.author().name.chars().count()
            }
        })
        .max()
        .unwrap_or(0);
    ui.request_pager();
    let mut out = ui.stdout();
    let mut told: HashSet<CommitId> = HashSet::new();
    let num_width = total.to_string().len();
    for (i, (origin, text)) in lines.iter().enumerate() {
        let n = i + 1;
        if !ranges.is_empty() && !ranges.iter().any(|(s, e)| n >= *s && n <= *e) {
            continue;
        }
        let o = match origin {
            Ok(o) | Err(o) => *o,
        };
        let c = &commits[&o.commit_id];
        let is_wc = o.commit_id == uncommitted;
        let text = String::from_utf8_lossy(text.as_ref());
        let text = text.strip_suffix('\n').unwrap_or(&text);
        if a.porcelain || a.line_porcelain {
            let hex = if is_wc {
                "0".repeat(HEX_LEN)
            } else {
                o.commit_id.hex()
            };
            writeln!(out, "{hex} {} {n} 1", o.line_number + 1)?;
            if a.line_porcelain || told.insert(o.commit_id.clone()) {
                let (au, cm) = (c.author(), c.committer());
                let (name, mail) = if is_wc {
                    (
                        "Not Committed Yet".to_owned(),
                        "<not.committed.yet>".to_owned(),
                    )
                } else {
                    (au.name.clone(), format!("<{}>", au.email))
                };
                writeln!(
                    out,
                    "author {name}\nauthor-mail {mail}\nauthor-time {}\nauthor-tz {}",
                    fmt::unix(&au.timestamp),
                    fmt::zone(&au.timestamp)
                )?;
                writeln!(
                    out,
                    "committer {}\ncommitter-mail <{}>\ncommitter-time {}\ncommitter-tz {}",
                    if is_wc { "Not Committed Yet" } else { &cm.name },
                    cm.email,
                    fmt::unix(&cm.timestamp),
                    fmt::zone(&cm.timestamp)
                )?;
                writeln!(
                    out,
                    "summary {}",
                    if is_wc {
                        "Version of working tree".to_owned()
                    } else {
                        fmt::subject(c.description())
                    }
                )?;
                for p in c.parent_ids().iter().take(1) {
                    if p != repo.store().root_commit_id() && !is_wc {
                        writeln!(
                            out,
                            "previous {} {}",
                            p.hex(),
                            path.as_internal_file_string()
                        )?;
                    }
                }
                if c.parent_ids().first() == Some(repo.store().root_commit_id()) {
                    writeln!(out, "boundary")?;
                }
                writeln!(out, "filename {}", path.as_internal_file_string())?;
            }
            writeln!(out, "\t{text}")?;
            continue;
        }
        let id = if a.l {
            if is_wc {
                "0".repeat(HEX_LEN)
            } else {
                o.commit_id.hex()
            }
        } else {
            format!("{:<width$}", shorts[&o.commit_id])
        };
        let who = if is_wc {
            if a.show_email {
                "<not.committed.yet>".to_owned()
            } else {
                "Not Committed Yet".to_owned()
            }
        } else if a.show_email {
            format!("<{}>", c.author().email)
        } else {
            c.author().name.clone()
        };
        let when = if a.t {
            format!(
                "{} {}",
                fmt::unix(&c.author().timestamp),
                fmt::zone(&c.author().timestamp)
            )
        } else {
            fmt::date(&c.author().timestamp, &date_mode)
        };
        let file_col = if a.show_name {
            format!("{} ", path.as_internal_file_string())
        } else {
            String::new()
        };
        let orig = if a.show_number {
            format!("{} ", o.line_number + 1)
        } else {
            String::new()
        };
        if a.s {
            writeln!(out, "{id} {file_col}{orig}{n:>num_width$}) {text}")?;
        } else {
            writeln!(
                out,
                "{id} {file_col}{orig}({who:<author_width$} {when} {n:>num_width$}) {text}"
            )?;
        }
    }
    Ok(0)
}

/// `-L <start>,<end>` (either may be empty, `+<n>` a count): the lines, 1-based, inclusive.
fn parse_line_range(s: &str, total: usize) -> Option<(usize, usize)> {
    let (a, b) = s.split_once(',').unwrap_or((s, ""));
    let start = if a.is_empty() {
        1
    } else {
        a.parse::<usize>().ok()?
    };
    let end = if b.is_empty() {
        total
    } else if let Some(n) = b.strip_prefix('+') {
        start + n.parse::<usize>().ok()?.saturating_sub(1)
    } else if let Some(n) = b.strip_prefix('-') {
        let n = n.parse::<usize>().ok()?;
        return Some((start.saturating_sub(n.saturating_sub(1)).max(1), start));
    } else {
        b.parse().ok()?
    };
    (start >= 1 && start <= end.max(start)).then_some((start, end.min(total)))
}

#[derive(clap::Parser, Debug)]
#[command(disable_help_subcommand = true, disable_help_flag = true)]
struct GrepArgs {
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,
    #[arg(short = 'n', long = "line-number")]
    line_number: bool,
    #[arg(long = "column")]
    column: bool,
    #[arg(short = 'i', long = "ignore-case")]
    ignore_case: bool,
    #[arg(short = 'w', long = "word-regexp")]
    word: bool,
    #[arg(short = 'v', long = "invert-match")]
    invert: bool,
    #[arg(short = 'l', long = "files-with-matches", visible_alias = "name-only")]
    files_with_matches: bool,
    #[arg(short = 'L', long = "files-without-match")]
    files_without_match: bool,
    #[arg(short = 'c', long = "count")]
    count: bool,
    #[arg(short = 'h')]
    no_filename: bool,
    #[arg(short = 'H')]
    with_filename: bool,
    #[arg(long = "full-name")]
    full_name: bool,
    #[arg(short = 'E', long = "extended-regexp")]
    extended: bool,
    #[arg(short = 'G', long = "basic-regexp")]
    basic: bool,
    #[arg(short = 'F', long = "fixed-strings")]
    fixed: bool,
    #[arg(short = 'P', long = "perl-regexp")]
    perl: bool,
    #[arg(short = 'e')]
    patterns: Vec<String>,
    #[arg(short = 'f')]
    pattern_file: Option<String>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'o', long = "only-matching")]
    only_matching: bool,
    #[arg(short = 'z', long = "null")]
    null: bool,
    #[arg(short = 'I')]
    skip_binary: bool,
    #[arg(short = 'a', long)]
    text: bool,
    #[arg(long = "cached")]
    cached: bool,
    #[arg(long = "untracked")]
    untracked: bool,
    #[arg(long = "no-index")]
    no_index: bool,
    #[arg(long = "recurse-submodules")]
    recurse_submodules: bool,
    #[arg(long = "max-depth")]
    max_depth: Option<i64>,
    #[arg(short = 'm', long = "max-count")]
    max_count: Option<usize>,
    #[arg(long = "color", num_args = 0..=1, require_equals = true, default_missing_value = "always")]
    color: Option<String>,
    #[arg(long = "no-color")]
    no_color: bool,
    #[arg(long = "heading")]
    heading: bool,
    #[arg(long = "break")]
    break_: bool,
    #[arg(short = 'A', long = "after-context")]
    after: Option<usize>,
    #[arg(short = 'B', long = "before-context")]
    before: Option<usize>,
    #[arg(short = 'C', long = "context")]
    context: Option<usize>,
    #[arg(long = "threads")]
    threads: Option<usize>,
    #[arg(long = "and")]
    and: bool,
    #[arg(long = "or")]
    or: bool,
    #[arg(long = "all-match")]
    all_match: bool,
    args: Vec<String>,
    #[arg(last = true)]
    paths: Vec<String>,
}

/// A basic regular expression as the extended syntax the regex crate reads: in BRE, `+ ? | ( ) {
/// }` are literal unless escaped.
fn bre_to_ere(p: &str) -> String {
    let mut out = String::new();
    let mut chars = p.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(n @ ('+' | '?' | '|' | '(' | ')' | '{' | '}')) => out.push(n),
                Some(n) => {
                    out.push('\\');
                    out.push(n);
                }
                None => out.push_str("\\\\"),
            },
            '+' | '?' | '|' | '(' | ')' | '{' | '}' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out
}

/// `git grep`: lines matching patterns in the working tree's files (or a revision's).
pub async fn grep(ui: &mut Ui, cx: &Cx<'_>, args: &[String]) -> Code {
    let a: GrepArgs = parse_or_return!(ui, "grep", args);
    let ws = cx.ws(ui).await?;
    let repo = ws.repo().as_ref();
    let st = repo::state(&ws).await?;
    let r = cx.resolver(&ws).await?;
    let mut patterns = a.patterns.clone();
    let mut positional = a.args.clone();
    if let Some(f) = &a.pattern_file {
        let text = match std::fs::read_to_string(f) {
            Ok(t) => t,
            Err(e) => return fatal(ui, format!("cannot open '{f}': {e}")),
        };
        patterns.extend(text.lines().map(str::to_owned));
    }
    if patterns.is_empty() {
        if positional.is_empty() {
            return fatal(ui, "no pattern given");
        }
        patterns.push(positional.remove(0));
    }
    let (revs, mut paths) =
        match split_revs_paths(&r, ws.workspace_root(), cx.command.cwd(), &positional).await {
            Ok(x) => x,
            Err(e) => return fatal(ui, e),
        };
    paths.extend(a.paths.iter().cloned());
    let matcher = match cx.pathspec(&ws, &paths) {
        Ok(m) => m,
        Err(e) => return fatal(ui, e.0),
    };
    let mut res = Vec::new();
    for p in &patterns {
        let p = if a.fixed {
            regex::escape(p)
        } else if a.extended || a.perl {
            p.clone()
        } else {
            bre_to_ere(p)
        };
        let p = if a.word { format!(r"\b(?:{p})\b") } else { p };
        match regex::bytes::RegexBuilder::new(&p)
            .case_insensitive(a.ignore_case)
            .build()
        {
            Ok(re) => res.push(re),
            Err(e) => return fatal(ui, format!("invalid pattern: {e}")),
        }
    }
    let trees: Vec<(Option<String>, MergedTree)> = if revs.is_empty() {
        vec![(None, st.wc.tree())]
    } else {
        let mut v = Vec::new();
        for rv in &revs {
            match r.commit_of(rv).await {
                Ok(c) => v.push((Some(rv.clone()), c.tree())),
                Err(e) => return rev_error(ui, rv, e),
            }
        }
        v
    };
    let converter = cwd_converter(cx, &ws);
    let root = ws.workspace_root().to_owned();
    let mut found = false;
    let show_names = !a.no_filename;
    let sep_name = if a.null { "\0" } else { ":" };
    ui.request_pager();
    let mut out = ui.stdout();
    let after = a.after.or(a.context).unwrap_or(0);
    let before = a.before.or(a.context).unwrap_or(0);
    for (rev, tree) in &trees {
        for (path, value) in repo::files(tree, &matcher).await? {
            let bytes = if rev.is_none() {
                match std::fs::read(root.join(path.to_fs_path_unchecked(std::path::Path::new(""))))
                {
                    Ok(b) => b,
                    Err(_) => continue,
                }
            } else {
                read_value(repo, &path, &value).await?
            };
            let binary = bytes.iter().take(8000).any(|b| *b == 0);
            if binary && (a.skip_binary || !a.text) && a.skip_binary {
                continue;
            }
            let shown = if a.full_name {
                path.as_internal_file_string().to_owned()
            } else {
                converter.format_file_path(&path)
            };
            let name = match rev {
                Some(rv) => format!("{rv}:{shown}"),
                None => shown,
            };
            let lines: Vec<&[u8]> = bytes.split_inclusive(|b| *b == b'\n').collect();
            let mut hits: Vec<usize> = Vec::new();
            for (i, line) in lines.iter().enumerate() {
                let line = line.strip_suffix(b"\n").unwrap_or(line);
                let m = if a.all_match {
                    res.iter().all(|re| re.is_match(line))
                } else {
                    res.iter().any(|re| re.is_match(line))
                };
                if m != a.invert {
                    hits.push(i);
                    if a.max_count.is_some_and(|mc| hits.len() >= mc) {
                        break;
                    }
                }
            }
            if a.files_without_match {
                if hits.is_empty() {
                    found = true;
                    if !a.quiet {
                        write!(out, "{name}{}", if a.null { "\0" } else { "\n" })?;
                    }
                }
                continue;
            }
            if hits.is_empty() {
                continue;
            }
            found = true;
            if a.quiet {
                return Ok(0);
            }
            if a.files_with_matches {
                write!(out, "{name}{}", if a.null { "\0" } else { "\n" })?;
                continue;
            }
            if a.count {
                writeln!(out, "{name}{sep_name}{}", hits.len())?;
                continue;
            }
            if binary && !a.text {
                writeln!(out, "Binary file {name} matches")?;
                continue;
            }
            if a.heading && show_names {
                writeln!(out, "{name}")?;
            }
            let shown_set: HashSet<usize> = hits
                .iter()
                .flat_map(|h| h.saturating_sub(before)..=(*h + after).min(lines.len() - 1))
                .collect();
            let mut order: Vec<usize> = shown_set.into_iter().collect();
            order.sort();
            let hit_set: HashSet<usize> = hits.iter().copied().collect();
            let mut last: Option<usize> = None;
            for i in order {
                if (before > 0 || after > 0) && last.is_some_and(|l| i > l + 1) {
                    writeln!(out, "--")?;
                }
                last = Some(i);
                let line = lines[i].strip_suffix(b"\n").unwrap_or(lines[i]);
                let is_hit = hit_set.contains(&i);
                let mark = if is_hit {
                    sep_name
                } else if a.null {
                    "\0"
                } else {
                    "-"
                };
                let mut prefix = String::new();
                if show_names && !a.heading {
                    prefix.push_str(&name);
                    prefix.push_str(mark);
                }
                if a.line_number {
                    prefix.push_str(&(i + 1).to_string());
                    prefix.push_str(mark);
                }
                if a.only_matching && is_hit {
                    for re in &res {
                        for m in re.find_iter(line) {
                            let mut p = prefix.clone();
                            if a.column {
                                p.push_str(&(m.start() + 1).to_string());
                                p.push_str(mark);
                            }
                            out.write_all(p.as_bytes())?;
                            out.write_all(m.as_bytes())?;
                            writeln!(out)?;
                        }
                    }
                    continue;
                }
                if a.column && is_hit {
                    let col = res
                        .iter()
                        .filter_map(|re| re.find(line))
                        .map(|m| m.start() + 1)
                        .min()
                        .unwrap_or(1);
                    prefix.push_str(&col.to_string());
                    prefix.push_str(mark);
                }
                out.write_all(prefix.as_bytes())?;
                out.write_all(line)?;
                writeln!(out)?;
            }
        }
    }
    Ok(if found { 0 } else { 1 })
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn args() {
        StatusArgs::command().debug_assert();
        DiffArgs::command().debug_assert();
        DiffIndexArgs::command().debug_assert();
        DiffTreeArgs::command().debug_assert();
        LogArgs::command().debug_assert();
        ShowArgs::command().debug_assert();
        RevListArgs::command().debug_assert();
        ShortlogArgs::command().debug_assert();
        LsFilesArgs::command().debug_assert();
        LsTreeArgs::command().debug_assert();
        CatFileArgs::command().debug_assert();
        CheckIgnoreArgs::command().debug_assert();
        BlameArgs::command().debug_assert();
        GrepArgs::command().debug_assert();
    }

    #[test]
    fn stats() {
        let rows = vec![
            StatRow {
                name: "a.txt".to_owned(),
                change: Some((1, 1)),
            },
            StatRow {
                name: "dir/{b.txt => c.txt}".to_owned(),
                change: Some((0, 0)),
            },
        ];
        assert_eq!(
            git_stat(&rows, 80),
            " a.txt                | 2 +-\n dir/{b.txt => c.txt} | 0\n"
        );
        // A binary file widens the counts to its `Bin`, as git's do.
        let rows = vec![
            StatRow {
                name: "a.txt".to_owned(),
                change: Some((1, 1)),
            },
            StatRow {
                name: "img.png".to_owned(),
                change: None,
            },
        ];
        assert_eq!(git_stat(&rows, 80), " a.txt   |   2 +-\n img.png | Bin\n");
        let wide = vec![StatRow {
            name: "f".to_owned(),
            change: Some((300, 100)),
        }];
        let line = git_stat(&wide, 80);
        assert!(line.len() <= 81, "{line}");
        assert!(line.starts_with(" f | 400 +++"), "{line}");
    }

    #[test]
    fn renames() {
        assert_eq!(
            pretty_rename("sub/b.txt", "sub/c.txt"),
            "sub/{b.txt => c.txt}"
        );
        assert_eq!(pretty_rename("a.txt", "b.txt"), "a.txt => b.txt");
        assert_eq!(pretty_rename("a/x/f", "b/x/f"), "{a => b}/x/f");
        assert_eq!(pretty_rename("d/a/f", "d/b/f"), "d/{a => b}/f");
    }

    #[test]
    fn filters() {
        assert!(passes_filter("AM", 'A'));
        assert!(!passes_filter("AM", 'D'));
        assert!(passes_filter("d", 'M'));
        assert!(!passes_filter("d", 'D'));
    }

    #[test]
    fn line_ranges() {
        assert_eq!(parse_line_range("2,4", 10), Some((2, 4)));
        assert_eq!(parse_line_range("2,+3", 10), Some((2, 4)));
        assert_eq!(parse_line_range("5,", 10), Some((5, 10)));
        assert_eq!(parse_line_range(",3", 10), Some((1, 3)));
    }

    #[test]
    fn basic_regexps() {
        assert_eq!(bre_to_ere("a+b"), r"a\+b");
        assert_eq!(bre_to_ere(r"a\+b"), "a+b");
        assert_eq!(bre_to_ere(r"\(x\)"), "(x)");
        assert_eq!(bre_to_ere("x|y"), r"x\|y");
    }

    #[test]
    fn counts() {
        assert_eq!(
            numeric_counts(&["-3".to_owned(), "--".to_owned(), "-4".to_owned()]),
            vec!["--max-count=3", "--", "-4"]
        );
    }
}
