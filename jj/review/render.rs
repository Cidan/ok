//! How `jj cl status` shows a CL, and when following it stops.

use std::fmt::Write as _;

use ok_proto::ok::control::v1 as pb;
use pb::ClStatus;
use pb::Stage;
use pb::WaitingOn;

/// How following a CL ends, and the process's exit code for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    /// It reached what was asked for: 0.
    Reached,
    /// It waits on a person — its author, reviewers, or an admin — before it can get there: 2.
    NeedsPerson,
    /// Something failed, or it was abandoned: 1.
    Failed,
}

impl Outcome {
    pub fn code(self) -> i32 {
        match self {
            Outcome::Reached => 0,
            Outcome::Failed => 1,
            Outcome::NeedsPerson => 2,
        }
    }
}

/// The exit code for a follow that ran out of time.
pub const TIMED_OUT: i32 = 3;
/// The exit code for a CL ok could not be asked about: no such CL, or ok unreachable.
pub const UNREACHABLE: i32 = 4;

/// What a follow waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Until {
    /// Until nothing more happens without a person: finished, failed, or waiting on someone
    Settled,
    /// Until its current patchset's pre passed
    Pre,
    /// Until it is approved with pre passed: ready to submit
    Submittable,
    /// Until it lands on main
    Landed,
    /// Until every deployment of its commit succeeded
    Deployed,
}

/// The few facts following needs, from a status or its JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Point {
    pub stage: Stage,
    pub waiting_on: WaitingOn,
}

impl From<&ClStatus> for Point {
    fn from(st: &ClStatus) -> Point {
        Point {
            stage: st.stage(),
            waiting_on: st.waiting_on(),
        }
    }
}

impl Point {
    /// The point a status's JSON (ok control's REST form) is at.
    pub fn from_json(v: &serde_json::Value) -> Option<Point> {
        let stage = Stage::from_str_name(v.get("stage")?.as_str()?)?;
        let waiting_on = WaitingOn::from_str_name(v.get("waitingOn")?.as_str()?)?;
        Some(Point { stage, waiting_on })
    }
}

/// How far along the lifecycle a stage is, for the milestones: a later stage has passed every
/// earlier one.
fn rank(s: Stage) -> u8 {
    match s {
        Stage::Unspecified => 0,
        Stage::Draft | Stage::PreRunning | Stage::PreFailed => 1,
        Stage::InReview => 2,
        Stage::Submittable => 3,
        Stage::Submitted | Stage::PostFailed => 4,
        Stage::Landed | Stage::Deploying | Stage::DeployFailed => 5,
        Stage::Deployed => 6,
        Stage::Abandoned => 0,
    }
}

/// Whether following stops at p for until, and how it ended; None to keep following.
pub fn outcome(p: Point, until: Until) -> Option<Outcome> {
    let reached = match until {
        Until::Settled => p.stage == Stage::Deployed,
        // A draft whose pre passed waits on its author; past review, pre has passed.
        Until::Pre => (p.stage == Stage::Draft) || rank(p.stage) >= 2,
        Until::Submittable => rank(p.stage) >= 3 && p.stage != Stage::PostFailed,
        Until::Landed => rank(p.stage) >= 5,
        Until::Deployed => p.stage == Stage::Deployed,
    };
    if reached {
        return Some(Outcome::Reached);
    }
    match p.stage {
        Stage::Abandoned | Stage::PreFailed | Stage::PostFailed | Stage::DeployFailed => {
            return Some(Outcome::Failed);
        }
        _ => {}
    }
    match p.waiting_on {
        WaitingOn::Author | WaitingOn::Reviewers | WaitingOn::Admins => Some(Outcome::NeedsPerson),
        WaitingOn::Nobody => Some(Outcome::Reached),
        _ => None,
    }
}

/// A stage, in words.
pub fn stage_word(s: Stage) -> &'static str {
    match s {
        Stage::Unspecified => "unknown",
        Stage::Draft => "draft",
        Stage::PreRunning => "pre running",
        Stage::PreFailed => "pre failed",
        Stage::InReview => "in review",
        Stage::Submittable => "submittable",
        Stage::Submitted => "submitted",
        Stage::PostFailed => "submit failed",
        Stage::Landed => "landed",
        Stage::Deploying => "deploying",
        Stage::Deployed => "deployed",
        Stage::DeployFailed => "deploy failed",
        Stage::Abandoned => "abandoned",
    }
}

fn waiting_word(w: WaitingOn) -> &'static str {
    match w {
        WaitingOn::Unspecified => "",
        WaitingOn::Nobody => "nobody",
        WaitingOn::Automation => "ok",
        WaitingOn::Author => "its author",
        WaitingOn::Reviewers => "reviewers",
        WaitingOn::Admins => "a repository admin",
    }
}

/// One line saying where a CL stands: what following prints each time it changes.
pub fn line(st: &ClStatus) -> String {
    let n = st.cl.as_ref().map_or(0, |c| c.number);
    let mut out = format!("cl/{n}  {:<13}  {}", stage_word(st.stage()), st.summary);
    if matches!(
        st.waiting_on(),
        WaitingOn::Author | WaitingOn::Reviewers | WaitingOn::Admins
    ) {
        let _ = write!(out, "  [waiting on {}", waiting_word(st.waiting_on()));
        if !st.waiting_for.is_empty() {
            let _ = write!(out, ": {}", st.waiting_for.join(", "));
        }
        out.push(']');
    }
    out
}

/// What failed, with its link and its log's last lines.
pub fn failure(st: &ClStatus) -> String {
    let Some(f) = &st.failure else {
        return String::new();
    };
    let n = st.cl.as_ref().map_or(0, |c| c.number);
    let mut out = format!("cl/{n}  {} failed", f.what);
    if !f.name.is_empty() {
        let _ = write!(out, ": {}", f.name);
    }
    if !f.reason.is_empty() {
        let _ = write!(out, " — {}", f.reason);
    }
    out.push('\n');
    if !f.url.is_empty() {
        let _ = writeln!(out, "  {}", f.url);
    }
    if f.run > 0 {
        let _ = writeln!(out, "  log: jj cl log --cl {} {}", n, f.run);
    }
    for l in f.log_tail.lines() {
        let _ = writeln!(out, "  │ {l}");
    }
    out
}

fn check_status(s: pb::check::Status) -> &'static str {
    match s {
        pb::check::Status::Unspecified => "?",
        pb::check::Status::Queued => "queued",
        pb::check::Status::Running => "running",
        pb::check::Status::Passed => "passed",
        pb::check::Status::Failed => "failed",
        pb::check::Status::Canceled => "canceled",
        pb::check::Status::Error => "error",
    }
}

/// What a check runs, beside its kind: a built-in job's ID is its kind and its target
/// (`test //x:test`), or its kind alone with targets of its own (`build`); a workflow job's is its
/// name.
pub fn check_what(r: &pb::Check) -> String {
    if r.name == r.kind {
        return r.targets.join(" ");
    }
    if let Some(target) = r.name.strip_prefix(&format!("{} ", r.kind)) {
        return target.to_owned();
    }
    let mut what = r.name.clone();
    for t in &r.targets {
        if !what.contains(t.as_str()) {
            what.push(' ');
            what.push_str(t);
        }
    }
    what
}

/// The checks of a pre or a post: each that has not passed, so a passing one is one line.
fn checks(out: &mut String, runs: &[pb::Check]) {
    for r in runs
        .iter()
        .filter(|r| r.status() != pb::check::Status::Passed)
    {
        let _ = write!(
            out,
            "  {:<8}  {:<5}  {}",
            check_status(r.status()),
            r.kind,
            check_what(r)
        );
        if !r.reason.is_empty() {
            let _ = write!(out, " — {}", r.reason);
        }
        if r.carried_from > 0 {
            out.push_str(" (carried from pre)");
        }
        let _ = writeln!(out, "  (run {})", r.run);
    }
}

/// Everything about where a CL stands: what `jj cl status` prints.
pub fn full(st: &ClStatus) -> String {
    let mut out = String::new();
    let Some(cl) = &st.cl else {
        return out;
    };
    let status = match cl.status() {
        pb::cl::Status::Draft => "draft",
        pb::cl::Status::Open => "open",
        pb::cl::Status::Submitted => "submitted",
        pb::cl::Status::Abandoned => "abandoned",
        pb::cl::Status::Unspecified => "?",
    };
    let _ = writeln!(out, "cl/{}  {}", cl.number, cl.title);
    let _ = writeln!(
        out,
        "  {status} · patchset {} · by {} · {}",
        cl.patchset, cl.author, cl.url
    );
    if !cl.landed_commit.is_empty() {
        let _ = writeln!(
            out,
            "  landed as {} by {}",
            cl.landed_commit, cl.submitted_by
        );
    }
    let _ = writeln!(out, "{}", line(st));
    if !st.unavailable.is_empty() {
        let _ = writeln!(out, "  (could not read: {})", st.unavailable.join(", "));
    }

    let active = matches!(cl.status(), pb::cl::Status::Draft | pb::cl::Status::Open);
    if let Some(rv) = st.review.as_ref().filter(|_| !active) {
        // A closed CL's review is not evaluated again: whether it was approved, and its threads.
        let approved = if rv.approved {
            "approved"
        } else {
            "not approved"
        };
        let _ = writeln!(out, "review: {approved}");
        for t in &rv.unresolved {
            let on = if t.file.is_empty() { "the CL" } else { &t.file };
            let _ = writeln!(out, "  unresolved thread {} by {} on {on}", t.id, t.author);
        }
    }
    if let Some(rv) = st.review.as_ref().filter(|_| active) {
        let approved = if rv.approved {
            "approved"
        } else {
            "not approved"
        };
        let _ = writeln!(out, "review: {approved}");
        if !rv.error.is_empty() {
            let _ = writeln!(out, "  OWNERS: {}", rv.error);
        }
        let lgtm = if rv.lgtm {
            format!("yes ({})", rv.lgtm_by.join(", "))
        } else {
            "no".to_owned()
        };
        let _ = writeln!(out, "  LGTM       {lgtm}");
        if !rv.reviewers.is_empty() {
            let _ = writeln!(out, "  reviewers  {}", rv.reviewers.join(", "));
        }
        for o in &rv.owner_sets {
            let label = if o.config.is_empty() {
                "anyone (no OWNERS)".to_owned()
            } else {
                format!("//{}", o.config)
            };
            let state = if o.implicit {
                "approved: the author owns it".to_owned()
            } else if o.satisfied {
                format!("approved by {}", o.approved_by.join(", "))
            } else {
                {
                    let who: Vec<&str> = o
                        .owners
                        .iter()
                        .map(|w| if w == "*" { "anyone" } else { w.as_str() })
                        .collect();
                    format!("needs Approval from {}", who.join(", "))
                }
            };
            let _ = writeln!(out, "  {label}  {state}");
        }
        for i in &rv.identity {
            let _ = writeln!(out, "  needs {i}");
        }
        for t in &rv.unresolved {
            let at = match (t.file.as_str(), t.line) {
                ("", _) => "the CL".to_owned(),
                (f, 0) => f.to_owned(),
                (f, l) => format!("{f}:{l}"),
            };
            let body: String = t
                .body
                .lines()
                .next()
                .unwrap_or_default()
                .chars()
                .take(100)
                .collect();
            let _ = writeln!(
                out,
                "  unresolved thread {} by {} on {at}: {body}",
                t.id, t.author
            );
        }
        if !rv.blocked.is_empty() {
            let _ = writeln!(out, "  cannot submit: {}", rv.blocked);
        }
    }

    if let Some(pre) = &st.pre {
        let verdict = match pre.verdict() {
            pb::checks::Verdict::Passed => "passed",
            pb::checks::Verdict::Failed => "failed",
            pb::checks::Verdict::Pending => "running",
            pb::checks::Verdict::None => "not planned yet",
            pb::checks::Verdict::Unspecified => "unknown",
        };
        let _ = writeln!(
            out,
            "pre (patchset {}): {verdict} {}/{}",
            pre.patchset, pre.passed, pre.total
        );
        checks(&mut out, &pre.runs);
    }

    if let Some(p) = &st.post {
        let status = match p.status() {
            pb::post::Status::Active => "submitting",
            pb::post::Status::Landed => "landed",
            pb::post::Status::Failed => "failed",
            pb::post::Status::Canceled => "canceled",
            pb::post::Status::Unspecified => "?",
        };
        let _ = write!(
            out,
            "post (submission {}, patchset {}): {status}",
            p.submission, p.patchset
        );
        if !p.attempt.is_empty() {
            let _ = write!(out, ", {}", p.attempt);
        }
        let _ = writeln!(out, " {}/{}", p.passed, p.total);
        if !p.note.is_empty() {
            let _ = writeln!(out, "  {}", p.note);
        }
        if !p.reason.is_empty() {
            let _ = writeln!(out, "  {}", p.reason);
        }
        checks(&mut out, &p.runs);
    }

    if let Some(d) = &st.deployments {
        let _ = writeln!(
            out,
            "deployments: {}",
            if d.planned {
                "planned"
            } else {
                "not planned yet"
            }
        );
        for it in &d.items {
            let status = match it.status() {
                pb::deployment::Status::Waiting => "waiting",
                pb::deployment::Status::Running => "running",
                pb::deployment::Status::Succeeded => "succeeded",
                pb::deployment::Status::Failed => "failed",
                pb::deployment::Status::Halted => "halted",
                pb::deployment::Status::Canceled => "canceled",
                pb::deployment::Status::Unspecified => "?",
            };
            let _ = write!(out, "  {status:<9}  {}", it.name);
            if !it.workload.is_empty() {
                let _ = write!(out, " ({})", it.workload);
            }
            let _ = write!(out, "  {}", it.stage);
            if !it.reason.is_empty() {
                let _ = write!(out, " — {}", it.reason);
            }
            let _ = writeln!(out, "  (deployment {})", it.id);
        }
    }

    out.push_str(&failure(st));
    out
}
