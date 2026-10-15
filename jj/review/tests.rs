use crate::pb;
use crate::render::*;
use pb::Stage;
use pb::WaitingOn;

fn p(stage: Stage, waiting_on: WaitingOn) -> Point {
    Point { stage, waiting_on }
}

/// Following stops where it was asked to, or sooner: at a failure, or where it waits on a
/// person — never hanging on a review nobody is giving.
#[test]
fn outcomes() {
    use Outcome::*;
    use Until::*;
    let cases = [
        (p(Stage::PreRunning, WaitingOn::Automation), Settled, None),
        (p(Stage::PreRunning, WaitingOn::Automation), Deployed, None),
        (
            p(Stage::PreFailed, WaitingOn::Author),
            Settled,
            Some(Failed),
        ),
        (
            p(Stage::PreFailed, WaitingOn::Author),
            Deployed,
            Some(Failed),
        ),
        (p(Stage::Draft, WaitingOn::Author), Pre, Some(Reached)),
        (
            p(Stage::Draft, WaitingOn::Author),
            Settled,
            Some(NeedsPerson),
        ),
        (
            p(Stage::Draft, WaitingOn::Author),
            Landed,
            Some(NeedsPerson),
        ),
        (p(Stage::InReview, WaitingOn::Reviewers), Pre, Some(Reached)),
        (
            p(Stage::InReview, WaitingOn::Reviewers),
            Submittable,
            Some(NeedsPerson),
        ),
        (
            p(Stage::Submittable, WaitingOn::Author),
            Submittable,
            Some(Reached),
        ),
        (
            p(Stage::Submittable, WaitingOn::Author),
            Deployed,
            Some(NeedsPerson),
        ),
        (p(Stage::Submitted, WaitingOn::Automation), Deployed, None),
        (
            p(Stage::Submitted, WaitingOn::Automation),
            Submittable,
            Some(Reached),
        ),
        (
            p(Stage::PostFailed, WaitingOn::Author),
            Submittable,
            Some(Failed),
        ),
        (p(Stage::PostFailed, WaitingOn::Author), Pre, Some(Reached)),
        (
            p(Stage::Landed, WaitingOn::Automation),
            Landed,
            Some(Reached),
        ),
        (p(Stage::Landed, WaitingOn::Automation), Deployed, None),
        (p(Stage::Deploying, WaitingOn::Automation), Settled, None),
        (
            p(Stage::Deploying, WaitingOn::Admins),
            Deployed,
            Some(NeedsPerson),
        ),
        (
            p(Stage::DeployFailed, WaitingOn::Author),
            Landed,
            Some(Reached),
        ),
        (
            p(Stage::DeployFailed, WaitingOn::Author),
            Deployed,
            Some(Failed),
        ),
        (
            p(Stage::Deployed, WaitingOn::Nobody),
            Settled,
            Some(Reached),
        ),
        (
            p(Stage::Deployed, WaitingOn::Nobody),
            Deployed,
            Some(Reached),
        ),
        (
            p(Stage::Abandoned, WaitingOn::Nobody),
            Settled,
            Some(Failed),
        ),
        (p(Stage::Abandoned, WaitingOn::Nobody), Pre, Some(Failed)),
    ];
    for (point, until, want) in cases {
        assert_eq!(outcome(point, until), want, "{point:?} until {until:?}");
    }
    assert_eq!(
        (
            Reached.code(),
            Failed.code(),
            NeedsPerson.code(),
            TIMED_OUT,
            UNREACHABLE
        ),
        (0, 1, 2, 3, 4)
    );
}

/// The JSON a status is written as (ok control's REST form) gives the same point.
#[test]
fn point_from_json() {
    let v: serde_json::Value = serde_json::from_str(
        r#"{"stage":"STAGE_DEPLOYING","waitingOn":"WAITING_ON_ADMINS","summary":"x"}"#,
    )
    .unwrap();
    assert_eq!(
        Point::from_json(&v),
        Some(p(Stage::Deploying, WaitingOn::Admins))
    );
    let bad: serde_json::Value = serde_json::from_str(r#"{"stage":"STAGE_NOPE"}"#).unwrap();
    assert_eq!(Point::from_json(&bad), None);
}

fn status() -> pb::ClStatus {
    pb::ClStatus {
        cl: Some(pb::Cl {
            number: 87,
            title: "feat(x): y".into(),
            author: "ann".into(),
            status: pb::cl::Status::Open as i32,
            patchset: 3,
            url: "https://ok.example.com/acme/cl/87".into(),
            ..Default::default()
        }),
        stage: Stage::InReview as i32,
        waiting_on: WaitingOn::Reviewers as i32,
        waiting_for: vec!["bea".into(), "cy".into()],
        summary: "In review; pre passed (7/7); needs an LGTM".into(),
        ..Default::default()
    }
}

#[test]
fn lines() {
    assert_eq!(
        line(&status()),
        "cl/87  in review      In review; pre passed (7/7); needs an LGTM  [waiting on reviewers: bea, cy]"
    );
    let mut st = status();
    st.stage = Stage::Submitted as i32;
    st.waiting_on = WaitingOn::Automation as i32;
    st.waiting_for.clear();
    st.summary = "Submitted: testing post".into();
    assert_eq!(line(&st), "cl/87  submitted      Submitted: testing post");
}

#[test]
fn failures_show_their_log() {
    let mut st = status();
    st.stage = Stage::PreFailed as i32;
    st.failure = Some(pb::Failure {
        what: "pre".into(),
        name: "test".into(),
        run: 5013,
        reason: "exit 1".into(),
        log_tail: "--- FAIL: TestX\nFAIL".into(),
        url: "https://ok.example.com/acme/cl/87/run/5013".into(),
        ..Default::default()
    });
    let f = failure(&st);
    assert_eq!(
        f,
        "cl/87  pre failed: test — exit 1\n  https://ok.example.com/acme/cl/87/run/5013\n  log: jj cl log --cl 87 5013\n  │ --- FAIL: TestX\n  │ FAIL\n"
    );
    assert!(full(&st).ends_with(&f));
}

fn check(name: &str, kind: &str, targets: &[&str]) -> pb::Check {
    pb::Check {
        name: name.into(),
        kind: kind.into(),
        targets: targets.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    }
}

/// A check is shown by what it runs, never its kind or target twice.
#[test]
fn checks_by_what_they_run() {
    assert_eq!(
        check_what(&check("test //x:test", "test", &["//x:test"])),
        "//x:test"
    );
    assert_eq!(
        check_what(&check("build", "build", &["//a:a", "//b:b"])),
        "//a:a //b:b"
    );
    assert_eq!(check_what(&check("plan", "plan", &[])), "");
    assert_eq!(
        check_what(&check("unit", "test", &["//x:test"])),
        "unit //x:test"
    );
    assert_eq!(
        check_what(&check("e2e //x:e2e", "run", &["//x:e2e"])),
        "e2e //x:e2e"
    );
}

/// Status lists only the checks that have not passed; a closed CL's review is not re-evaluated,
/// so it shows no LGTM line.
#[test]
fn full_status() {
    let mut st = status();
    st.pre = Some(pb::Checks {
        verdict: pb::checks::Verdict::Pending as i32,
        patchset: 3,
        passed: 1,
        total: 2,
        runs: vec![
            pb::Check {
                status: pb::check::Status::Passed as i32,
                run: 1,
                ..check("plan", "plan", &[])
            },
            pb::Check {
                status: pb::check::Status::Running as i32,
                run: 2,
                ..check("test //x:test", "test", &["//x:test"])
            },
        ],
        ..Default::default()
    });
    st.review = Some(pb::Review {
        approved: false,
        reviewers: vec!["bea".into()],
        ..Default::default()
    });
    let out = full(&st);
    assert!(
        out.contains("pre (patchset 3): running 1/2\n  running   test   //x:test  (run 2)\n"),
        "{out}"
    );
    assert!(!out.contains("(run 1)"), "{out}");
    assert!(out.contains("  LGTM       no\n"), "{out}");

    st.cl.as_mut().unwrap().status = pb::cl::Status::Submitted as i32;
    st.review.as_mut().unwrap().approved = true;
    let out = full(&st);
    assert!(
        out.contains("review: approved\n") && !out.contains("  LGTM  "),
        "{out}"
    );
}
