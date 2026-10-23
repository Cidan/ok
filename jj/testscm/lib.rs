//! An ok SCM for the jj client's end-to-end tests (docs/building.md, "Tests").
//!
//! [`endpoint`] starts one `ok scm serve -dev` per test process, over the PostgreSQL the
//! environment names as PostgreSQL's tools name it (PGHOST, PGUSER, …), serving plaintext on a
//! loopback port, and the object store `OK_TEST_OBJECTS` names (as ok's Go tests take it,
//! internal/testdb/testobjects): `s3` (the default), the S3 server AWS's tools name
//! (AWS_ENDPOINT_URL_S3, …); `gcs`, a GCS emulator this process starts on its loopback
//! (`$OK_GCS_BINARY`, else `ok-test-gcs` on the PATH); or `fs`, a directory. The SCM makes its own
//! schema in the database, and its S3 bucket, [`BUCKET`], if it does not exist. The `ok` binary is
//! `$OK_BINARY`, else `ok` on the PATH. With no ok identity behind it, a development SCM trusts the principal a caller names
//! in its `ok-dev-principal` metadata, which the jj client sends for a loopback SCM (a clone's
//! recorded principal), and which the clients here send for any principal a test acts as —
//! [`CONTROL`] included. Tests share the server and isolate by repository: each creates its own.

use std::io::BufRead as _;
use std::io::Read as _;
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ok_proto::ok::scm::v1 as pb;
use tonic::Request;
use tonic::Status;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// The principal tests act as ok control, which alone may call the SCM's `Review`; the test SCM's
/// configuration names it control's robot.
pub const CONTROL: &str = "control.ok.robot.example.com";

/// The principal tests act as ok automation, whose landing queue alone moves main (only it may
/// call `Landing`).
pub const AUTOMATION: &str = "automation.ok.robot.example.com";

/// The bucket the test SCM keeps its packs in; a development SCM makes it.
pub const BUCKET: &str = "ok-scm";

struct Server {
    endpoint: String,
    config: PathBuf,
    _child: Child,
}

static SERVER: OnceLock<Server> = OnceLock::new();
static IDENTITY: OnceLock<Server> = OnceLock::new();
static GCS: OnceLock<Emulator> = OnceLock::new();

/// The object store the test SCM keeps its packs in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Objects {
    S3,
    Gcs,
    Fs,
}

/// The object store `OK_TEST_OBJECTS` names: S3 unless it says `gcs` or `fs`.
fn objects() -> Objects {
    match std::env::var("OK_TEST_OBJECTS").as_deref() {
        Err(_) | Ok("") | Ok("s3") => Objects::S3,
        Ok("gcs") => Objects::Gcs,
        Ok("fs") => Objects::Fs,
        Ok(other) => panic!("OK_TEST_OBJECTS={other:?} is none of s3, gcs, and fs"),
    }
}

/// The GCS emulator, its address `127.0.0.1:<port>`, with [`BUCKET`] made.
struct Emulator {
    addr: String,
    _child: Child,
}

/// The people the test identity knows, from its static system of record: ada.human.example.com.
pub const IDENTITY_PEOPLE: &str = "people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n";

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is not set: the jj client's end-to-end tests need PostgreSQL and S3 (docs/building.md, \"Tests\")"
        )
    })
}

/// Whether the environment names the servers the end-to-end tests need. A test calls it first and
/// returns when it is false: the test is skipped, and says so — unless `OK_TEST_INTEGRATION` is
/// `require`, as CI sets it, when a missing server fails the test instead.
pub fn available() -> bool {
    let mut needed = vec!["PGHOST", "PGUSER"];
    if objects() == Objects::S3 {
        needed.extend([
            "AWS_ENDPOINT_URL_S3",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]);
    }
    let missing: Vec<&str> = needed
        .into_iter()
        .filter(|v| std::env::var(v).map_or(true, |x| x.is_empty()))
        .collect();
    if missing.is_empty() {
        return true;
    }
    assert!(
        std::env::var("OK_TEST_INTEGRATION").as_deref() != Ok("require"),
        "OK_TEST_INTEGRATION=require, and {} not set (docs/building.md, \"Tests\")",
        missing.join(", ")
    );
    eprintln!(
        "skipped: {} not set (docs/building.md, \"Tests\")",
        missing.join(", ")
    );
    false
}

/// The test SCM's endpoint, `http://127.0.0.1:<port>`, starting it on first use. It runs until
/// the test process ends.
pub fn endpoint() -> &'static str {
    &SERVER.get_or_init(start).endpoint
}

/// The ok binary, finding the GCS emulator, when the SCM keeps its packs there, as a GCS client
/// finds any emulator: by STORAGE_EMULATOR_HOST.
fn ok() -> Command {
    let bin = std::env::var("OK_BINARY").unwrap_or_else(|_| "ok".to_owned());
    let mut cmd = Command::new(bin);
    cmd.env_clear()
        .env("PATH", var("PATH"))
        .env("HOME", var("HOME"));
    if objects() == Objects::Gcs {
        cmd.env("STORAGE_EMULATOR_HOST", &GCS.get_or_init(start_gcs).addr);
    }
    cmd
}

/// Starts the GCS emulator on a loopback port, with [`BUCKET`]; it prints the address it listens
/// at, and runs until the test process ends.
fn start_gcs() -> Emulator {
    let bin = std::env::var("OK_GCS_BINARY").unwrap_or_else(|_| "ok-test-gcs".to_owned());
    let mut cmd = Command::new(bin);
    cmd.env_clear()
        .env("PATH", var("PATH"))
        .env("HOME", var("HOME"))
        .args(["-listen", "127.0.0.1:0", "-bucket", BUCKET])
        .stdin(Stdio::null())
        .stdout(Stdio::piped());
    let mut child = spawn_for_process(cmd)
        .expect("start the GCS emulator (is ok-test-gcs on the PATH, or OK_GCS_BINARY set?)");
    let mut addr = String::new();
    std::io::BufReader::new(child.stdout.take().expect("the GCS emulator's output"))
        .read_line(&mut addr)
        .expect("read the GCS emulator's address");
    let addr = addr.trim().to_owned();
    assert!(!addr.is_empty(), "the GCS emulator printed no address");
    Emulator {
        addr,
        _child: child,
    }
}

/// The database URL the environment's PGHOST, PGPORT, PGUSER, PGPASSWORD, and PGDATABASE name
/// (the `postgres` database unless PGDATABASE says otherwise); the SCM keeps its tables in a
/// schema of its own there.
fn database_url() -> String {
    let database = std::env::var("PGDATABASE")
        .ok()
        .filter(|d| !d.is_empty())
        .unwrap_or_else(|| "postgres".to_owned());
    let mut url = format!(
        "postgres:///{database}?host={}&user={}",
        var("PGHOST"),
        var("PGUSER")
    );
    for (key, name) in [
        ("port", "PGPORT"),
        ("password", "PGPASSWORD"),
        ("sslmode", "PGSSLMODE"),
    ] {
        if let Ok(v) = std::env::var(name).map(|v| v.trim().to_owned()).as_deref()
            && !v.is_empty()
        {
            url.push_str(&format!("&{key}={v}"));
        }
    }
    url
}

/// Writes the test SCM's configuration, and the secrets it names, into a directory of its own.
fn write_config(port: u16) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ok-scm-{}-{port}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("make the test SCM's directory");
    let secret = |name: &str, value: &str| {
        let p = dir.join(name);
        std::fs::write(&p, value).expect("write a test SCM secret");
        p.display().to_string()
    };
    let db = secret("database-url", &database_url());
    let objects = match objects() {
        Objects::S3 => {
            let access = secret("access-key", &var("AWS_ACCESS_KEY_ID"));
            let secret_key = secret("secret-key", &var("AWS_SECRET_ACCESS_KEY"));
            format!(
                "{{endpoint: {:?}, bucket: {BUCKET}, region: {:?}, access_key_file: {access:?}, secret_key_file: {secret_key:?}}}",
                var("AWS_ENDPOINT_URL_S3"),
                std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
            )
        }
        Objects::Gcs => format!(
            "{{type: gcs, endpoint: \"http://{}/storage/v1/\", bucket: {BUCKET}}}",
            GCS.get_or_init(start_gcs).addr
        ),
        Objects::Fs => {
            let packs = dir.join("objects");
            std::fs::create_dir_all(&packs).expect("make the test SCM's object directory");
            format!("{{type: fs, dir: {:?}}}", packs.display().to_string())
        }
    };
    let config = format!(
        "domain: example.com\npublic_url: http://127.0.0.1:{port}\nlisten: 127.0.0.1:{port}\nmetrics_listen: 127.0.0.1:0\n\
         roles: {{control: control.ok, automation: automation.ok}}\n\
         database: {{url_file: {db:?}}}\n\
         scm:\n  objects: {objects}\n"
    );
    let path = dir.join("ok.yaml");
    std::fs::write(&path, config).expect("write the test SCM's configuration");
    path
}

/// A development ok identity's endpoint, `http://127.0.0.1:<port>`, starting it on first use, over
/// the same PostgreSQL (its own schema) — for the tests of signing in (`jj auth`). It knows
/// [`IDENTITY_PEOPLE`], and, being a development server, trusts the principal a request names in its
/// `ok-dev-principal` header, its pages' included: the browser's half of a device login.
pub fn identity_endpoint() -> &'static str {
    &IDENTITY.get_or_init(start_identity).endpoint
}

fn start_identity() -> Server {
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("find a free loopback port")
        .port();
    let dir = std::env::temp_dir().join(format!("ok-identity-{}-{port}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("make the test identity's directory");
    let ca = dir.join("ca");
    let made = ok()
        .args(["identity", "ca", "init", "-dir"])
        .arg(&ca)
        .args(["-name", "Test"])
        .output()
        .expect("run ok identity ca init");
    assert!(
        made.status.success(),
        "ok identity ca init: {}",
        String::from_utf8_lossy(&made.stderr)
    );
    let people = dir.join("people.yaml");
    std::fs::write(&people, IDENTITY_PEOPLE).expect("write the test identity's people");
    let db = dir.join("database-url");
    std::fs::write(&db, database_url()).expect("write the test identity's database URL");
    let config = dir.join("ok.yaml");
    std::fs::write(
        &config,
        format!(
            "domain: example.com\npublic_url: http://127.0.0.1:{port}\nlisten: 127.0.0.1:{port}\nmetrics_listen: 127.0.0.1:0\n\
             database: {{url_file: {db:?}}}\n\
             identity:\n  ca_dir: {ca:?}\n  system_of_record: {{static: {{file: {people:?}}}}}\n"
        ),
    )
    .expect("write the test identity's configuration");
    let log = std::env::temp_dir().join(format!("ok-identity-{port}.log"));
    let out = std::fs::File::create(&log).expect("create the ok identity log");
    let mut cmd = ok();
    cmd.args(["identity", "serve", "-dev", "-config"])
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(out.try_clone().expect("share the ok identity log"))
        .stderr(out);
    let mut child =
        spawn_for_process(cmd).expect("start ok identity (is ok on the PATH, or OK_BINARY set?)");
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(&addr).is_ok() {
            break;
        }
        let exited = child.try_wait().expect("check on ok identity");
        if exited.is_some() || Instant::now() > deadline {
            let mut text = String::new();
            let _ = std::fs::File::open(&log).and_then(|mut f| f.read_to_string(&mut text));
            panic!("ok identity did not start ({exited:?}):\n{text}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // The sync from the system of record runs as identity starts, after it listens: until it has
    // made ada, she is no one identity knows.
    let endpoint = format!("http://{addr}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let resp = request_as(&endpoint, "GET", "/id/account", "ada.human.example.com", "");
        if resp.starts_with("HTTP/1.1 200") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ok identity never synced ada:\n{resp}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    Server {
        endpoint,
        config,
        _child: child,
    }
}

/// Posts a form to a development server's page as `principal`, over HTTP/1.1, returning the
/// response's status line and body.
pub fn post_form_as(endpoint: &str, path: &str, principal: &str, form: &[(&str, &str)]) -> String {
    let body: Vec<String> = form
        .iter()
        .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
        .collect();
    request_as(endpoint, "POST", path, principal, &body.join("&"))
}

/// Makes a request of a development server's page as `principal`, over HTTP/1.1, returning the
/// response's status line and body.
fn request_as(endpoint: &str, method: &str, path: &str, principal: &str, body: &str) -> String {
    use std::io::Write as _;
    let addr = endpoint.trim_start_matches("http://");
    let mut s = TcpStream::connect(addr).expect("reach the development server");
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nOrigin: {endpoint}\r\nok-dev-principal: {principal}\r\n\
         Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("send the request");
    let mut out = String::new();
    s.read_to_string(&mut out).expect("read the response");
    out
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Runs one garbage-collection pass over a repository (`ok scm gc`), returning its report.
pub fn gc(repository: &str) -> String {
    let server = SERVER.get_or_init(start);
    let out = ok()
        .args(["scm", "gc", "-config"])
        .arg(&server.config)
        .arg(repository)
        .stdin(Stdio::null())
        .output()
        .expect("run ok scm gc");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "ok scm gc {repository}: {}\n{text}{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    text
}

fn start() -> Server {
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("find a free loopback port")
        .port();
    let addr = format!("127.0.0.1:{port}");
    let config = write_config(port);
    let log = std::env::temp_dir().join(format!("ok-scm-{port}.log"));
    let out = std::fs::File::create(&log).expect("create the ok scm log");
    let mut cmd = ok();
    cmd.args(["scm", "serve", "-dev", "-config"])
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(out.try_clone().expect("share the ok scm log"))
        .stderr(out);
    let mut child =
        spawn_for_process(cmd).expect("start ok scm (is ok on the PATH, or OK_BINARY set?)");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(&addr).is_ok() {
            break;
        }
        let exited = child.try_wait().expect("check on ok scm");
        if exited.is_some() || Instant::now() > deadline {
            let mut text = String::new();
            let _ = std::fs::File::open(&log).and_then(|mut f| f.read_to_string(&mut text));
            panic!("ok scm did not start ({exited:?}):\n{text}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Server {
        endpoint: format!("http://{addr}"),
        config,
        _child: child,
    }
}

/// Starts `cmd` from a thread that lives as long as the test process, the child set to get SIGTERM
/// when that thread ends — at the process's end, however it ends — so the server, which a static
/// holds and Rust never drops, outlives no test process. Linux sends the parent-death signal when
/// the thread that started the child ends, so a test's own thread, which ends with its test, cannot
/// start a server the process's other tests share. Elsewhere the server is stopped by no one.
fn spawn_for_process(mut cmd: Command) -> std::io::Result<Child> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;
        let parent = std::process::id();
        // SAFETY: prctl, getppid, and _exit are async-signal-safe, as pre_exec requires.
        unsafe {
            cmd.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // The process may have ended before the signal was asked for.
                if libc::getppid() as u32 != parent {
                    libc::_exit(1);
                }
                Ok(())
            });
        }
    }
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("ok-scm-parent".to_owned())
        .spawn(move || {
            let _ = tx.send(cmd.spawn());
            loop {
                std::thread::park();
            }
        })?;
    rx.recv().map_err(std::io::Error::other)?
}

/// Names the caller in its `ok-dev-principal` metadata, which a development SCM trusts.
#[derive(Clone)]
pub struct As(MetadataValue<Ascii>);

impl Interceptor for As {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("ok-dev-principal", self.0.clone());
        Ok(req)
    }
}

pub type Svc = InterceptedService<Channel, As>;

async fn channel(principal: &str) -> (Channel, As) {
    let channel = Channel::from_shared(endpoint().to_owned())
        .expect("the test SCM's endpoint")
        .connect()
        .await
        .expect("connect to the test SCM");
    (
        channel,
        As(principal.parse().expect("a principal is a header value")),
    )
}

/// A ChangeLists client acting as `principal`.
pub async fn change_lists(principal: &str) -> pb::change_lists_client::ChangeListsClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::change_lists_client::ChangeListsClient::with_interceptor(channel, as_)
}

/// A Review client acting as `principal`: ok control's lifecycle transitions.
pub async fn review(principal: &str) -> pb::review_client::ReviewClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::review_client::ReviewClient::with_interceptor(channel, as_)
}

/// A Landing client acting as `principal`: the landing queue's candidates and landings.
pub async fn landing(principal: &str) -> pb::landing_client::LandingClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::landing_client::LandingClient::with_interceptor(channel, as_)
}

/// A LandingChecks client acting as `principal`: dry runs of a landing.
pub async fn landing_checks(
    principal: &str,
) -> pb::landing_checks_client::LandingChecksClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::landing_checks_client::LandingChecksClient::with_interceptor(channel, as_)
}

/// A Repositories client acting as `principal`.
pub async fn repositories(principal: &str) -> pb::repositories_client::RepositoriesClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::repositories_client::RepositoriesClient::with_interceptor(channel, as_)
}

/// A Workspaces client acting as `principal`.
pub async fn workspaces(principal: &str) -> pb::workspaces_client::WorkspacesClient<Svc> {
    let (channel, as_) = channel(principal).await;
    pb::workspaces_client::WorkspacesClient::with_interceptor(channel, as_)
}

/// Makes `principal`'s workspace `name` of repository `repository_id`, with an operation log of
/// its own (docs/scm.md, "Decisions", 3).
pub async fn create_workspace(principal: &str, repository_id: i64, name: &str) {
    workspaces(principal)
        .await
        .create_workspace(pb::CreateWorkspaceRequest {
            repository_id,
            workspace: name.to_owned(),
        })
        .await
        .expect("create a workspace on the test SCM");
}

/// Creates a repository named for the test, as `principal`, who becomes its creator.
pub async fn create_repository(test: &str, principal: &str) -> ok_store::Repository {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("e2e-{}-{nanos}-{test}", std::process::id());
    repositories(principal)
        .await
        .create_repository(pb::CreateRepositoryRequest {
            name,
            actor: String::new(),
        })
        .await
        .expect("create a repository on the test SCM")
        .into_inner()
        .try_into()
        .expect("the test SCM returned a repository")
}
