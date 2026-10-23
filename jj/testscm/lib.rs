//! An ok SCM for the jj client's end-to-end tests (docs/building.md, "Tests").
//!
//! [`endpoint`] starts one `ok serve -dev -roles identity,scm,secrets` per test process — the SCM,
//! the ok identity that knows its callers and names its repositories, and ok secrets under a
//! keyring root, on one port — over the PostgreSQL the
//! environment names as PostgreSQL's tools name it (PGHOST, PGUSER, …), serving plaintext on a
//! loopback port, and the object store `OK_TEST_OBJECTS` names (as ok's Go tests take it,
//! internal/testdb/testobjects): `s3` (the default), the S3 server AWS's tools name
//! (AWS_ENDPOINT_URL_S3, …); `gcs`, a GCS emulator this process starts on its loopback
//! (`$OK_GCS_BINARY`, else `ok-test-gcs` on the PATH); or `fs`, a directory. The SCM makes its own
//! schema in the database, and its S3 bucket, [`BUCKET`], if it does not exist. The `ok` binary is
//! `$OK_BINARY`, else `ok` on the PATH. Its identity knows [`IDENTITY_PEOPLE`] and ok's roles'
//! robots; being a development server, it trusts the principal a caller names in its
//! `ok-dev-principal` metadata, which the jj client sends for a loopback SCM (a clone's recorded
//! principal), and which the clients here send for any principal a test acts as — [`CONTROL`]
//! included. Tests share the server and isolate by repository: each creates its own.

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

/// The ACLs the test identity's tree declares for the tests of secrets (docs/secrets.md): ada and
/// alice read, alice writes; bob is in neither. e2e administers the tree.
pub const SECRET_READERS: &str = "secret-readers";
pub const SECRET_WRITERS: &str = "secret-writers";

/// The principal tests act as the deployer, which alone applies the groups-and-ACLs tree.
pub const DEPLOY: &str = "deploy.ok.robot.example.com";

struct Server {
    endpoint: String,
    config: PathBuf,
    _child: Child,
}

static SERVER: OnceLock<Server> = OnceLock::new();
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

/// The people the test identity knows, from its static system of record: ada, alice, bob, and e2e,
/// each <handle>.human.example.com.
pub const IDENTITY_PEOPLE: &str = "people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n  \
    - {id: \"2\", email: alice@example.com, name: Alice}\n  - {id: \"3\", email: bob@example.com, name: Bob}\n  \
    - {id: \"4\", email: e2e@example.com, name: E2E}\n";

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is not set: the jj client's end-to-end tests need PostgreSQL and S3 (docs/building.md, \"Tests\")"
        )
    })
}

/// The stable ID ok's roles know a principal by, as the test identity minted it.
pub async fn id(principal: &str) -> String {
    let (channel, as_) = channel(CONTROL).await;
    ok_proto::ok::identity::v1::directory_client::DirectoryClient::with_interceptor(channel, as_)
        .get_principal(ok_proto::ok::identity::v1::GetPrincipalRequest {
            key: Some(
                ok_proto::ok::identity::v1::get_principal_request::Key::Name(principal.to_owned()),
            ),
        })
        .await
        .unwrap_or_else(|e| panic!("the test identity's {principal}: {e}"))
        .into_inner()
        .principal
        .expect("the test identity answered a principal")
        .id
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

/// Writes the test server's configuration, and the secrets, identity CA, and people it names, into
/// a directory of its own.
fn write_config(port: u16) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ok-scm-{}-{port}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("make the test SCM's directory");
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
    let secret = |name: &str, value: &str| {
        let p = dir.join(name);
        std::fs::write(&p, value).expect("write a test SCM secret");
        p.display().to_string()
    };
    let db = secret("database-url", &database_url());
    let key = ok()
        .args(["secrets", "keyring", "-id", "test"])
        .output()
        .expect("run ok secrets keyring");
    assert!(
        key.status.success(),
        "ok secrets keyring: {}",
        String::from_utf8_lossy(&key.stderr)
    );
    let keyring = dir.join("keyring");
    std::fs::write(&keyring, &key.stdout).expect("write the test keyring");
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
         roles: {{identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, deploy: deploy.ok, secrets: secrets.ok}}\n\
         database: {{url_file: {db:?}}}\n\
         identity:\n  ca_dir: {ca:?}\n  system_of_record: {{static: {{file: {people:?}}}}}\n\
         scm:\n  objects: {objects}\n\
         secrets:\n  root: {:?}\n",
        format!("keyring:{}", keyring.display())
    );
    let path = dir.join("ok.yaml");
    std::fs::write(&path, config).expect("write the test SCM's configuration");
    path
}

/// The test identity's endpoint — the test SCM's, whose process serves both — for the tests of
/// signing in (`jj auth`). Being a development server, it trusts the principal a request names in
/// its `ok-dev-principal` header, its pages' included: the browser's half of a device login.
pub fn identity_endpoint() -> &'static str {
    endpoint()
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
    for _ in 0..PORT_TRIES {
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let config = write_config(port);
        let log = std::env::temp_dir().join(format!("ok-scm-{port}.log"));
        let out = std::fs::File::create(&log).expect("create the ok scm log");
        let mut cmd = ok();
        cmd.args(["serve", "-dev", "-roles", "identity,scm,secrets", "-config"])
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(out.try_clone().expect("share the ok scm log"))
            .stderr(out);
        let mut child =
            spawn_for_process(cmd).expect("start ok serve (is ok on the PATH, or OK_BINARY set?)");
        if wait_serving(&mut child, &log, &addr, "ok serve") == Started::Serving {
            let endpoint = format!("http://{addr}");
            await_people(&endpoint);
            seed_tree(&endpoint);
            return Server {
                endpoint,
                config,
                _child: child,
            };
        }
    }
    panic!("ok scm lost {PORT_TRIES} ports in a row to other processes as it started");
}

/// Waits for the sync from the system of record, which runs as identity starts, after it listens:
/// until it has made each of [`IDENTITY_PEOPLE`], they are no one identity knows.
fn await_people(endpoint: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for person in [
        "ada.human.example.com",
        "alice.human.example.com",
        "bob.human.example.com",
        "e2e.human.example.com",
    ] {
        loop {
            let resp = request_as(endpoint, "GET", "/id/account", person, "");
            if resp.starts_with("HTTP/1.1 200") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ok identity never synced {person}:\n{resp}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Applies the test identity's groups-and-ACLs tree, as the deployer: the ACLs the tests of
/// secrets name. The tests' own runtime may be the caller's, so it runs on a thread of its own.
fn seed_tree(endpoint: &str) {
    use ok_proto::ok::identity::v1 as idpb;
    let endpoint = endpoint.to_owned();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime to apply the test tree");
        rt.block_on(async move {
            let channel = Channel::from_shared(endpoint)
                .expect("the test SCM's endpoint")
                .connect()
                .await
                .expect("connect to the test SCM");
            let file = |path: &str, members: &str| idpb::File {
                path: path.to_owned(),
                content: format!("members: [{members}]\n").into_bytes(),
            };
            idpb::tree_client::TreeClient::with_interceptor(channel, As(DEPLOY.parse().unwrap()))
                .apply(idpb::ApplyRequest {
                    commit: "seed".to_owned(),
                    sequence: 1,
                    files: vec![
                        file("acl.yaml", "e2e.human.example.com"),
                        file(
                            &format!("{SECRET_READERS}/acl.yaml"),
                            "ada.human.example.com, alice.human.example.com",
                        ),
                        file(
                            &format!("{SECRET_WRITERS}/acl.yaml"),
                            "alice.human.example.com",
                        ),
                    ],
                })
                .await
                .expect("apply the test identity's tree");
        });
    })
    .join()
    .expect("applying the test tree");
}

/// How often a server is started again on another port when another process took the one it was
/// given as it started.
const PORT_TRIES: usize = 5;

/// A loopback port no one was listening on just now. Another process may bind it before the
/// server given it does: wait_serving tells.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("find a free loopback port")
        .port()
}

/// How a server this process started came up.
#[derive(PartialEq, Eq)]
enum Started {
    Serving,
    /// It found its port taken, and exited: another process bound it first.
    PortTaken,
}

/// Waits until `child`, logging to `log`, says it serves at `addr`. Its own log says so, once it
/// holds the port: a server another process runs on the port — another test process's, or its
/// metrics — is never taken for it, which a connection to the port could not tell. A child that
/// exits having found its port taken is told apart, to start again on another; any other exit, or
/// a minute without serving, fails the test with its log.
fn wait_serving(child: &mut Child, log: &std::path::Path, addr: &str, what: &str) -> Started {
    let serving = format!("serve: serving addr={addr} ");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        if text.contains(&serving) {
            return Started::Serving;
        }
        if let Some(status) = child.try_wait().expect("check on a test server") {
            if text.contains("address already in use") {
                return Started::PortTaken;
            }
            panic!("{what} did not start ({status}):\n{text}");
        }
        assert!(
            Instant::now() < deadline,
            "{what} did not start in a minute:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(50));
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

/// ok secrets' Secrets client acting as `principal`.
pub async fn secrets(
    principal: &str,
) -> ok_proto::ok::secrets::v1::secrets_client::SecretsClient<Svc> {
    let (channel, as_) = channel(principal).await;
    ok_proto::ok::secrets::v1::secrets_client::SecretsClient::with_interceptor(channel, as_)
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

/// Makes a personal token of `principal`'s reaching `repositories` (full names) with at most
/// `role`, returning its secret.
pub async fn create_token(principal: &str, repositories: &[&str], role: &str) -> String {
    use ok_proto::ok::identity::v1 as idpb;
    let (channel, as_) = channel(principal).await;
    idpb::tokens_client::TokensClient::with_interceptor(channel, as_)
        .create_token(idpb::CreateTokenRequest {
            label: "a test's".to_owned(),
            role: role.to_owned(),
            repositories: repositories.iter().map(|r| (*r).to_owned()).collect(),
            lifetime_days: 1,
        })
        .await
        .expect("make a personal token")
        .into_inner()
        .secret
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
            actor_id: String::new(),
        })
        .await
        .expect("create a repository on the test SCM")
        .into_inner()
        .try_into()
        .expect("the test SCM returned a repository")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shell standing in for a server: it writes `out` to its log, then sleeps or exits.
    fn stand_in(log: &std::path::Path, script: &str) -> Child {
        let out = std::fs::File::create(log).unwrap();
        Command::new("sh")
            .args(["-c", script])
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .unwrap()
    }

    #[test]
    fn a_server_is_serving_once_its_own_log_says_so_at_its_address() {
        let dir = tempfile_dir("serving");
        let log = dir.join("log");
        // Its own line, for another address first: still not serving at ours.
        let mut child = stand_in(
            &log,
            "echo 'INFO serve: serving addr=127.0.0.1:12345 services=1'; sleep 0.3; \
             echo 'INFO serve: serving addr=127.0.0.1:1234 services=1'; sleep 5",
        );
        assert!(
            wait_serving(&mut child, &log, "127.0.0.1:1234", "the stand-in") == Started::Serving
        );
        let _ = child.kill();
    }

    #[test]
    fn a_server_that_lost_its_port_is_told_apart() {
        let dir = tempfile_dir("taken");
        let log = dir.join("log");
        let mut child = stand_in(
            &log,
            "echo 'ok: serve: listen 127.0.0.1:1234: bind: address already in use'; exit 1",
        );
        assert!(
            wait_serving(&mut child, &log, "127.0.0.1:1234", "the stand-in") == Started::PortTaken
        );
    }

    #[test]
    #[should_panic(expected = "the stand-in did not start")]
    fn a_server_that_exits_otherwise_fails_the_test() {
        let dir = tempfile_dir("failed");
        let log = dir.join("log");
        let mut child = stand_in(&log, "echo 'ok: the database: refused'; exit 1");
        wait_serving(&mut child, &log, "127.0.0.1:1234", "the stand-in");
    }

    fn tempfile_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ok-testscm-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
