//! The control socket (docs/okfs.md, "The control socket"): length-prefixed `Request`s and
//! `Response`s between jj and the daemon, over `$XDG_RUNTIME_DIR/okfs/control.sock`.

use std::io;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use ok_proto::ok::okfs::v1 as pb;

/// The largest message either side sends (a snapshot listing of a huge directory, a checkout's
/// materialized conflicts).
pub const MAX_MESSAGE: usize = 256 << 20;

/// Where the daemon keeps its state and its socket.
#[derive(Clone, Debug)]
pub struct Paths {
    /// `~/.local/share/ok`: workspaces (each with its own repository), the registry, the
    /// machine's ID.
    pub state: PathBuf,
    /// `$XDG_RUNTIME_DIR/okfs`: the control socket and the daemon's lock.
    pub runtime: PathBuf,
}

impl Paths {
    /// The paths for this user: `OKFS_STATE_DIR` and `OKFS_RUNTIME_DIR` override them (tests).
    pub fn from_env() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        let state = std::env::var_os("OKFS_STATE_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("XDG_DATA_HOME").map(|d| PathBuf::from(d).join("ok")))
            .unwrap_or_else(|| home.join(".local/share/ok"));
        let runtime = std::env::var_os("OKFS_RUNTIME_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("XDG_RUNTIME_DIR").map(|d| PathBuf::from(d).join("okfs")))
            .unwrap_or_else(|| PathBuf::from(format!("/tmp/okfs-{}", unsafe { libc::geteuid() })));
        Self { state, runtime }
    }

    pub fn socket(&self) -> PathBuf {
        self.runtime.join("control.sock")
    }

    pub fn workspaces(&self) -> PathBuf {
        self.state.join("workspaces")
    }

    pub fn workspace(&self, id: &str) -> PathBuf {
        self.workspaces().join(id)
    }

    pub fn registry(&self) -> PathBuf {
        self.state.join("workspaces.toml")
    }

    /// Presentations' state (docs/okfs.md, "ok's roles"): `repos/<id>/store`, each
    /// repository's bare store, and `mounts/<hash>`, each mount's overlay.
    pub fn presented(&self) -> PathBuf {
        self.state.join("presented")
    }
}

/// Makes a directory only its owner can enter.
pub fn private_dir(p: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::create_dir_all(p)?;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700))
}

pub fn write_frame(w: &mut impl io::Write, msg: &impl prost::Message) -> io::Result<()> {
    let body = msg.encode_to_vec();
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    w.write_all(&buf)?;
    w.flush()
}

/// Reads one frame; None at a clean end of stream.
pub fn read_frame<M: prost::Message + Default>(r: &mut impl io::Read) -> io::Result<Option<M>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_MESSAGE {
        return Err(io::Error::other(format!("a control message of {n} bytes")));
    }
    let mut body = vec![0u8; n];
    r.read_exact(&mut body)?;
    M::decode(body.as_slice())
        .map(Some)
        .map_err(io::Error::other)
}

/// A connection to the daemon.
pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub fn connect(paths: &Paths) -> io::Result<Self> {
        let stream = UnixStream::connect(paths.socket())?;
        Ok(Self { stream })
    }

    /// Sends a request and reads its response; the daemon's refusal is an error.
    pub fn call(&mut self, req: pb::request::Request) -> io::Result<pb::response::Response> {
        write_frame(&mut self.stream, &pb::Request { request: Some(req) })?;
        let resp: pb::Response = read_frame(&mut self.stream)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the okfs daemon closed the connection",
            )
        })?;
        if !resp.error.is_empty() {
            return Err(io::Error::other(resp.error));
        }
        resp.response
            .ok_or_else(|| io::Error::other("the okfs daemon answered nothing"))
    }

    /// The connection, to send a request whose responses stream (`Subscribe`).
    pub fn stream(&mut self) -> &mut UnixStream {
        &mut self.stream
    }

    /// The next streamed response (a subscription's event).
    pub fn next_response(&mut self) -> io::Result<Option<pb::Response>> {
        read_frame(&mut self.stream)
    }

    pub fn set_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(d)
    }
}
