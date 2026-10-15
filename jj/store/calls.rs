//! What the client asks ok, and how long it waits (docs/scm.md, "The client"): every RPC
//! by method — how many, how many failed, how long they took, and the bytes each way — recorded by
//! a layer on the connection every client shares (`Measured`); every batched fetch by kind
//! (`fetched`); and how long reads waited on ok (`Shared::waits`). `jj fs status` shows the
//! daemon's; `OK_STATS=<file>` has any jj command write its own there as JSON when it exits.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use serde::Serialize;
use tower_service::Service;

/// One method's RPCs since the process started.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct CallStats {
    pub calls: u64,
    /// Calls that failed: whose status was not OK, or whose connection or stream broke. A call the
    /// client stopped reading after its last message, before its status, is not one.
    pub failures: u64,
    pub seconds: f64,
    pub max_seconds: f64,
    pub sent_bytes: u64,
    pub received_bytes: u64,
}

/// One kind's batched fetches since the process started: the batches sent, the keys they asked
/// for, and the time they took.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FetchStats {
    pub batches: u64,
    pub keys: u64,
    pub seconds: f64,
}

/// Reads that waited on ok: how many, and how long, in all and at most.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct WaitStats {
    pub reads: u64,
    pub seconds: f64,
    pub max_seconds: f64,
}

impl WaitStats {
    pub(crate) fn add(&mut self, d: Duration) {
        let s = d.as_secs_f64();
        self.reads += 1;
        self.seconds += s;
        self.max_seconds = self.max_seconds.max(s);
    }

    fn merge(&mut self, o: &WaitStats) {
        self.reads += o.reads;
        self.seconds += o.seconds;
        self.max_seconds = self.max_seconds.max(o.max_seconds);
    }
}

static CALLS: LazyLock<Mutex<BTreeMap<String, CallStats>>> = LazyLock::new(Default::default);
static FETCHES: LazyLock<Mutex<BTreeMap<&'static str, FetchStats>>> =
    LazyLock::new(Default::default);

/// Every method's RPCs so far, by `Service/Method`.
pub fn calls() -> BTreeMap<String, CallStats> {
    CALLS.lock().unwrap().clone()
}

/// Every kind's batched fetches so far.
pub fn fetches() -> BTreeMap<&'static str, FetchStats> {
    FETCHES.lock().unwrap().clone()
}

/// Records one batch of `keys` fetched for `kind` in `d`.
pub(crate) fn fetched(kind: &'static str, keys: usize, d: Duration) {
    let mut f = FETCHES.lock().unwrap();
    let s = f.entry(kind).or_default();
    s.batches += 1;
    s.keys += keys as u64;
    s.seconds += d.as_secs_f64();
}

/// The process's RPCs, fetches, and the reads of every repository it opened that waited on ok,
/// as `OK_STATS` writes them.
#[derive(Serialize)]
struct Report {
    calls: BTreeMap<String, CallStats>,
    fetches: BTreeMap<&'static str, FetchStats>,
    waits: WaitStats,
}

/// Writes the process's report to the file `OK_STATS` names, if it names one.
pub fn write_report() -> std::io::Result<()> {
    let Some(path) = std::env::var_os("OK_STATS").filter(|p| !p.is_empty()) else {
        return Ok(());
    };
    let mut waits = WaitStats::default();
    for shared in crate::shared::registered() {
        waits.merge(&shared.waits());
    }
    let report = Report {
        calls: calls(),
        fetches: fetches(),
        waits,
    };
    let json = serde_json::to_vec_pretty(&report).map_err(std::io::Error::other)?;
    std::fs::write(path, json)
}

/// `/ok.scm.v1.Store/ReadFiles` → `Store/ReadFiles`.
fn method_of(path: &str) -> String {
    let path = path.trim_start_matches('/');
    match path.split_once('/') {
        Some((service, method)) => {
            let service = service.rsplit('.').next().unwrap_or(service);
            format!("{service}/{method}")
        }
        None => path.to_owned(),
    }
}

/// The gRPC status a header map carries, if any.
fn grpc_status(headers: &http::HeaderMap) -> Option<i32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

/// A call whose status was never read: the client stopped reading it.
const UNREAD: i32 = -2;
/// A call whose connection or stream broke.
const BROKEN: i32 = -1;

/// One RPC in flight: its bytes so far, and its status once known. It is recorded once, when its
/// response ends or is dropped.
struct Call {
    method: String,
    start: Instant,
    sent: AtomicU64,
    received: AtomicU64,
    code: AtomicI32,
    done: AtomicBool,
}

impl Call {
    fn new(method: String) -> Self {
        Self {
            method,
            start: Instant::now(),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            code: AtomicI32::new(UNREAD),
            done: AtomicBool::new(false),
        }
    }

    fn finish(&self) {
        if self.done.swap(true, Ordering::AcqRel) {
            return;
        }
        let elapsed = self.start.elapsed().as_secs_f64();
        let mut calls = CALLS.lock().unwrap();
        let s = calls.entry(self.method.clone()).or_default();
        s.calls += 1;
        let code = self.code.load(Ordering::Acquire);
        if code != 0 && code != UNREAD {
            s.failures += 1;
        }
        s.seconds += elapsed;
        s.max_seconds = s.max_seconds.max(elapsed);
        s.sent_bytes += self.sent.load(Ordering::Relaxed);
        s.received_bytes += self.received.load(Ordering::Relaxed);
    }
}

/// A connection that records every RPC made over it (`calls`).
#[derive(Clone)]
pub struct Measured<S> {
    inner: S,
}

impl<S> Measured<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<http::Request<tonic::body::Body>> for Measured<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let call = Arc::new(Call::new(method_of(req.uri().path())));
        let req = req.map(|b| {
            tonic::body::Body::new(Counted {
                inner: b,
                call: call.clone(),
                response: false,
            })
        });
        let fut = self.inner.call(req);
        Box::pin(async move {
            match fut.await {
                Ok(resp) => {
                    if let Some(code) = grpc_status(resp.headers()) {
                        call.code.store(code, Ordering::Release);
                    }
                    Ok(resp.map(|b| {
                        tonic::body::Body::new(Counted {
                            inner: b,
                            call,
                            response: true,
                        })
                    }))
                }
                Err(e) => {
                    call.code.store(BROKEN, Ordering::Release);
                    call.finish();
                    Err(e)
                }
            }
        })
    }
}

/// A request or response body that counts its bytes into its call; a response's records the call
/// when it ends, fails, or is dropped.
struct Counted {
    inner: tonic::body::Body,
    call: Arc<Call>,
    response: bool,
}

impl Body for Counted {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(d) = frame.data_ref() {
                    let counter = if this.response {
                        &this.call.received
                    } else {
                        &this.call.sent
                    };
                    counter.fetch_add(d.len() as u64, Ordering::Relaxed);
                }
                if this.response
                    && let Some(code) = frame.trailers_ref().and_then(grpc_status)
                {
                    this.call.code.store(code, Ordering::Release);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.call.code.store(BROKEN, Ordering::Release);
                if this.response {
                    this.call.finish();
                }
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                if this.response {
                    this.call.finish();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        if self.response {
            self.call.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::poll_fn;

    use super::*;

    /// A body that yields the frames it is given.
    struct Frames(VecDeque<Result<Frame<Bytes>, tonic::Status>>);

    impl Body for Frames {
        type Data = Bytes;
        type Error = tonic::Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
            Poll::Ready(self.0.pop_front())
        }
    }

    fn trailers(code: i32) -> Frame<Bytes> {
        let mut t = http::HeaderMap::new();
        t.insert("grpc-status", code.to_string().parse().unwrap());
        Frame::trailers(t)
    }

    /// A connection whose every call is answered with the response `answer` makes.
    #[derive(Clone)]
    struct Answer(
        Arc<dyn Fn() -> Result<http::Response<tonic::body::Body>, std::io::Error> + Send + Sync>,
    );

    impl Service<http::Request<tonic::body::Body>> for Answer {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::io::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: http::Request<tonic::body::Body>) -> Self::Future {
            std::future::ready((self.0)())
        }
    }

    /// Makes one call of `method` (its own, so other tests' calls do not mix in) over a connection
    /// answering with `frames` (and `headers`), reads `read` frames of the answer, drops it, and
    /// returns the method's counts.
    fn call(
        method: &str,
        headers: &[(&'static str, &'static str)],
        frames: Vec<Result<Frame<Bytes>, tonic::Status>>,
        read: usize,
    ) -> CallStats {
        let frames = Arc::new(Mutex::new(Some(frames)));
        let headers: Vec<_> = headers.to_vec();
        let mut conn = Measured::new(Answer(Arc::new(move || {
            let mut resp = http::Response::new(tonic::body::Body::new(Frames(
                frames.lock().unwrap().take().unwrap().into(),
            )));
            for (k, v) in &headers {
                resp.headers_mut().insert(*k, v.parse().unwrap());
            }
            Ok(resp)
        })));
        let req = http::Request::builder()
            .uri(format!("http://ok/ok.test.v1.Test/{method}"))
            .body(tonic::body::Body::empty())
            .unwrap();
        futures::executor::block_on(async {
            let mut resp = conn.call(req).await.unwrap();
            let body = resp.body_mut();
            for _ in 0..read {
                poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await;
            }
        });
        calls()[&format!("Test/{method}")].clone()
    }

    #[test]
    fn a_call_is_counted_with_its_bytes_and_its_status() {
        let data = || Ok(Frame::data(Bytes::from_static(b"twelve bytes")));
        let ok = call("Ok", &[], vec![data(), Ok(trailers(0))], 3);
        assert_eq!((ok.calls, ok.failures, ok.received_bytes), (1, 0, 12));
        assert!(ok.seconds > 0.0 && ok.max_seconds == ok.seconds);
        let unread = call("StoppedReading", &[], vec![data(), Ok(trailers(0))], 1);
        assert_eq!(
            (unread.calls, unread.failures, unread.received_bytes),
            (1, 0, 12),
            "a call the client stopped reading before its status is no failure"
        );
        let refused = call("Refused", &[], vec![data(), Ok(trailers(5))], 3);
        assert_eq!((refused.calls, refused.failures), (1, 1));
        let at_once = call("RefusedAtOnce", &[("grpc-status", "14")], vec![], 1);
        assert_eq!((at_once.calls, at_once.failures), (1, 1));
        let broken = call(
            "Broken",
            &[],
            vec![data(), Err(tonic::Status::unavailable("reset"))],
            2,
        );
        assert_eq!((broken.calls, broken.failures), (1, 1));
    }

    #[test]
    fn a_call_whose_connection_fails_is_a_failure() {
        let mut conn = Measured::new(Answer(Arc::new(|| Err(std::io::Error::other("no route")))));
        let req = http::Request::builder()
            .uri("http://ok/ok.test.v1.Test/NoConnection")
            .body(tonic::body::Body::empty())
            .unwrap();
        assert!(futures::executor::block_on(conn.call(req)).is_err());
        let c = &calls()["Test/NoConnection"];
        assert_eq!((c.calls, c.failures), (1, 1));
    }

    #[test]
    fn methods_are_named_by_service_and_method() {
        assert_eq!(method_of("/ok.scm.v1.Store/ReadFiles"), "Store/ReadFiles");
        assert_eq!(method_of("/Plain/Call"), "Plain/Call");
    }
}
