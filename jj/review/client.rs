//! The client of ok control's ChangeLists service (proto/ok/control/v1): a CL's status,
//! followed as it changes, and every action of its lifecycle.
//!
//! jj runs its commands on its own executor; tonic needs a tokio runtime. Every call therefore
//! runs on the store's runtime (`ok_store::shared::run`), and a stream's messages reach the
//! command through a channel, which any executor can await.

use bytes::Bytes;
use http_body_util::BodyExt as _;
use ok_proto::ok::control::v1 as pb;
use ok_store::client::Headers;
use ok_store::shared;
use tonic::Status;
use tonic::codegen::http;
use tower::ServiceExt as _;

pub use pb::ClRef;
pub use pb::ClStatus;

type Client = pb::change_lists_client::ChangeListsClient<ok_store::client::Svc>;

/// The largest message the service answers: a status with every run of a big pre.
const MAX_MESSAGE: usize = 64 << 20;

/// What a stream delivers.
pub enum Event<T> {
    /// The next message.
    Item(T),
    /// It ended normally.
    End,
    /// It failed.
    Failed(Status),
}

/// A connection to ok control, as this machine's user.
#[derive(Clone)]
pub struct Control {
    client: Client,
    channel: ok_store::client::Conn,
    headers: Headers,
    endpoint: String,
}

impl Control {
    /// Connects to ok control at `endpoint` (`ok.web-url`, e.g. `https://ok.example.com`) about
    /// `repository`'s CLs (its full name), which every request names for the edge in front to
    /// route by.
    pub async fn connect(endpoint: &str, repository: &str) -> Result<Control, Status> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let repository = repository.to_owned();
        shared::run(async move {
            let (channel, headers) = ok_store::client::connect(&endpoint, None).await?;
            let headers = headers.for_repository(&repository);
            let client = pb::change_lists_client::ChangeListsClient::with_interceptor(
                channel.clone(),
                headers.clone(),
            )
            .max_decoding_message_size(MAX_MESSAGE);
            Ok(Control {
                client,
                channel,
                headers,
                endpoint,
            })
        })
        .await
    }

    /// Runs one unary call on the store runtime, again while its repository's network is frozen
    /// between cells (ok_store::client::while_frozen).
    async fn call<Req, Resp, F, Fut>(&self, req: Req, f: F) -> Result<Resp, Status>
    where
        Req: Clone + Send + Sync + 'static,
        Resp: Send + 'static,
        F: Fn(Client, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<tonic::Response<Resp>, Status>> + Send,
    {
        let client = self.client.clone();
        shared::run(async move {
            ok_store::client::while_frozen(|| f(client.clone(), req.clone()))
                .await
                .map(tonic::Response::into_inner)
        })
        .await
    }

    pub async fn status(&self, cl: ClRef) -> Result<ClStatus, Status> {
        self.call(cl, |mut c, r| async move { c.status(r).await })
            .await
    }

    pub async fn request_review(&self, req: pb::ReviewersRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.request_review(r).await })
            .await
    }

    pub async fn return_to_draft(&self, cl: ClRef) -> Result<ClStatus, Status> {
        self.call(cl, |mut c, r| async move { c.return_to_draft(r).await })
            .await
    }

    pub async fn add_reviewers(&self, req: pb::ReviewersRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.add_reviewers(r).await })
            .await
    }

    pub async fn remove_reviewers(&self, req: pb::ReviewersRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.remove_reviewers(r).await })
            .await
    }

    pub async fn reply(&self, req: pb::ReplyRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.reply(r).await })
            .await
    }

    pub async fn comment(&self, req: pb::CommentRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.comment(r).await })
            .await
    }

    pub async fn resolve(&self, req: pb::ResolveRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.resolve(r).await })
            .await
    }

    pub async fn submit(&self, req: pb::SubmitRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.submit(r).await })
            .await
    }

    pub async fn cancel_submit(&self, cl: ClRef) -> Result<ClStatus, Status> {
        self.call(cl, |mut c, r| async move { c.cancel_submit(r).await })
            .await
    }

    pub async fn abandon(&self, req: pb::AbandonRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.abandon(r).await })
            .await
    }

    pub async fn restore(&self, cl: ClRef) -> Result<ClStatus, Status> {
        self.call(cl, |mut c, r| async move { c.restore(r).await })
            .await
    }

    pub async fn rollback(&self, req: pb::RollbackRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.rollback(r).await })
            .await
            .and_then(|r| {
                r.rollback.ok_or_else(|| {
                    Status::internal("ok control answered a rollback without its CL")
                })
            })
    }

    pub async fn rerun(&self, req: pb::RunsRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.rerun(r).await })
            .await
    }

    pub async fn run_pre(&self, cl: ClRef) -> Result<ClStatus, Status> {
        self.call(cl, |mut c, r| async move { c.run_pre(r).await })
            .await
    }

    pub async fn cancel_runs(&self, req: pb::RunsRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.cancel_runs(r).await })
            .await
    }

    pub async fn retry_deployment(&self, req: pb::DeploymentRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.retry_deployment(r).await })
            .await
    }

    pub async fn halt_deployment(&self, req: pb::DeploymentRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.halt_deployment(r).await })
            .await
    }

    pub async fn allow_hazards(&self, req: pb::DeploymentRequest) -> Result<ClStatus, Status> {
        self.call(req, |mut c, r| async move { c.allow_hazards(r).await })
            .await
    }

    /// Follows a CL's status: each message the service streams, then the end.
    pub fn watch(&self, cl: ClRef) -> tokio::sync::mpsc::UnboundedReceiver<Event<ClStatus>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut client = self.client.clone();
        shared::spawn(async move {
            match client.watch(cl).await {
                Err(s) => {
                    let _ = tx.send(Event::Failed(s));
                }
                Ok(resp) => {
                    let mut stream = resp.into_inner();
                    loop {
                        match stream.message().await {
                            Ok(Some(m)) => {
                                if tx.send(Event::Item(m)).is_err() {
                                    return;
                                }
                            }
                            Ok(None) => {
                                let _ = tx.send(Event::End);
                                return;
                            }
                            Err(s) => {
                                let _ = tx.send(Event::Failed(s));
                                return;
                            }
                        }
                    }
                }
            }
        });
        rx
    }

    /// Reads a run's log: its chunks as they come, then the end.
    pub fn read_log(
        &self,
        req: pb::ReadLogRequest,
    ) -> tokio::sync::mpsc::UnboundedReceiver<Event<pb::LogChunk>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut client = self.client.clone();
        shared::spawn(async move {
            match client.read_log(req).await {
                Err(s) => {
                    let _ = tx.send(Event::Failed(s));
                }
                Ok(resp) => {
                    let mut stream = resp.into_inner();
                    loop {
                        match stream.message().await {
                            Ok(Some(m)) => {
                                if tx.send(Event::Item(m)).is_err() {
                                    return;
                                }
                            }
                            Ok(None) => {
                                let _ = tx.send(Event::End);
                                return;
                            }
                            Err(s) => {
                                let _ = tx.send(Event::Failed(s));
                                return;
                            }
                        }
                    }
                }
            }
        });
        rx
    }

    /// Calls a method of the service through its REST surface (the same server and
    /// authorization), streaming each line it answers: the JSON objects a person or an agent
    /// reads, exactly as ok control writes them. A unary method answers one object; a
    /// streaming method one `{"result": …}` per message, or `{"error": …}`.
    pub fn rest(
        &self,
        method: &str,
        body: serde_json::Value,
    ) -> tokio::sync::mpsc::UnboundedReceiver<Event<String>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let uri = format!("{}/ok.control.v1.ChangeLists/{method}", self.endpoint);
        let channel = self.channel.clone();
        let headers = self.headers.clone();
        shared::spawn(async move {
            let mut req = http::Request::builder()
                .method(http::Method::POST)
                .uri(&uri)
                .header(http::header::CONTENT_TYPE, "application/json");
            if let Some(p) = headers.dev_principal() {
                req = req.header(ok_store::client::DEV_PRINCIPAL_KEY, p.as_bytes());
            }
            if let Some(r) = headers.repository() {
                req = req.header(ok_store::client::REPOSITORY_KEY, r.as_bytes());
            }
            let body =
                tonic::body::Body::new(http_body_util::Full::new(Bytes::from(body.to_string())));
            let req = match req.body(body) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Failed(Status::internal(format!(
                        "REST request: {e}"
                    ))));
                    return;
                }
            };
            let resp = match channel.oneshot(req).await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Failed(Status::unavailable(format!("{uri}: {e}"))));
                    return;
                }
            };
            let ok = resp.status().is_success();
            let mut body = resp.into_body();
            let mut pending = Vec::<u8>::new();
            let mut lines = Vec::new();
            loop {
                match body.frame().await {
                    None => break,
                    Some(Err(e)) => {
                        let _ = tx.send(Event::Failed(Status::unavailable(format!("{uri}: {e}"))));
                        return;
                    }
                    Some(Ok(frame)) => {
                        let Ok(data) = frame.into_data() else {
                            continue;
                        };
                        pending.extend_from_slice(&data);
                        while let Some(i) = pending.iter().position(|&b| b == b'\n') {
                            let line: Vec<u8> = pending.drain(..=i).collect();
                            let line = String::from_utf8_lossy(&line).trim().to_owned();
                            if line.is_empty() {
                                continue;
                            }
                            if ok {
                                if tx.send(Event::Item(line)).is_err() {
                                    return;
                                }
                            } else {
                                lines.push(line);
                            }
                        }
                    }
                }
            }
            let rest = String::from_utf8_lossy(&pending).trim().to_owned();
            if !rest.is_empty() {
                if ok {
                    let _ = tx.send(Event::Item(rest));
                } else {
                    lines.push(rest);
                }
            }
            if ok {
                let _ = tx.send(Event::End);
            } else {
                let _ = tx.send(Event::Failed(rest_error(&lines.join("\n"))));
            }
        });
        rx
    }
}

/// The status a REST error answers: a google.rpc.Status object.
pub fn rest_error(body: &str) -> Status {
    let v: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let code = v
        .get("code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(2) as i32;
    let message = v
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(body);
    Status::new(tonic::Code::from_i32(code), message)
}

/// What following CLs delivers, for one of them.
pub enum Followed {
    /// Where it stands now (a typed status).
    Status(Box<ClStatus>),
    /// Where it stands now, as ok control's JSON, and the point it is at.
    Json(String, crate::render::Point),
    /// It could not be followed: no such CL, or ok unreachable for good.
    Failed(Box<Status>),
}

/// What following delivers: one CL's news, or the time running out.
pub enum FollowEvent {
    Cl(i64, Followed),
    TimedOut,
}

/// How long following waits before asking again after ok could not be reached.
const RETRY: std::time::Duration = std::time::Duration::from_secs(2);
/// How many times in a row ok may be unreachable before following gives up on a CL.
const RETRIES: u32 = 30;

impl Control {
    /// Follows CLs until the receiver is dropped, or `timeout` passes: each one's status as it
    /// changes — typed, or as ok control's JSON — watched again whenever its watch ends (an
    /// hour passes) or ok is briefly unreachable.
    pub fn follow(
        &self,
        cls: Vec<ClRef>,
        json: bool,
        timeout: Option<std::time::Duration>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<FollowEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if let Some(t) = timeout {
            let tx = tx.clone();
            shared::spawn(async move {
                tokio::time::sleep(t).await;
                let _ = tx.send(FollowEvent::TimedOut);
            });
        }
        for cl in cls {
            let this = self.clone();
            let tx = tx.clone();
            shared::spawn(async move { this.follow_one(cl, json, tx).await });
        }
        rx
    }

    async fn follow_one(
        self,
        cl: ClRef,
        json: bool,
        tx: tokio::sync::mpsc::UnboundedSender<FollowEvent>,
    ) {
        let n = cl.number;
        let mut failures = 0;
        loop {
            let mut finished = false;
            let mut failed: Option<Status> = None;
            if json {
                let body = serde_json::json!({ "repository": cl.repository, "number": cl.number });
                let mut rx = self.rest("Watch", body);
                while let Some(ev) = rx.recv().await {
                    match ev {
                        Event::Item(line) => {
                            let v: serde_json::Value =
                                serde_json::from_str(&line).unwrap_or_default();
                            if let Some(err) = v.get("error") {
                                failed = Some(rest_error(&err.to_string()));
                                break;
                            }
                            let result = v.get("result").cloned().unwrap_or_default();
                            let Some(point) = crate::render::Point::from_json(&result) else {
                                continue;
                            };
                            failures = 0;
                            finished =
                                matches!(point.stage, pb::Stage::Deployed | pb::Stage::Abandoned);
                            if tx
                                .send(FollowEvent::Cl(
                                    n,
                                    Followed::Json(result.to_string(), point),
                                ))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Event::End => break,
                        Event::Failed(s) => {
                            failed = Some(s);
                            break;
                        }
                    }
                }
            } else {
                let mut rx = self.watch(cl.clone());
                while let Some(ev) = rx.recv().await {
                    match ev {
                        Event::Item(st) => {
                            failures = 0;
                            finished =
                                matches!(st.stage(), pb::Stage::Deployed | pb::Stage::Abandoned);
                            if tx
                                .send(FollowEvent::Cl(n, Followed::Status(Box::new(st))))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Event::End => break,
                        Event::Failed(s) => {
                            failed = Some(s);
                            break;
                        }
                    }
                }
            }
            if finished {
                return;
            }
            if let Some(s) = failed {
                let transient = matches!(
                    s.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::Internal
                        | tonic::Code::Unknown
                        | tonic::Code::DeadlineExceeded
                );
                failures += 1;
                if !transient || failures > RETRIES {
                    let _ = tx.send(FollowEvent::Cl(n, Followed::Failed(Box::new(s))));
                    return;
                }
                tokio::time::sleep(RETRY).await;
            }
            if tx.is_closed() {
                return;
            }
        }
    }
}
