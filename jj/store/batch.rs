//! Coalescing of concurrent reads. jj reads many objects at once (a tree diff, an index
//! build, a checkout); each read joins the pending batch, and a flush on the store runtime
//! — a moment later, so concurrent reads can join — fetches the whole batch in one RPC. Up to
//! `MAX_IN_FLIGHT` fetches of a kind run at once: a read made while one is in flight goes out at
//! the next flush rather than waiting for it to return. A read of a key a fetch is already reading
//! joins that fetch: nothing is fetched twice at once.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tonic::Status;

use crate::shared;

/// How long a flush waits for concurrent reads to join its batch.
const GATHER: Duration = Duration::from_millis(1);

/// How many fetches of one kind run at once. A request's round trip is most of a small read, so
/// overlapping them is what lets concurrent readers go at the network's pace rather than one
/// request's; the bound keeps one client from flooding the SCM.
pub(crate) const MAX_IN_FLIGHT: usize = 8;

type Fetch<K, V> =
    dyn Fn(Vec<K>) -> BoxFuture<'static, Result<HashMap<K, V>, Status>> + Send + Sync;

pub struct Batcher<K, V> {
    state: Mutex<State<K, V>>,
    fetch: Box<Fetch<K, V>>,
    max: usize,
    /// What it fetches, for its statistics (`crate::calls::fetches`).
    kind: &'static str,
}

/// A reader waiting for a key's value.
type Waiter<V> = oneshot::Sender<Result<Option<V>, Status>>;

/// A batch's keys, each with the readers waiting for it.
type Readers<K, V> = Vec<(K, Vec<Waiter<V>>)>;

struct State<K, V> {
    waiting: HashMap<K, Vec<Waiter<V>>>,
    /// The keys the fetches in flight are reading, with every reader waiting for each.
    inflight: HashMap<K, Vec<Waiter<V>>>,
    /// A flush is gathering: it will send what is waiting.
    gathering: bool,
    /// Fetches in flight.
    active: usize,
}

impl<K, V> Batcher<K, V>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// A batcher fetching at most `max` keys per call with `fetch`, whose result omits keys
    /// that do not exist.
    pub fn new(kind: &'static str, max: usize, fetch: Box<Fetch<K, V>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                waiting: HashMap::new(),
                inflight: HashMap::new(),
                gathering: false,
                active: 0,
            }),
            fetch,
            max,
            kind,
        })
    }

    /// The value of `key` (None if it does not exist), fetched with whatever else is being
    /// read concurrently.
    pub async fn get(self: &Arc<Self>, key: K) -> Result<Option<V>, Status> {
        let (tx, rx) = oneshot::channel();
        let gather = {
            let mut state = self.state.lock().unwrap();
            if let Some(readers) = state.inflight.get_mut(&key) {
                readers.push(tx);
                false
            } else {
                state.waiting.entry(key).or_default().push(tx);
                // At the limit, the next fetch to return sends what waits.
                let start = !state.gathering && state.active < MAX_IN_FLIGHT;
                if start {
                    state.gathering = true;
                }
                start
            }
        };
        if gather {
            let this = self.clone();
            shared::spawn(async move {
                tokio::time::sleep(GATHER).await;
                let batches = {
                    let mut state = this.state.lock().unwrap();
                    state.gathering = false;
                    this.take(&mut state)
                };
                this.send(batches);
            });
        }
        rx.await
            .map_err(|_| Status::internal("a batched read was dropped"))?
    }

    /// Moves what waits into batches of at most `max` keys, as many as may be in flight.
    fn take(&self, state: &mut State<K, V>) -> Vec<Vec<K>> {
        let mut batches = Vec::new();
        while !state.waiting.is_empty() && state.active < MAX_IN_FLIGHT {
            let keys: Vec<K> = state.waiting.keys().take(self.max).cloned().collect();
            for k in &keys {
                let waiters = state.waiting.remove(k).unwrap();
                state.inflight.insert(k.clone(), waiters);
            }
            state.active += 1;
            batches.push(keys);
        }
        batches
    }

    /// Starts a fetch of each batch.
    fn send(self: &Arc<Self>, batches: Vec<Vec<K>>) {
        for keys in batches {
            let this = self.clone();
            shared::spawn(async move { this.fetch_batch(keys).await });
        }
    }

    /// Fetches one batch and answers its readers; then sends what waited for a fetch to return.
    async fn fetch_batch(self: Arc<Self>, keys: Vec<K>) {
        let started = std::time::Instant::now();
        let result = (self.fetch)(keys.clone()).await;
        crate::calls::fetched(self.kind, keys.len(), started.elapsed());
        let (batch, next): (Readers<K, V>, Vec<Vec<K>>) = {
            let mut state = self.state.lock().unwrap();
            let batch = keys
                .into_iter()
                .map(|k| {
                    let waiters = state.inflight.remove(&k).unwrap();
                    (k, waiters)
                })
                .collect();
            state.active -= 1;
            let next = if state.gathering {
                Vec::new()
            } else {
                self.take(&mut state)
            };
            (batch, next)
        };
        self.send(next);
        match result {
            Ok(mut found) => {
                for (key, waiters) in batch {
                    let value = found.remove(&key);
                    for w in waiters {
                        let _ = w.send(Ok(value.clone()));
                    }
                }
            }
            Err(status) => {
                for (_, waiters) in batch {
                    for w in waiters {
                        let _ = w.send(Err(status.clone()));
                    }
                }
            }
        }
    }
}
