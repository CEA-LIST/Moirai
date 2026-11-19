use crate::{
    Config, TcpTransport, Transport, http_api,
    wire::{Codec, Incoming},
};
use deepsize::DeepSizeOf;
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    event::{Event, id::EventId, lamport::Lamport},
    replica::{IsReplica, Replica},
    state::log::IsLog,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_REQUESTS: usize = 256;
const REQUESTS_PER_TICK: usize = 64;

pub type CoreReplica<L> = Replica<L, Tcsb<<L as IsLog>::Update>>;

/// Application-specific behavior, independent of the transport and HTTP server.
/// The commitment predicate must use the protocol's frontier, not local delivery.
pub struct Application<L: IsLog> {
    pub query: fn(&CoreReplica<L>) -> Value,
    pub committed: fn(&CoreReplica<L>, &EventId) -> bool,
    pub no_op: fn() -> L::Command,
}

/// Runs once per loop after normal processing. Keep it short and nonblocking.
/// It may capture external protocol state, mutate the replica, and use broadcast.
pub type TickHook<'a, L, T> = dyn FnMut(&mut Node<L, T>) -> io::Result<()> + 'a;

#[derive(Debug, Serialize)]
pub struct UpdateResult {
    pub success: bool,
    /// A timeout does not roll back an update that was already applied.
    pub applied: bool,
    pub message: String,
}

impl UpdateResult {
    fn success() -> Self {
        Self {
            success: true,
            applied: true,
            message: "ok".into(),
        }
    }
    fn failure(applied: bool, message: impl Into<String>) -> Self {
        Self {
            success: false,
            applied,
            message: message.into(),
        }
    }
}

enum ClientRequest<C> {
    Update {
        command: C,
        wait: bool,
        deadline: Instant,
        reply: Sender<UpdateResult>,
    },
    Query {
        health: bool,
        reply: Sender<Value>,
    },
}

/// Thread-safe client channel. The core replica itself stays on its owner thread.
pub struct NodeHandle<C> {
    requests: SyncSender<ClientRequest<C>>,
    stopped: Arc<AtomicBool>,
    timeout: Duration,
}

impl<C> Clone for NodeHandle<C> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            stopped: self.stopped.clone(),
            timeout: self.timeout,
        }
    }
}

impl<C> NodeHandle<C> {
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
    pub fn request_timeout(&self) -> Duration {
        self.timeout
    }

    fn enqueue(&self, request: ClientRequest<C>) -> io::Result<()> {
        if self.is_stopped() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        self.requests
            .try_send(request)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => io::ErrorKind::WouldBlock.into(),
                mpsc::TrySendError::Disconnected(_) => io::ErrorKind::BrokenPipe.into(),
            })
    }

    /// Weak: acknowledge local application. Strong: wait for the application
    /// commitment predicate. These are update acknowledgments, not read barriers.
    pub fn update(&self, command: C, wait_for_commit: bool) -> io::Result<Receiver<UpdateResult>> {
        let (reply, response) = mpsc::channel();
        self.enqueue(ClientRequest::Update {
            command,
            wait: wait_for_commit,
            deadline: Instant::now() + self.timeout,
            reply,
        })?;
        Ok(response)
    }

    pub fn state(&self) -> io::Result<Receiver<Value>> {
        self.query(false)
    }
    pub fn health(&self) -> io::Result<Receiver<Value>> {
        self.query(true)
    }

    fn query(&self, health: bool) -> io::Result<Receiver<Value>> {
        let (reply, response) = mpsc::channel();
        self.enqueue(ClientRequest::Query { health, reply })?;
        Ok(response)
    }
}

struct Pending {
    event: EventId,
    deadline: Instant,
    reply: Sender<UpdateResult>,
}

pub struct Node<L: IsLog, T = TcpTransport> {
    config: Config,
    replica: CoreReplica<L>,
    transport: T,
    codec: Codec,
    application: Application<L>,
    handle: NodeHandle<L::Command>,
    requests: Receiver<ClientRequest<L::Command>>,
    pending: Vec<Pending>,
    next_pull: Instant,
    next_noop: Option<Instant>,
    next_memory: Instant,
    pull_cursor: usize,
}

impl<L> Node<L, TcpTransport>
where
    L: IsLog + DeepSizeOf,
    L::Update: Serialize + DeserializeOwned,
{
    pub fn new(config: Config, state: L, application: Application<L>) -> io::Result<Self> {
        config.validate()?;
        let transport = TcpTransport::bind(
            config.replica_id,
            ("0.0.0.0", config.listen_port),
            &config.peers,
        )?;
        Self::with_transport(config, state, application, transport)
    }
}

impl<L, T> Node<L, T>
where
    L: IsLog + DeepSizeOf,
    L::Update: Serialize + DeserializeOwned,
    T: Transport,
{
    pub fn with_transport(
        config: Config,
        state: L,
        application: Application<L>,
        transport: T,
    ) -> io::Result<Self> {
        config.validate()?;
        let members = config.members();
        let ids: Vec<_> = members.iter().map(|(_, id)| id.clone()).collect();
        let replica =
            Replica::bootstrap_with_state(config.replica_id.to_string().into(), &ids, state);
        let codec = Codec::new(members.iter().map(|(id, _)| id.0));
        let (requests, receiver) = mpsc::sync_channel(MAX_REQUESTS);
        let handle = NodeHandle {
            requests,
            stopped: Arc::new(AtomicBool::new(false)),
            timeout: Duration::from_millis(config.request_timeout_ms.get()),
        };
        let now = Instant::now();
        Ok(Self {
            next_pull: now + Duration::from_millis(config.pull_interval_ms.get()),
            next_noop: config
                .noop_interval_ms
                .map(|ms| now + Duration::from_millis(ms.get())),
            next_memory: now + Duration::from_millis(config.memory_usage_interval_ms.get()),
            config,
            replica,
            transport,
            codec,
            application,
            handle,
            requests: receiver,
            pending: Vec::new(),
            pull_cursor: 0,
        })
    }

    pub fn handle(&self) -> NodeHandle<L::Command> {
        self.handle.clone()
    }
    pub fn replica(&self) -> &CoreReplica<L> {
        &self.replica
    }
    pub fn replica_mut(&mut self) -> &mut CoreReplica<L> {
        &mut self.replica
    }
    pub fn transport(&self) -> &T {
        &self.transport
    }
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
    pub fn log_memory_usage_bytes(&self) -> usize {
        self.replica.state().deep_size_of()
    }

    /// Apply and push a local command. Use this from a hook so its events are
    /// broadcast as well as applied. It does not reset the client inactivity timer.
    pub fn broadcast(&mut self, command: L::Command) -> io::Result<EventId> {
        self.apply(command).map_err(|(_, error)| error)
    }

    fn apply(&mut self, command: L::Command) -> Result<EventId, (bool, io::Error)> {
        // Preflight the full next event before mutating state, including its clock
        // and commitment metadata. As in the core API, prepare should be pure.
        let mut version = self.replica.since().since().version().clone();
        version.increment();
        let preview = Event::new(
            EventId::from(&version),
            Lamport::from(&version),
            self.replica.prepare(command.clone()),
            version,
        );
        self.codec
            .encode_event(&preview)
            .map_err(|error| (false, error))?;
        let message = self.replica.send(command).map_err(|error| {
            (
                false,
                io::Error::new(io::ErrorKind::InvalidInput, error.to_string()),
            )
        })?;
        let bytes = self
            .codec
            .encode_event(message.event())
            .map_err(|error| (true, error))?;
        for peer in &self.config.peers {
            // Send failures leave the event in TCSB history for later pulls.
            let _ = self.transport.send(peer.replica_id, bytes.clone());
        }
        Ok(message.event().id().clone())
    }

    /// Execute one bounded iteration. Useful when embedding in another event loop.
    pub fn tick(&mut self) -> io::Result<()> {
        for packet in self.transport.poll()? {
            match self
                .codec
                .decode::<L::Update>(&packet.bytes, packet.peer, self.config.replica_id)
            {
                Ok(Incoming::Event(message)) => self.replica.receive(message),
                Ok(Incoming::Batch(message)) => self.replica.receive_batch(message),
                Ok(Incoming::Pull(since)) => {
                    let batch = self.replica.pull(since);
                    for bytes in self.codec.encode_batch(&batch)? {
                        if self.transport.send(packet.peer, bytes).is_err() {
                            break;
                        }
                    }
                }
                Err(error) => eprintln!("discarding invalid message from {}: {error}", packet.peer),
            }
        }
        for _ in 0..REQUESTS_PER_TICK {
            let Ok(request) = self.requests.try_recv() else {
                break;
            };
            match request {
                ClientRequest::Query { health, reply } => {
                    let body = if health {
                        json!({"status":"ok", "replica_id":self.config.replica_id, "replica_name":self.config.replica_name})
                    } else {
                        json!({"replica_id":self.config.replica_id, "replica_name":self.config.replica_name, "state":(self.application.query)(&self.replica)})
                    };
                    let _ = reply.send(body);
                }
                ClientRequest::Update {
                    command,
                    wait,
                    deadline,
                    reply,
                } => {
                    let now = Instant::now();
                    self.next_noop = self
                        .config
                        .noop_interval_ms
                        .map(|ms| now + Duration::from_millis(ms.get()));
                    if now >= deadline {
                        let _ = reply.send(UpdateResult::failure(
                            false,
                            "request expired before application",
                        ));
                    } else if wait && self.pending.len() >= self.config.http_consistent_limit.get()
                    {
                        let _ = reply.send(UpdateResult::failure(
                            false,
                            "too many pending commitment requests",
                        ));
                    } else {
                        match self.apply(command) {
                            Ok(event) if wait => self.pending.push(Pending {
                                event,
                                deadline,
                                reply,
                            }),
                            Ok(_) => {
                                let _ = reply.send(UpdateResult::success());
                            }
                            Err((applied, error)) => {
                                let _ =
                                    reply.send(UpdateResult::failure(applied, error.to_string()));
                            }
                        }
                    }
                }
            }
        }
        let now = Instant::now();
        if now >= self.next_pull {
            self.next_pull = now + Duration::from_millis(self.config.pull_interval_ms.get());
            let count = self.config.pull_peer_count.min(self.config.peers.len());
            if count > 0 {
                let bytes = self.codec.encode_pull(&self.replica.since())?;
                for _ in 0..count {
                    let peer = &self.config.peers[self.pull_cursor];
                    self.pull_cursor = (self.pull_cursor + 1) % self.config.peers.len();
                    let _ = self.transport.send(peer.replica_id, bytes.clone());
                }
            }
        }
        if self.next_noop.is_some_and(|deadline| now >= deadline) {
            self.broadcast((self.application.no_op)())?;
            self.next_noop = self
                .config
                .noop_interval_ms
                .map(|ms| Instant::now() + Duration::from_millis(ms.get()));
        }
        let committed = self.application.committed;
        self.pending.retain(|pending| {
            let result = if committed(&self.replica, &pending.event) {
                UpdateResult::success()
            } else if now >= pending.deadline {
                UpdateResult::failure(true, "commitment timeout; update remains applied")
            } else {
                return true;
            };
            let _ = pending.reply.send(result);
            false
        });
        if self.config.memory_usage_enabled && now >= self.next_memory {
            self.next_memory =
                now + Duration::from_millis(self.config.memory_usage_interval_ms.get());
            println!(
                "{}",
                json!({"type":"log_memory_usage", "replica_id":self.config.replica_id, "replica_name":self.config.replica_name,
                "timestamp_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
                "log_memory_bytes":self.log_memory_usage_bytes()})
            );
        }
        Ok(())
    }

    /// Run until the handle is stopped or an error occurs. Pass `None` for the
    /// built-in protocols, or a callback for external commitment/protocol work.
    pub fn run(&mut self, mut hook: Option<&mut TickHook<'_, L, T>>) -> io::Result<()>
    where
        L::Command: DeserializeOwned + Send + 'static,
    {
        let api = self
            .config
            .http_port
            .map(|port| {
                http_api::start(
                    port,
                    self.handle(),
                    self.config.http_optimistic_limit.get(),
                    self.config.http_consistent_limit.get(),
                )
            })
            .transpose()?;
        let result = (|| {
            while !self.handle.is_stopped() {
                self.tick()?;
                if let Some(hook) = hook.as_mut() {
                    hook(self)?;
                }
                thread::sleep(Duration::from_millis(2));
            }
            Ok(())
        })();
        self.handle.stop();
        self.finish_pending();
        drop(api);
        result
    }
}

impl<L: IsLog, T> Node<L, T> {
    fn finish_pending(&mut self) {
        for pending in self.pending.drain(..) {
            let _ = pending.reply.send(UpdateResult::failure(
                true,
                "node stopped before commitment",
            ));
        }
        while let Ok(request) = self.requests.try_recv() {
            if let ClientRequest::Update { reply, .. } = request {
                let _ = reply.send(UpdateResult::failure(
                    false,
                    "node stopped before application",
                ));
            }
        }
    }
}

impl<L: IsLog, T> Drop for Node<L, T> {
    fn drop(&mut self) {
        self.handle.stop();
        self.finish_pending();
    }
}
