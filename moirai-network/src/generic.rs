//! Generic network node for any Arachne-generated (or manually-written) CRDT.
//!
//! This module provides [`GenericNode`], which is parameterised with a single
//! log type `L` that implements [`IsLog`].
//!
//! The operation type `L::Op` must satisfy the transport bounds:
//! `Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static`
//!

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::sync::mpsc::{self, Receiver, Sender};
#[cfg(feature = "test_utils")]
use std::sync::Mutex;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard};
use std::thread;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::log_id::LogId;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;
use moirai_protocol::utils::intern_str::{lock_interner, InternalizeOp, Interner, SharedInterner};

use crate::composite::CompositeTransport;
use crate::dashboard::{now_ms, DashboardConfig, DashboardSink, EventRecord, SnapshotRecord};
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::query::QueryableLog;
use crate::state_transfer::{LogPayload, TransferableLog};
use crate::transport::{CrdtTransport, PeerId, TransportMessage};
use crate::HashMap;

/// A replica over the transport that routes per peer. The alias to build a node
/// with unless there is a reason not to.
pub type Node<L> = GenericNode<L, CompositeTransport<<L as IsLog>::Op>>;

/// Former name of [`Node`], from when TCP was the only way to reach a peer.
#[deprecated(
    since = "0.1.0",
    note = "renamed to `Node`; the transport underneath is now a composite that routes each peer direct or relayed"
)]
pub type TcpNode<L> = Node<L>;

/// One hosted log together with its causal bookkeeping: the per-model bundle a
/// node keeps, which is not a peer, a process or a connection. See
/// [`GenericNode`].
pub type LogReplica<L> = Replica<L, Tcsb<<L as IsLog>::Op>>;

/// The application's registration hook: the operations that open a newly
/// created model's log, given its id and the descriptor it was registered
/// under. See [`GenericNode::enable_registration`].
pub type RegisterFn<O> = fn(&LogId, &ServedDescriptor) -> Vec<O>;

/// The application's intake guard: `Err(reason)` refuses an operation before a
/// registered log applies it. Handed the log's id and the descriptor the log
/// was registered under — the one `register` matched, never the caller's
/// claim — so a guard that checks the operation against that descriptor can
/// be a plain function pointer with no state of its own. See
/// [`GenericNode::enable_op_guard`].
pub type OpGuardFn<O> = fn(&LogId, &ServedDescriptor, &O) -> Result<(), String>;

/// The application's description of one descriptor's text: the key it is
/// served under, the entry `GET /api/metamodels` lists for it, or the reason
/// the text is not a descriptor at all. See
/// [`GenericNode::enable_metamodel_upload`].
///
/// This is the whole of what `POST /api/metamodels` knows how to do with a
/// body. The node reads nothing inside the text — it cannot, and it must not:
/// what a descriptor means is the application's, and a key claimed by the
/// caller instead of computed from the bytes would be a caller naming its own
/// identity.
pub type DescribeFn = fn(&str) -> Result<ServedDescriptor, String>;

/// The application's look inside a bound log: the descriptor's text, once
/// the log carries one, or `None` while it does not.
///
/// The node calls this on every pass for each bound log it has not yet
/// compared with the model's own history — every join under a metamodel it
/// holds no descriptor for, and every other binding exactly once — and hands
/// whatever comes back to [`GenericNode::add_metamodel`],
/// which describes it through the application's own [`DescribeFn`] and so
/// re-derives the key from the bytes. As with [`DescribeFn`], the node reads
/// nothing inside the text: where a log keeps a descriptor, and whether it
/// keeps one at all, is the application's business.
///
/// See [`GenericNode::enable_descriptor_adoption`].
pub type AdoptFn<L> = fn(&LogReplica<L>) -> Option<String>;

/// How long a replica with no history waits on one donor before asking another,
/// and how long it waits before starting a fresh round once every peer has been
/// asked.
///
/// Asking once, on connect, is not enough, and not because the network is
/// unreliable. A peer that has written nothing yet has nothing to transfer and
/// says so; the replicas of a session that starts together are all in that
/// state until the first operation is applied, and the one that arrives second
/// must be able to ask again afterwards.
///
/// It is also what covers T5 — a donor that dies mid-transfer must not strand
/// the joiner. A donor that has said nothing by the time this elapses is passed
/// over for the next one, so silence costs one interval rather than the
/// session.
///
/// Same reasoning as P1-D9, where the node re-dials on every roster rather than
/// only on a changed one: a first attempt can legitimately fail, and gating the
/// retry on an event that will not recur reintroduces ask-once.
const STATE_TRANSFER_RETRY: Duration = Duration::from_secs(2);

/// Largest serialised log this replica will put in a `StateResponse`.
///
/// # Why there has to be one
///
/// A `StateResponse` is a single newline-delimited frame, and over a relay the
/// relay reads it with a cap: `MAX_FRAME_BYTES = 1 MiB` in
/// `moirai-relay/src/main.rs`, where an over-long frame is an *error* rather
/// than a truncation, because truncating would feed the remainder back as the
/// next frame. So the donor's own relay session is what dies, and the joiner —
/// still with no history — re-asks every [`STATE_TRANSFER_RETRY`] and kills it
/// again. Without a ceiling here the failure mode of a large model is a loop
/// that takes the donor off the relay rather than a refusal anybody can act on.
///
/// # Why this number
///
/// Below the relay's 1 MiB, with the rest left for the `snapshot` beside the
/// log and the relay envelope around both. It is checked against the log
/// *before* compression, which is the conservative direction twice over:
/// [`LogPayload::encode`] keeps whichever of the compressed and plain forms is
/// smaller, so what goes on the wire is never larger than what is measured
/// here, and the number is simultaneously a bound on the memory a joiner needs
/// to hold the log — which compression does not reduce.
///
/// Uniform across transports rather than per route: the donor answers a
/// `StateRequest` without knowing whether the reply will go direct or be
/// relayed, and a direct connection reads the whole frame into memory with no
/// cap of its own (`direct_transport.rs`), so a ceiling is wanted there too.
///
/// # What it does not fix
///
/// A session whose model genuinely exceeds this cannot transfer state at all,
/// and no ordered feature compacts — see the eg-walker finding — so that is
/// reachable by session age alone rather than only by model size. Chunking is
/// the answer and it is phase 3; this makes the failure clean and diagnosable
/// in the meantime, which is what the joiner's existing `StateUnavailable`
/// handling was already written for.
const MAX_STATE_TRANSFER_BYTES: usize = 768 * 1024;

// Alias for these bounds, needed to transport operations over the network (e.g. via HTTP API).
pub trait NetworkOp:
    Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static
{
}

impl<T> NetworkOp for T where
    T: Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static
{
}

/// How many ids of not-hosted logs one node remembers at once.
///
/// A session holds a handful of models, so sixty-four is far past what any
/// real session reaches; it is a ceiling on a long-lived process, not a
/// working limit. When the set is full a further new id is simply not
/// recorded — the ids already there keep being offered, and the frame is
/// counted in `frames_not_hosted` as it always was.
const SEEN_NOT_HOSTED_CAP: usize = 64;

// =============================================================================
// GenericNode — one peer, its hosted logs, one transport, external adapters
// =============================================================================

/// A generic network node.
///
/// `L` is the CRDT **log** type (e.g. `JsonLog`).
/// `T` is the transport backend (e.g. [`CompositeTransport`]).
///
/// The [`Node`] type alias is the usual choice.
///
/// One node is one peer: one process, one transport, one HTTP server — and
/// one log per model it hosts, keyed by [`LogId`]. Each hosted log carries its
/// own causal bookkeeping ([`LogReplica`]: inbox, outbox, matrix clock),
/// because causal stability is per log. What the logs share is the member
/// table ([`SharedInterner`]), because that describes the session and not any
/// one model.
pub struct GenericNode<L: IsLog, T: CrdtTransport<Op = L::Op>>
where
    L::Op: NetworkOp,
{
    replica_id: String,
    /// The logs this node hosts, one per model.
    ///
    /// A node hosts a log only after [`host_log`] registered it (the
    /// constructor registers the default log); a frame for any other id is
    /// filtered and counted in `frames_not_hosted`, never adopted. Hosting on
    /// sight would let any peer spawn unbounded logs on every node.
    ///
    /// [`host_log`]: GenericNode::host_log
    logs: BTreeMap<LogId, HostedLog<L>>,
    /// The log named at start. The unscoped routes, [`apply_op`] and the
    /// dashboard read it.
    ///
    /// [`apply_op`]: GenericNode::apply_op
    default_log: LogId,
    /// The member table every hosted log indexes by. See [`SharedInterner`].
    interner: SharedInterner,
    /// Frames dropped because no hosted log carries their id.
    ///
    /// A filter, not a refusal. One session carries every model's traffic, so
    /// a node hosting one model of sixteen drops fifteen frames in sixteen,
    /// and on a multi-model session a flat zero is the suspicious reading. The
    /// counter is the whole observable: nothing is printed per frame, which
    /// would flood any such session. The refusal is the other counter,
    /// `foreign_log_refusals` inside each log — a frame handed to a log that
    /// does not own it, which is a dispatch defect and stays at zero once
    /// frames route by id.
    frames_not_hosted: u64,
    /// The ids of logs this node does not host, seen on a frame that was
    /// dropped. The counter above says how many frames went by; this says
    /// which models they belonged to, so a second editor can be offered a
    /// list to click rather than an id to retype.
    ///
    /// Ids only, and nothing more is knowable here: which metamodel a log is
    /// bound to lives in that log's first operation, which this node does not
    /// hold, and `L::Op` is opaque to this type anyway.
    ///
    /// An id leaves the set when [`host_log`] starts hosting it, so a model
    /// this node has joined stops being offered as one it could join.
    ///
    /// Bounded at [`SEEN_NOT_HOSTED_CAP`]: once full, a further *new* id is
    /// dropped and only the ids already in the set are ever reported. A
    /// session carries a handful of models, so the cap is never reached in
    /// practice; what it rules out is a long-lived replica in a churning
    /// session growing this set for the life of the process.
    ///
    /// [`host_log`]: GenericNode::host_log
    seen_not_hosted: BTreeSet<LogId>,
    transport: T,
    adapter_op_rx: Receiver<OpEnvelope<L::Op>>,
    adapter_op_tx: Sender<OpEnvelope<L::Op>>,
    ctrl_rx: Receiver<ControlCmd>,
    ctrl_tx: Sender<ControlCmd>,

    /// Optional callback to query a log's state as JSON.
    /// Set by `enable_state_query()` when `L: QueryableLog`.
    query_fn: Option<fn(&LogReplica<L>) -> serde_json::Value>,
    /// The application's hook for lifting a descriptor out of a log that
    /// arrived with one, installed by [`Self::enable_descriptor_adoption`].
    ///
    /// `None` is the behaviour a node had before in full: a log joined under
    /// an unheld metamodel merges and reads out, and the descriptor never
    /// joins the served set, so `GET /api/model/{id}/metamodel` stays 404 for
    /// it for the node's whole life.
    adopt_fn: Option<AdoptFn<L>>,
    /// Bootnode poller, when `BOOTNODE_URL` was configured.
    ///
    /// `None` is the pre-phase-1 behaviour in full: peers come from `PEERS`,
    /// are dialled once, and nothing is ever discovered.
    discovery: Option<Discovery>,
    /// Serialise / rebuild the CRDT log, set by `enable_state_transfer()` when
    /// `L: TransferableLog`.
    ///
    /// `None` means this replica can neither serve nor accept a state
    /// transfer, and both paths degrade to `SyncRequest`. Same shape, and same
    /// reason, as `query_fn`: a hand-written log that is not serialisable must
    /// keep working.
    export_log: Option<fn(&L) -> serde_json::Value>,
    import_log: Option<fn(serde_json::Value) -> Option<L>>,
    /// The metamodel descriptors this node serves, in the order the
    /// application gave them; see [`Self::serve_metamodels`]. Empty is the
    /// pre-existing behaviour in full: `GET /api/metamodel` answers 404
    /// exactly like any other unknown path.
    ///
    /// Shared with the HTTP thread, which reads it — a listing, a
    /// descriptor's text — on every request rather than from a snapshot taken
    /// when it spawned, because [`Self::add_metamodel`] can grow it while the
    /// node runs. Written from the event loop and from nowhere else.
    descriptors: Arc<RwLock<Vec<ServedDescriptor>>>,
    /// The application's description hook, installed by
    /// [`Self::enable_metamodel_upload`]; `None` until then, and
    /// `POST /api/metamodels` answers [`ServeRefused::NotEnabled`].
    describe_fn: Option<DescribeFn>,
    /// The application's two registration hooks, installed together by
    /// [`Self::enable_registration`]; `None` until then, and registration
    /// answers [`RegisterRefused::NotEnabled`].
    ///
    /// The first names the key a `metamodel_id` resolves to, which the node
    /// checks against the descriptors it holds. The second returns the
    /// operations that open a newly created model — its header — and is
    /// called on create only, never on join: the header travels with the
    /// log, and the creator writes it exactly once.
    descriptor_key_fn: Option<fn(&serde_json::Value) -> Option<String>>,
    register_fn: Option<RegisterFn<L::Op>>,
    /// The application's intake guard, installed by [`Self::enable_op_guard`]:
    /// consulted for every operation an adapter submits to a *registered*
    /// log, never for the default log and never for the opening operations
    /// `register_fn` writes. `None` guards nothing. What it protects is the
    /// application's business; the node knows nothing about the shape of an
    /// operation.
    op_guard_fn: Option<OpGuardFn<L::Op>>,
    /// Outbound reporting, when `DASHBOARD_URL` was configured. `None` is the
    /// pre-existing behaviour in full: no thread, no request, and the delivery
    /// trace left switched off.
    dashboard: Option<DashboardSink<L::Op>>,
    /// The operations behind events not yet reported. Empty and untouched while
    /// `dashboard` is `None`.
    ops_by_event: OpsByEvent<L::Op>,
    last_report: Option<Instant>,
    last_state_render: Option<Instant>,
    started_at: Instant,
}

/// One hosted log and the bookkeeping that belongs to it rather than to the
/// node.
struct HostedLog<L: IsLog> {
    /// The log with its causal bookkeeping.
    replica: LogReplica<L>,
    /// Log of all operations delivered to this log (for the operations
    /// endpoint). Display only: it over-counts remote operations while both
    /// peers dial each other.
    operation_log: Vec<L::Op>,
    /// Operations *originated* here and accepted by the CRDT.
    ///
    /// Separate from `operation_log`, which counts deliveries. This one is
    /// exact, so `/api/metrics` can be trusted as a test oracle.
    local_ops: usize,
    /// Where this log stands with state transfer. Per log rather than per
    /// node, or a joiner of four models would ask one donor for one of them.
    transfer: TransferState,
    /// What the log was registered under, when it was registered at all: the
    /// default log has none.
    binding: Option<Binding>,
}

/// The metamodel a hosted log was registered under, as the application named
/// it. Opaque here: the key is whatever `descriptor_key_fn` answered and the
/// id is echoed back on `GET /api/models` verbatim.
#[derive(Debug, Clone)]
struct Binding {
    key: String,
    metamodel_id: serde_json::Value,
    /// Whether this node holds the descriptor `key` names.
    state: BindingState,
    /// Whether the model's own history has been read against `key` yet.
    ///
    /// The key is the model's identity, and a binding is only a *claim* about
    /// it until the model itself is heard from: the log carries the
    /// descriptor it was opened with, and until that descriptor has been
    /// described and its key compared with this one, nothing here knows
    /// whether the claim is true. `false` at registration for a create and a
    /// join alike, and set once the comparison has been made — after which
    /// there is nothing left to compare, because one log holds one metamodel
    /// for its whole life.
    ///
    /// It is what keeps [`GenericNode::resolve_bindings`] from describing
    /// every hosted model's descriptor on every pass of the event loop.
    confirmed: bool,
}

/// Whether a bound log's metamodel is one this node can serve.
///
/// The three states are one distinction made twice: a log always has a
/// metamodel *key*, and this says whether the bytes behind it are here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindingState {
    /// The descriptor was in the served set when the log was bound, or has
    /// been adopted into it since. Everything answers as it always has.
    ///
    /// Held is not the same as *checked*: the log was bound to a key this
    /// node happens to hold, which says nothing about the language the model
    /// is actually written in. `Binding::confirmed` is what carries that,
    /// and a held binding the model's own history contradicts becomes
    /// `Foreign` rather than staying here.
    Held,
    /// It is not, and the model's own history has not brought it yet.
    ///
    /// The log was joined by id under a metamodel this node holds no
    /// descriptor for, and it is hosted anyway, because a joined log carries
    /// its own metamodel in the operations it receives. Until one arrives the
    /// log has no semantics here, which every route it answers is written
    /// against: `GET /api/model/{id}/state` reads out an empty log,
    /// `POST /api/model/{id}/op` is refused by the log itself, and
    /// `GET /api/model/{id}/metamodel` looks `key` up among the served
    /// descriptors, finds none, and answers 404.
    ///
    /// A *create* is never pending. A new model's opening operations are
    /// written from the descriptor, so there is nothing to write them from,
    /// and `register` refuses it as it always has.
    Pending,
    /// The model brought a descriptor and it is not the one the log was
    /// joined under, or it is one the application will not serve.
    ///
    /// Reached from `Pending` — the log was joined under a metamodel this
    /// node holds no descriptor for and the model carries a different one —
    /// and from `Held` alike: a join names a digest this node happens to
    /// hold, which the node has no way to check at join time, and the
    /// model's own history is what checks it. Both are the same
    /// disagreement, and the second is the dangerous one, because everything
    /// answers normally until the history arrives.
    ///
    /// `GET /api/model/{id}/metamodel` answers 404 for a foreign binding,
    /// and does so for the reason rather than by accident: the key the log
    /// was bound under may well name a descriptor this node serves, and
    /// serving it would tell an editor the model is written in a language it
    /// is not. See [`BoundDescriptor::Disputed`].
    ///
    /// Terminal, and it exists so that a disagreement is reported once
    /// instead of on every pass of the event loop: one log holds one
    /// metamodel for its whole life, so a descriptor that did not match will
    /// not be replaced by one that does.
    Foreign,
}

impl<L: IsLog> HostedLog<L>
where
    L::Op: NetworkOp,
{
    fn new(replica: LogReplica<L>) -> Self {
        Self {
            replica,
            operation_log: Vec::new(),
            local_ops: 0,
            transfer: TransferState::default(),
            binding: None,
        }
    }

    /// `true` while this log has delivered nothing at all.
    ///
    /// The whole precondition for adopting a donor's state wholesale, in one
    /// place, because it is checked twice: once when deciding what to ask a peer
    /// for, and again when a response arrives — several donors can answer the
    /// same request, and the second answer must not undo the first.
    ///
    /// Deliberately *not* "and knows no other replica", which the plan proposed
    /// and which does not work: a peer's `SyncRequest` is internalised, so being
    /// asked for a delta adds the asker to the member set. A replica that has
    /// merely been spoken to would then look like one with history, and — as
    /// measured — a joiner would receive its donors' state transfers and
    /// silently discard every one of them. Knowing who the members are is not
    /// history. Having delivered something is.
    fn has_no_history(&self) -> bool {
        self.replica.stability().delivered == 0
    }

    /// `true` while this log holds nothing at all: nothing delivered, nothing
    /// waiting in its inbox, nothing retained in its outbox.
    ///
    /// Stricter than [`has_no_history`], and the condition a *sibling* must
    /// meet before another log may adopt a donor's ordering: `Tcsb` re-seats
    /// a clock on a rebuilt member table only while its inbox and outbox are
    /// empty (`follow_shared_interner`), and a frame received but not yet
    /// deliverable sits in both with `delivered` still at zero. M-E3's first
    /// run found the gap: two of five replicas joining sixteen models died at
    /// that assertion, each holding one such frame in a sibling log while a
    /// donor's snapshot for another log arrived.
    ///
    /// [`has_no_history`]: HostedLog::has_no_history
    fn holds_nothing(&self) -> bool {
        let stability = self.replica.stability();
        stability.delivered == 0 && stability.pending == 0 && stability.retained == 0
    }
}

/// What the hosted-log map costs in memory, for M-E2 of the model plane's
/// validation plan: the deep size of every hosted log with the member table
/// counted once however many logs hold its handle, and the check that they
/// do hold one handle rather than one table each.
///
/// Behind `test_utils` because it is a measurement surface and not a node
/// capability; `experiments/mp2-dispatch-cost/` in the vault holds its one caller.
#[cfg(feature = "test_utils")]
mod cost {
    use std::sync::Arc;

    use deepsize::{Context, DeepSizeOf};
    use moirai_protocol::broadcast::tcsb::IsTcsbTest;

    use super::*;

    impl<L: IsLog> DeepSizeOf for HostedLog<L>
    where
        L::Op: NetworkOp + DeepSizeOf,
    {
        /// The log's causal bookkeeping (inbox, outbox, matrix clock, the
        /// stable version, the log id, and the shared member table once per
        /// [`Context`]) beside the node-side records around it. The CRDT
        /// log's own heap is not walked: `POLog` carries no `DeepSizeOf`
        /// (`moirai-protocol/src/state/po_log.rs`, the derive is commented
        /// out), and M-E2 reads empty logs, whose stable and unstable states
        /// have allocated nothing.
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.replica.id().len()
                + self.replica.tcsb().deep_size_of_children(context)
                + self.operation_log.deep_size_of_children(context)
                + self.transfer.deep_size_of_children(context)
                + self
                    .binding
                    .as_ref()
                    .map_or(0, |binding| binding.deep_size_of_children(context))
        }
    }

    impl DeepSizeOf for TransferState {
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.donor.deep_size_of_children(context)
                + self.donors_tried.deep_size_of_children(context)
        }
    }

    impl DeepSizeOf for Binding {
        /// `serde_json::Value` has no `DeepSizeOf`; its compact text is the
        /// floor of what it holds, and a binding is a few dozen bytes either
        /// way.
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.key.deep_size_of_children(context) + self.metamodel_id.to_string().len()
        }
    }

    /// What one entry of the hosted map holds: the key and the log, inline
    /// and on the heap, walked with the context the caller is charging.
    fn entry_size<L: IsLog>(log_id: &LogId, log: &HostedLog<L>, context: &mut Context) -> usize
    where
        L::Op: NetworkOp + DeepSizeOf,
    {
        std::mem::size_of::<LogId>()
            + log_id.deep_size_of_children(context)
            + std::mem::size_of::<HostedLog<L>>()
            + log.deep_size_of_children(context)
    }

    /// The entries of a hosted map as one walk, so an allocation two logs
    /// share — the member table — is charged once and not once per log.
    ///
    /// Charged entry by entry rather than through `BTreeMap`'s own
    /// `DeepSizeOf`, which amortises node storage at an assumed occupancy
    /// and so reports a one-entry map as smaller than its entry; here `m(1)`
    /// equals `s` by construction and the B-tree's node slack is the one
    /// thing left out, which is stated rather than estimated.
    struct Entries<'a, L: IsLog>(&'a BTreeMap<LogId, HostedLog<L>>);

    impl<L: IsLog> DeepSizeOf for Entries<'_, L>
    where
        L::Op: NetworkOp + DeepSizeOf,
    {
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.0
                .iter()
                .map(|(log_id, log)| entry_size(log_id, log, context))
                .sum()
        }
    }

    /// One entry on its own, walked with a fresh context.
    struct Entry<'a, L: IsLog>(&'a LogId, &'a HostedLog<L>);

    impl<L: IsLog> DeepSizeOf for Entry<'_, L>
    where
        L::Op: NetworkOp + DeepSizeOf,
    {
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            entry_size(self.0, self.1, context)
        }
    }

    impl<L: IsLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
    where
        L::Op: NetworkOp + DeepSizeOf,
    {
        /// The deep size of the hosted-log map, `m(N)` in the plan: every
        /// entry's key and log, inline and on the heap, the member table
        /// counted once.
        pub fn hosted_logs_deep_size(&self) -> usize {
            let entries = Entries(&self.logs);
            entries.deep_size_of() - std::mem::size_of_val(&entries)
        }

        /// The deep size of one hosted log's entry on its own, `s` in the
        /// plan when read at one hosted log: the member table is counted in
        /// it, once; `None` for an id this node does not host.
        pub fn hosted_log_deep_size(&self, log_id: &LogId) -> Option<usize> {
            self.logs.get_key_value(log_id).map(|(log_id, log)| {
                let entry = Entry(log_id, log);
                entry.deep_size_of() - std::mem::size_of_val(&entry)
            })
        }

        /// How many distinct member tables the hosted logs hold between
        /// them, by pointer identity of the handle: one, or the sharing the
        /// design promises has been lost.
        pub fn member_tables(&self) -> usize {
            let mut tables: Vec<*const Mutex<Interner>> = self
                .logs
                .values()
                .map(|log| Arc::as_ptr(log.replica.tcsb().interner()))
                .chain(std::iter::once(Arc::as_ptr(&self.interner)))
                .collect();
            tables.sort_unstable();
            tables.dedup();
            tables.len()
        }
    }
}

/// One log's side of the state-transfer protocol. See [`STATE_TRANSFER_RETRY`].
#[derive(Debug, Default)]
struct TransferState {
    /// When the last `StateRequest` went out.
    last_request: Option<Instant>,
    /// The peer currently being asked for a state transfer, if any.
    ///
    /// `Some` means a request is outstanding and the deadline is running;
    /// `None` means the next pass may choose somebody. Cleared the moment a
    /// donor refuses, so a refusal costs a loop iteration rather than an
    /// interval.
    donor: Option<PeerId>,
    /// Peers already asked in the current round. A `Vec` because it is bounded
    /// by the member count and only ever scanned linearly.
    donors_tried: Vec<PeerId>,
}

/// Why [`GenericNode::host_log`] declined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    /// The node already hosts a log with this id.
    AlreadyHosted(LogId),
}

impl Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyHosted(log_id) => write!(f, "log {log_id} is already hosted here"),
        }
    }
}

impl std::error::Error for HostError {}

/// A metamodel descriptor the node serves, as the application handed it over.
///
/// Opaque on purpose: the key and the listing entry are whatever the
/// application chose (a namespace URI, a digest), and the text is served
/// verbatim. `moirai-network` never reads inside any of them.
#[derive(Debug, Clone)]
pub struct ServedDescriptor {
    /// What a registration's `metamodel_id` resolves to through the
    /// application's `descriptor_key_fn`; a model is bound to this key.
    pub key: String,
    /// What `GET /api/metamodels` lists for it, verbatim. `Null` keeps the
    /// descriptor off the list.
    pub listing: serde_json::Value,
    /// The descriptor itself, served verbatim.
    pub text: String,
}

impl ServedDescriptor {
    /// A descriptor with no key and no listing: served on `GET /api/metamodel`
    /// and nowhere else, which is all a node before registration existed did.
    pub fn unlisted(text: String) -> Self {
        Self {
            key: String::new(),
            listing: serde_json::Value::Null,
            text,
        }
    }
}

/// What [`GenericNode::register`] answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// The log the model lives in: minted here on a create, given on a join.
    pub model_id: LogId,
    /// `true` when this node created the model and wrote its opening
    /// operations; `false` when it joined one by id and wrote nothing.
    pub created: bool,
}

/// Why [`GenericNode::register`] declined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterRefused {
    /// No registration hooks were installed; see
    /// [`GenericNode::enable_registration`].
    NotEnabled,
    /// The `metamodel_id` names no descriptor this node holds, on a *create*
    /// or in a form `descriptor_key_fn` cannot read at all. Refused before
    /// anything is hosted: a node cannot mint a model under a metamodel it
    /// does not hold, because the opening operations are written from the
    /// descriptor.
    ///
    /// A *join* under an unheld metamodel is not this: it is hosted with a
    /// pending binding, and the metamodel reaches the log with the model. See
    /// [`GenericNode::register`].
    UnknownMetamodel(serde_json::Value),
    /// The node already hosts a log with this id.
    AlreadyHosted(LogId),
}

impl Display for RegisterRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotEnabled => write!(f, "model registration is not enabled on this node"),
            Self::UnknownMetamodel(id) => {
                write!(f, "this node holds no descriptor for metamodel {id}")
            }
            Self::AlreadyHosted(log_id) => write!(f, "model {log_id} is already hosted here"),
        }
    }
}

impl std::error::Error for RegisterRefused {}

/// What [`GenericNode::add_metamodel`] answers: the descriptor as the
/// application described it, and whether this node had it already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Served {
    /// The key the descriptor is served under, as the application's
    /// [`DescribeFn`] computed it.
    pub key: String,
    /// The entry `GET /api/metamodels` lists for it, verbatim.
    pub listing: serde_json::Value,
    /// `false` when the node already held this key and nothing changed.
    pub added: bool,
}

/// Why [`GenericNode::add_metamodel`] declined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServeRefused {
    /// No description hook was installed; see
    /// [`GenericNode::enable_metamodel_upload`].
    NotEnabled,
    /// The application could not describe the text, and said why.
    Unreadable(String),
}

impl Display for ServeRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotEnabled => {
                write!(
                    f,
                    "adding a metamodel descriptor is not enabled on this node"
                )
            }
            Self::Unreadable(why) => write!(f, "not a descriptor this node can serve: {why}"),
        }
    }
}

impl std::error::Error for ServeRefused {}

impl From<HostError> for RegisterRefused {
    fn from(error: HostError) -> Self {
        match error {
            HostError::AlreadyHosted(log_id) => Self::AlreadyHosted(log_id),
        }
    }
}

/// The operations behind recently delivered events, keyed `origin:seq`.
///
/// The delivery trace names *which* event a log resolved; it deliberately does
/// not carry the operation, because `moirai-protocol` has no serialisation
/// bound on it. The network layer does — every operation it sends or receives
/// has just been through serde — so the join happens here.
///
/// **Only operations this replica originated.** A remote delivery reports its
/// outcome without a payload, because the replica that originated it is
/// reporting the payload already and the dashboard joins the two by event id.
/// Keeping a copy on every replica would clone every operation N times to send
/// the same bytes N times, which is the one thing a monitoring path must not
/// do: measured, it was most of a 4% throughput regression.
///
/// Bounded and FIFO. An event whose operation has already been evicted is still
/// reported, without its payload: a feed missing a body is better than a buffer
/// that grows for as long as the replica runs.
///
/// Keyed by `origin:seq` alone because the delivery trace it is joined with
/// carries no log id. On a node hosting several logs one key can therefore
/// name one event per log, and the join is then by arrival order; the
/// dashboard reads the default log's state, so this is an imprecision of the
/// monitoring path and not of replication.
#[derive(Debug)]
struct OpsByEvent<O> {
    by_id: crate::HashMap<String, O>,
    order: std::collections::VecDeque<String>,
}

impl<O> Default for OpsByEvent<O> {
    fn default() -> Self {
        Self {
            by_id: crate::HashMap::default(),
            order: std::collections::VecDeque::new(),
        }
    }
}

impl<O> OpsByEvent<O> {
    /// Roughly one second of a busy rig at 100 operations per second, per
    /// replica. Reports flush several times a second, so nothing that is going
    /// to be reported is ever evicted first in practice.
    const CAPACITY: usize = 1024;

    fn remember(&mut self, key: String, op: O) {
        if self.by_id.contains_key(&key) {
            return;
        }
        if self.order.len() >= Self::CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.by_id.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.by_id.insert(key, op);
    }

    fn take(&mut self, key: &str) -> Option<O> {
        self.by_id.remove(key)
    }
}

/// Envelope for ops submitted, with a oneshot reply channel.
pub struct OpEnvelope<O> {
    pub op: O,
    /// The log to apply to; `None` is the default log.
    pub log_id: Option<LogId>,
    pub reply: Sender<OpResult>,
}

/// Control commands sent by external adapters (for example, HTTP).
pub(crate) enum ControlCmd {
    Pause {
        peer_id: String,
        reply: Sender<OpResult>,
    },
    Resume {
        peer_id: String,
        reply: Sender<OpResult>,
    },
    PauseAll {
        reply: Sender<OpResult>,
    },
    ResumeAll {
        reply: Sender<OpResult>,
    },
    Peers {
        reply: Sender<serde_json::Value>,
    },
    Query {
        reply: Sender<serde_json::Value>,
    },
    Operations {
        reply: Sender<serde_json::Value>,
    },
    Metrics {
        reply: Sender<serde_json::Value>,
    },
    Leave {
        reply: Sender<OpResult>,
    },
    /// The hosted models, for `GET /api/models`.
    Models {
        reply: Sender<serde_json::Value>,
    },
    /// `POST /api/models`. See [`GenericNode::register`].
    Register {
        model_id: Option<LogId>,
        metamodel_id: serde_json::Value,
        reply: Sender<Result<Registered, RegisterRefused>>,
    },
    /// One model's state; `None` when the node does not host it.
    QueryLog {
        log_id: LogId,
        reply: Sender<Option<serde_json::Value>>,
    },
    /// One model's counters; `None` when the node does not host it.
    LogMetrics {
        log_id: LogId,
        reply: Sender<Option<serde_json::Value>>,
    },
    /// What to serve on one model's metamodel route: `None` when the node
    /// does not host it, and otherwise a [`BoundDescriptor`].
    Binding {
        log_id: LogId,
        reply: Sender<Option<BoundDescriptor>>,
    },
    /// `POST /api/metamodels`. See [`GenericNode::add_metamodel`].
    ///
    /// The descriptor list is the event loop's, like everything else the node
    /// holds, so the HTTP thread asks for the change rather than making it.
    AddMetamodel {
        text: String,
        reply: Sender<Result<Served, ServeRefused>>,
    },
    /// Whether the node hosts a log.
    Hosts {
        log_id: LogId,
        reply: Sender<bool>,
    },
}

/// What `GET /api/model/{id}/metamodel` is to answer for one hosted log.
///
/// Three answers rather than the key alone, because "no key to serve" and
/// "no binding at all" are different questions with different answers, and
/// the first one is not always a key this node fails to find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundDescriptor {
    /// Serve the descriptor this key names, and answer 404 when this node
    /// holds none: a binding nothing has contradicted.
    Key(String),
    /// A log hosted with no binding — the default log — which is answered
    /// the node's first descriptor, as `GET /api/metamodel` is.
    Unbound,
    /// The log was bound to one metamodel and the model is written in
    /// another, so this node serves no descriptor for it and says so with a
    /// 404.
    ///
    /// Its own answer rather than a missing key, because the key a wrong
    /// binding carries is very often one this node *does* hold — that is how
    /// the binding came to be held in the first place — and looking it up
    /// would answer a descriptor for a language the model is not written in.
    Disputed,
}

/// Result sent back to HTTP callers.
#[derive(Debug, Clone, Serialize)]
pub struct OpResult {
    pub success: bool,
    pub message: String,
}

impl<L: IsLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    fn unit_result_to_op_result<E: Display>(
        result: Result<(), E>,
        success_message: String,
        error_prefix: String,
    ) -> OpResult {
        match result {
            Ok(()) => OpResult {
                success: true,
                message: success_message,
            },
            Err(e) => OpResult {
                success: false,
                message: format!("{}: {}", error_prefix, e),
            },
        }
    }

    /// Create a node with the given transport.
    /// Transport-agnostic creator
    ///
    /// Mints a fresh [`LogId`] for the default log, which is right for the
    /// replica that creates a log and wrong for one joining a log that already
    /// exists — use [`with_transport_and_log_id`] wherever the log is shared.
    ///
    /// [`with_transport_and_log_id`]: GenericNode::with_transport_and_log_id
    pub fn with_transport(replica_id: String, members: &[&str], transport: T) -> Self {
        Self::with_transport_and_log_id(replica_id, members, transport, LogId::generate())
    }

    /// Create a node with the given transport, hosting the log named `log_id`
    /// as its default log.
    pub fn with_transport_and_log_id(
        replica_id: String,
        members: &[&str],
        transport: T,
        log_id: LogId,
    ) -> Self {
        let interner = Interner::new().into_shared();
        let replica: LogReplica<L> = IsReplica::bootstrap_with_log_id_and_interner(
            replica_id.clone(),
            members,
            log_id.clone(),
            interner.clone(),
        );
        let (adapter_op_tx, adapter_op_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();

        Self {
            replica_id,
            logs: BTreeMap::from([(log_id.clone(), HostedLog::new(replica))]),
            default_log: log_id,
            interner,
            frames_not_hosted: 0,
            seen_not_hosted: BTreeSet::new(),
            transport,
            adapter_op_rx,
            adapter_op_tx,
            ctrl_rx,
            ctrl_tx,
            query_fn: None,
            adopt_fn: None,
            discovery: None,
            export_log: None,
            import_log: None,
            descriptors: Arc::new(RwLock::new(Vec::new())),
            describe_fn: None,
            descriptor_key_fn: None,
            register_fn: None,
            op_guard_fn: None,
            dashboard: None,
            ops_by_event: OpsByEvent::default(),
            last_report: None,
            last_state_render: None,
            started_at: Instant::now(),
        }
    }

    /// Host one more log, with no history: the primitive behind registration.
    ///
    /// The log shares this node's member table and starts empty; if state
    /// transfer is enabled the event loop asks the connected peers for its
    /// state exactly as it does for the default log. A node hosts a log only
    /// through this call — never because a frame for the id arrived.
    pub fn host_log(&mut self, log_id: LogId) -> Result<(), HostError> {
        if self.logs.contains_key(&log_id) {
            return Err(HostError::AlreadyHosted(log_id));
        }
        let replica: LogReplica<L> = IsReplica::bootstrap_with_log_id_and_interner(
            self.replica_id.clone(),
            &[self.replica_id.as_str()],
            log_id.clone(),
            self.interner.clone(),
        );
        self.seen_not_hosted.remove(&log_id);
        self.logs.insert(log_id, HostedLog::new(replica));
        Ok(())
    }

    /// Note that an id was seen on a frame for a log this node does not host.
    /// Called beside every `frames_not_hosted` increment. See the field.
    fn note_not_hosted(&mut self, log_id: &LogId) {
        if self.seen_not_hosted.contains(log_id) {
            return;
        }
        if self.seen_not_hosted.len() >= SEEN_NOT_HOSTED_CAP {
            return;
        }
        self.seen_not_hosted.insert(log_id.clone());
    }

    /// `true` when this node hosts the log named `log_id`.
    pub fn hosts(&self, log_id: &LogId) -> bool {
        self.logs.contains_key(log_id)
    }

    /// The hosted log named `log_id`, with its causal bookkeeping.
    pub fn hosted(&self, log_id: &LogId) -> Option<&LogReplica<L>> {
        self.logs.get(log_id).map(|log| &log.replica)
    }

    /// The ids of every log this node hosts, in id order.
    pub fn hosted_logs(&self) -> impl Iterator<Item = &LogId> {
        self.logs.keys()
    }

    /// Frames dropped because no hosted log carried their id. See the field.
    pub fn frames_not_hosted(&self) -> u64 {
        self.frames_not_hosted
    }

    /// The ids of logs this node does not host but has seen traffic for, in
    /// id order. See the `seen_not_hosted` field for what bounds it.
    pub fn seen_not_hosted(&self) -> impl Iterator<Item = &LogId> {
        self.seen_not_hosted.iter()
    }

    /// Start reporting to a dashboard.
    ///
    /// Purely additive, exactly like [`enable_discovery`]: not calling this
    /// leaves the replica as it was, with no thread, no outbound request, and
    /// the delivery trace switched off.
    ///
    /// [`enable_discovery`]: GenericNode::enable_discovery
    pub fn enable_dashboard(&mut self, config: DashboardConfig) {
        self.dashboard = Some(DashboardSink::spawn(config));
    }

    /// Serve `descriptor` verbatim on `GET /api/metamodel`.
    ///
    /// The one-descriptor form of [`serve_metamodels`]: the descriptor is
    /// unkeyed and unlisted, so no model can be registered under it. Purely
    /// additive, exactly like [`enable_dashboard`]: not calling this leaves
    /// the endpoint answering 404 as it always has. The HTTP adapter
    /// snapshots the descriptors when it spawns, so call this before
    /// [`start_http`].
    ///
    /// [`serve_metamodels`]: GenericNode::serve_metamodels
    /// [`enable_dashboard`]: GenericNode::enable_dashboard
    /// [`start_http`]: GenericNode::start_http
    pub fn serve_metamodel(&mut self, descriptor: String) {
        self.serve_metamodels(vec![ServedDescriptor::unlisted(descriptor)]);
    }

    /// Serve `descriptors`: the first on `GET /api/metamodel`, every listed
    /// one on `GET /api/metamodels`, and each on `GET /api/model/{id}/metamodel`
    /// for the models registered under its key. Replaces whatever was served
    /// before, which is what makes this the *start-up* call; while the node
    /// runs, [`add_metamodel`] adds one.
    ///
    /// [`add_metamodel`]: GenericNode::add_metamodel
    pub fn serve_metamodels(&mut self, descriptors: Vec<ServedDescriptor>) {
        *self.descriptors_mut() = descriptors;
    }

    /// The descriptors this node serves, for reading.
    fn descriptors(&self) -> RwLockReadGuard<'_, Vec<ServedDescriptor>> {
        self.descriptors
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The descriptors this node serves, for writing. Called from the event
    /// loop and from the application's own thread before the node runs, never
    /// from the HTTP thread.
    fn descriptors_mut(&self) -> std::sync::RwLockWriteGuard<'_, Vec<ServedDescriptor>> {
        self.descriptors
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Install the application's description hook, which is what lets a
    /// descriptor be added while the node runs; see [`DescribeFn`] and
    /// [`add_metamodel`].
    ///
    /// Purely additive: without it `POST /api/metamodels` answers 501 and the
    /// node serves exactly the descriptors it was started with.
    ///
    /// [`add_metamodel`]: GenericNode::add_metamodel
    pub fn enable_metamodel_upload(&mut self, describe: DescribeFn) {
        self.describe_fn = Some(describe);
    }

    /// Install the application's hook for a descriptor that arrives inside a
    /// log, which is what makes a metamodel obtained by joining a model
    /// visible at this node's API; see [`AdoptFn`] and
    /// [`resolve_bindings`].
    ///
    /// Purely additive: without it a log joined under an unheld metamodel
    /// still merges and still reads out, and its binding simply stays pending
    /// for ever, so `GET /api/model/{id}/metamodel` keeps answering 404 for
    /// it. It needs [`enable_metamodel_upload`] as well, because adoption
    /// goes through [`add_metamodel`] and therefore through the application's
    /// own [`DescribeFn`], which is what re-derives the key from the bytes: a
    /// peer cannot install a descriptor under a key it does not hash to.
    ///
    /// [`resolve_bindings`]: GenericNode::resolve_bindings
    /// [`enable_metamodel_upload`]: GenericNode::enable_metamodel_upload
    /// [`add_metamodel`]: GenericNode::add_metamodel
    pub fn enable_descriptor_adoption(&mut self, adopt: AdoptFn<L>) {
        self.adopt_fn = Some(adopt);
    }

    /// Check every bound log against the descriptor the model itself
    /// carries, once the model carries one: serve it when this node did not
    /// have it, and mark the binding foreign when it is not the one the log
    /// was bound under.
    ///
    /// # Why a held binding is checked too
    ///
    /// A registration names a metamodel by key and the node has no way to
    /// check it at that moment: a join writes nothing and the log is empty,
    /// so there is nothing to check the key against. Whether this node
    /// happens to hold a descriptor under that key is a fact about *this
    /// node*, not about the model, and the two cases differ only in how long
    /// the mistake stays invisible. A key this node does not hold leaves the
    /// binding `Pending`, and every route says so: the model reads out empty,
    /// a local write is refused, `GET /api/model/{id}/metamodel` is 404. A
    /// key this node *does* hold binds straight to `Held`, and every route
    /// answers as though the binding were known to be right — including the
    /// metamodel route, which then serves a descriptor for a language the
    /// model is not written in, and an editor that types the model against
    /// it reports the model's own classes as unknown.
    ///
    /// So both are checked, against the same thing and by the same rule: the
    /// descriptor inside the model's own history, described through the
    /// application's [`DescribeFn`], which re-derives the key from the bytes.
    /// The key is compared and nothing inside either descriptor is read. A
    /// mismatch is `Foreign` in both directions, and `Foreign` is terminal.
    ///
    /// # What it costs per pass
    ///
    /// One `Option` clone per unconfirmed binding, and a description only
    /// when the model has brought a descriptor — after which the binding is
    /// confirmed and never looked at again. A model created here confirms on
    /// the pass after its opening operation; a model joined here confirms
    /// when its history lands. So the steady state is what it was: nothing
    /// but the filter, once per hosted log.
    fn resolve_bindings(&mut self) {
        let Some(adopt) = self.adopt_fn else {
            return;
        };
        let arrived: Vec<(LogId, String, BindingState, String)> = self
            .logs
            .iter()
            .filter_map(|(log_id, log)| {
                let binding = log.binding.as_ref().filter(|binding| match binding.state {
                    BindingState::Pending => true,
                    // Held is a claim until the model is heard from, and
                    // this is the hearing. Once.
                    BindingState::Held => !binding.confirmed,
                    BindingState::Foreign => false,
                })?;
                let text = adopt(&log.replica)?;
                Some((
                    log_id.clone(),
                    binding.key.clone(),
                    binding.state,
                    text,
                ))
            })
            .collect();
        for (log_id, key, was, text) in arrived {
            let outcome = self.add_metamodel(text);
            let state = match &outcome {
                Ok(served) if served.key == key => BindingState::Held,
                _ => BindingState::Foreign,
            };
            if let Some(binding) = self
                .logs
                .get_mut(&log_id)
                .and_then(|log| log.binding.as_mut())
            {
                binding.state = state;
                binding.confirmed = true;
            }
            match outcome {
                // The model agrees with the key it was bound under. Said
                // out loud only when this node did not hold the descriptor
                // before, because that is the event: the language arrived.
                Ok(_) if state == BindingState::Held && was == BindingState::Held => {}
                Ok(served) if state == BindingState::Held => eprintln!(
                    "[{}] model {} brought its own metamodel {}: now served here",
                    self.replica_id, log_id, served.key
                ),
                Ok(served) if was == BindingState::Held => eprintln!(
                    "[{}] model {} was joined under metamodel {}, which this node holds, but \
                     the model is written in {}: the binding is wrong and this node serves no \
                     descriptor for the model",
                    self.replica_id, log_id, key, served.key
                ),
                Ok(served) => eprintln!(
                    "[{}] model {} was joined under metamodel {} and carries {} instead; \
                     its descriptor is not served for it",
                    self.replica_id, log_id, key, served.key
                ),
                Err(refused) => eprintln!(
                    "[{}] the descriptor model {} carries cannot be served here: {}",
                    self.replica_id, log_id, refused
                ),
            }
        }
    }

    /// What `GET /api/model/{id}/metamodel` is to answer for one log:
    /// `None` when this node does not host it.
    fn bound_descriptor(&self, log_id: &LogId) -> Option<BoundDescriptor> {
        self.logs.get(log_id).map(|log| match &log.binding {
            None => BoundDescriptor::Unbound,
            // A foreign binding names a key this node may well hold — that
            // is how it came to be held — and serving it is the whole of
            // what this state exists to prevent.
            Some(binding) if binding.state == BindingState::Foreign => BoundDescriptor::Disputed,
            Some(binding) => BoundDescriptor::Key(binding.key.clone()),
        })
    }

    /// Serve one more descriptor, now: the primitive behind
    /// `POST /api/metamodels`.
    ///
    /// The text is handed to the application's [`DescribeFn`], which answers
    /// with the key and the listing entry or with the reason it will not.
    /// A key this node already holds changes nothing and is answered
    /// `added: false`, so posting the same descriptor twice — to a node, or
    /// to a node and then to its restarted self — is not an error.
    ///
    /// The model registered under the new key afterwards is registered by the
    /// path every other model takes: nothing here hosts a log.
    pub fn add_metamodel(&mut self, text: String) -> Result<Served, ServeRefused> {
        let describe = self.describe_fn.ok_or(ServeRefused::NotEnabled)?;
        let descriptor = describe(&text).map_err(ServeRefused::Unreadable)?;
        let mut descriptors = self.descriptors_mut();
        if descriptors.iter().any(|held| held.key == descriptor.key) {
            return Ok(Served {
                key: descriptor.key,
                listing: descriptor.listing,
                added: false,
            });
        }
        let served = Served {
            key: descriptor.key.clone(),
            listing: descriptor.listing.clone(),
            added: true,
        };
        descriptors.push(descriptor);
        drop(descriptors);
        eprintln!(
            "[{}] now serving metamodel descriptor {}",
            self.replica_id, served.key
        );
        Ok(served)
    }

    /// Install the application's registration hooks; see the fields.
    pub fn enable_registration(
        &mut self,
        descriptor_key_fn: fn(&serde_json::Value) -> Option<String>,
        register_fn: RegisterFn<L::Op>,
    ) {
        self.descriptor_key_fn = Some(descriptor_key_fn);
        self.register_fn = Some(register_fn);
    }

    /// Install the application's intake guard: `Err(reason)` from `guard`
    /// refuses an adapter-submitted operation on a registered log, and the
    /// caller is answered with a failed [`OpResult`] carrying the reason.
    /// The guard receives the log's id and the descriptor the log was
    /// registered under, and this node reads neither the operation nor the
    /// descriptor on its behalf.
    ///
    /// Purely additive, like [`enable_registration`]: without it every
    /// operation is applied as before. The default log is not guarded, and
    /// neither is a log hosted with no binding, nor the opening operations
    /// `register_fn` returns, which are applied before any adapter can reach
    /// the new log. Operations received from peers are never guarded: every
    /// honest peer ran its own guard at its own intake under the same
    /// descriptor, and a refusal on receive is what would let two replicas
    /// disagree.
    ///
    /// [`enable_registration`]: GenericNode::enable_registration
    pub fn enable_op_guard(&mut self, guard: OpGuardFn<L::Op>) {
        self.op_guard_fn = Some(guard);
    }

    /// Register a model: the primitive behind `POST /api/models`.
    ///
    /// Without a `model_id` this node *creates* the model: it mints a
    /// [`LogId`], hosts the log, and applies the opening operations the
    /// application's `register_fn` returns. With one it *joins*: it hosts the
    /// id with no history and writes nothing, and the event loop asks its
    /// peers for a state transfer as it does for any empty log. A join is
    /// refused when the id is hosted already.
    ///
    /// # The two answers to a metamodel this node holds no descriptor for
    ///
    /// They differ, and the asymmetry is the design's. A **create** is
    /// refused before anything is hosted, exactly as it always was: the
    /// opening operations are written from the descriptor by `register_fn`,
    /// and there is nothing to write them from. A **join** is hosted, with
    /// the binding left *pending*: the log carries its own metamodel in the
    /// operations it receives, so a joiner that hosts the empty log is a
    /// joiner that will hold the semantics as soon as the model's own history
    /// reaches it, by state transfer or by delta. Until then the log has no
    /// semantics here and refuses every local write on its own account.
    ///
    /// A `metamodel_id` `descriptor_key_fn` cannot read a key out of at all
    /// is refused either way: there would be nothing to bind to.
    ///
    /// # Neither answer is a check
    ///
    /// A join names a key, and this node cannot tell whether the model is
    /// written in it: the log is empty at that moment and there is nothing
    /// to compare. That is true of the key this node holds exactly as it is
    /// of the one it does not — the two differ in what the node can serve,
    /// not in what it knows. The check is
    /// [`resolve_bindings`](GenericNode::resolve_bindings), which runs when
    /// the model's own history brings the descriptor it was opened with.
    pub fn register(
        &mut self,
        model_id: Option<LogId>,
        metamodel_id: serde_json::Value,
    ) -> Result<Registered, RegisterRefused> {
        let (Some(descriptor_key), Some(register)) = (self.descriptor_key_fn, self.register_fn)
        else {
            return Err(RegisterRefused::NotEnabled);
        };
        let unknown = || RegisterRefused::UnknownMetamodel(metamodel_id.clone());
        let key = descriptor_key(&metamodel_id).ok_or_else(unknown)?;
        let held = self
            .descriptors()
            .iter()
            .find(|held| held.key == key)
            .cloned();
        let (log_id, created) = match model_id {
            Some(log_id) => (log_id, false),
            None => (LogId::generate(), true),
        };
        let descriptor = match &held {
            Some(descriptor) => Some(descriptor.clone()),
            // A create needs the descriptor in hand; a join does not.
            None if created => return Err(unknown()),
            None => None,
        };
        self.host_log(log_id.clone())?;
        let binding = Binding {
            key: key.clone(),
            metamodel_id: metamodel_id.clone(),
            state: if descriptor.is_some() {
                BindingState::Held
            } else {
                BindingState::Pending
            },
            // Neither answer is checked yet, and holding the descriptor is
            // not a check: it says this node could serve that key, not that
            // the model is written in it. `resolve_bindings` does the
            // checking, once the model's own history is here to check
            // against — for a create too, whose opening operation carries
            // the descriptor it was written from.
            confirmed: false,
        };
        if let Some(log) = self.logs.get_mut(&log_id) {
            log.binding = Some(binding);
        }
        if let (true, Some(descriptor)) = (created, &descriptor) {
            for op in register(&log_id, descriptor) {
                let result = self.apply_op_to(&log_id, op);
                if !result.success {
                    eprintln!(
                        "[{}] an opening operation of model {} was refused: {}",
                        self.replica_id, log_id, result.message
                    );
                }
            }
        }
        eprintln!(
            "[{}] {} model {} under metamodel {}{}",
            self.replica_id,
            if created { "created" } else { "joined" },
            log_id,
            metamodel_id,
            if descriptor.is_some() {
                ""
            } else {
                ", whose descriptor this node does not hold: waiting for it to arrive with the model"
            }
        );
        Ok(Registered {
            model_id: log_id,
            created,
        })
    }

    /// The hosted models as `GET /api/models` lists them: every log, with the
    /// `metamodel_id` it was registered under or `null` for the default log.
    ///
    /// Beside them, under `seen`, the ids of logs this node does *not* host
    /// but has seen traffic for — an added key on an object that was already
    /// an object, so every parser reading `models` keeps working. Ids only,
    /// with no binding: see the `seen_not_hosted` field for why none is
    /// knowable here. The two lists are disjoint by construction, because
    /// hosting an id removes it from the set.
    fn models(&self) -> serde_json::Value {
        let models: Vec<serde_json::Value> = self
            .logs
            .iter()
            .map(|(log_id, log)| {
                json!({
                    "model_id": log_id.as_str(),
                    "metamodel_id": log
                        .binding
                        .as_ref()
                        .map(|binding| binding.metamodel_id.clone())
                        .unwrap_or(serde_json::Value::Null),
                })
            })
            .collect();
        let seen: Vec<&str> = self.seen_not_hosted.iter().map(LogId::as_str).collect();
        json!({ "models": models, "seen": seen })
    }

    /// Start discovering peers through a bootnode.
    ///
    /// Purely additive: not calling this leaves the node exactly as it was
    /// before phase 1. `PEERS` keeps working either way and takes effect
    /// immediately, so a static list and a discovered one compose — the static
    /// entries are simply already in the address book when the first roster
    /// arrives.
    pub fn enable_discovery(&mut self, config: DiscoveryConfig) {
        eprintln!(
            "[{}] discovery on: {} session `{}` as {} every {:?}",
            self.replica_id,
            config.bootnode_url,
            config.session,
            config.advertise_addr,
            config.interval
        );
        self.discovery = Some(Discovery::spawn(config));
    }

    /// The default log: the one named at start, which the unscoped routes
    /// serve.
    pub fn log_id(&self) -> &LogId {
        &self.default_log
    }

    /// Get a sender that can be used to submit ops from other threads.
    pub fn op_sender(&self) -> Sender<OpEnvelope<L::Op>> {
        self.adapter_op_tx.clone()
    }

    /// Start the optional HTTP API on the given port.
    ///
    /// The HTTP implementation lives in `http_api.rs` and communicates with the
    /// generic node through channels.
    pub fn start_http(&self, port: u16) {
        crate::http_api::start_http_api::<L::Op>(
            port,
            self.replica_id.clone(),
            self.default_log.clone(),
            self.adapter_op_tx.clone(),
            self.ctrl_tx.clone(),
            Arc::clone(&self.descriptors),
        );
    }

    /// Ask `peer` for everything this node has not seen yet, log by log.
    ///
    /// The single place that turns "we have a link to `peer`" into a history
    /// pull. Every path that needs one — an accepted `Hello`, a resumed peer, a
    /// freshly dialled peer — funnels through here, so the four copies of
    /// `since()` -> `SyncRequest` -> `send()` that used to exist cannot drift
    /// apart.
    fn request_sync(&mut self, peer: &PeerId) {
        // A log that has been in the session asks for the delta, which is
        // what its peers can actually answer from their outboxes. A log that
        // has not needs the compacted state as well, and asking for a delta
        // would get it a correct answer to the wrong question: an empty batch
        // from a healthy peer, because everything it needs has already been
        // pruned.
        //
        // It does not ask *here*, though. `retry_state_transfer` owns the
        // choice of donor and runs on every pass of the loop, so a new link
        // only has to exist; asking from both places is what used to make a
        // joiner receive one full transfer per peer and then a second round of
        // them, and decode every one to keep the first.
        let wants_delta: Vec<LogId> = self
            .logs
            .iter()
            .filter(|(_, log)| self.import_log.is_none() || !log.has_no_history())
            .map(|(id, _)| id.clone())
            .collect();
        for log_id in &wants_delta {
            self.request_delta_sync(log_id, peer);
        }
    }

    /// Ask `peer` for the events of `log_id` it holds above what this node has
    /// already delivered.
    ///
    /// Answered out of the peer's outbox, which `prune_outbox` keeps to exactly
    /// the events above its stable frontier. That is the right question for a
    /// log that has been in the session; it is the wrong one for a log that
    /// has not, which is what `StateRequest` is for.
    fn request_delta_sync(&mut self, log_id: &LogId, peer: &PeerId) {
        let Some(log) = self.logs.get(log_id) else {
            return;
        };
        let since = log.replica.since();
        let msg = TransportMessage::SyncRequest { since };
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!(
                "[{}] Failed to request sync from {}: {}",
                self.replica_id, peer, e
            );
        }
    }

    /// Ask `peer` for everything of `log_id`, compacted state included.
    fn request_state_transfer(&mut self, log_id: &LogId, peer: &PeerId) {
        let msg = TransportMessage::StateRequest {
            id: self.replica_id.clone(),
            log_id: log_id.clone(),
        };
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!(
                "[{}] Failed to request a state transfer from {}: {}",
                self.replica_id, peer, e
            );
        }
    }

    /// Build the answer to a `StateRequest` from `requester` for `log_id`.
    ///
    /// The refusal below is the whole reason returning-member merge stays out
    /// of this phase rather than half-happening by accident. A requester this
    /// replica already has operations from is *returning* — evicted, or long
    /// partitioned — and it may hold operations the session has never seen.
    /// Adopting a snapshot is a replace, not a merge, so serving one would
    /// discard them silently. Refusing costs the requester one round trip and
    /// gives phase 3 a defined starting point.
    fn state_response_for(&self, requester: &str, log_id: &LogId) -> TransportMessage<L::Op> {
        let Some(log) = self.logs.get(log_id) else {
            // Not a joiner of anything held here. Stamped with the default
            // log — the transport's contract is "the log the donor hosts" —
            // and the reason names the id asked for beside what is hosted,
            // because that line is the only symptom an operator gets and
            // either id alone is ungreppable on the other side.
            let hosted: Vec<&str> = self.logs.keys().map(LogId::as_str).collect();
            return TransportMessage::StateUnavailable {
                reason: format!(
                    "`{requester}` asked for log {log_id}, but this replica does not host \
                     it (it hosts {}); a state transfer never crosses logs",
                    hosted.join(", ")
                ),
                log_id: self.default_log.clone(),
            };
        };
        let ours = log_id.clone();
        // Two replicas that start together are both empty and both ask. Serving
        // an empty snapshot would work, but it would make one of them adopt the
        // other's index ordering for nothing; saying there is nothing to give
        // lets both fall back to a delta sync, which is the right shape for
        // peers that are equals rather than donor and joiner.
        if log.has_no_history() {
            return TransportMessage::StateUnavailable {
                reason: "this replica has no history to transfer".to_string(),
                log_id: ours,
            };
        }
        if log.replica.has_history_for(requester) {
            return TransportMessage::StateUnavailable {
                reason: format!(
                    "`{requester}` is a returning member, not a fresh one; merging its \
                     history with a snapshot is not implemented"
                ),
                log_id: ours,
            };
        }
        let Some(export) = self.export_log else {
            return TransportMessage::StateUnavailable {
                reason: "state transfer is not enabled on this replica".to_string(),
                log_id: ours,
            };
        };
        // The fourth refusal, and the only one that is about size rather than
        // eligibility. Serialising to measure is not waste: this is the same
        // work `transport.send` is about to do, and it is what keeps an
        // oversized answer from being discovered by the relay instead of here.
        let exported = export(log.replica.log());
        let raw = match serde_json::to_vec(&exported) {
            Ok(bytes) => bytes,
            Err(e) => {
                return TransportMessage::StateUnavailable {
                    reason: format!("this replica's log could not be serialised: {e}"),
                    log_id: ours,
                };
            }
        };
        let log_bytes = raw.len();
        if log_bytes > MAX_STATE_TRANSFER_BYTES {
            eprintln!(
                "[{}] refusing a state transfer of log {} to {}: the log is {} bytes, ceiling is {}",
                self.replica_id, ours, requester, log_bytes, MAX_STATE_TRANSFER_BYTES
            );
            return TransportMessage::StateUnavailable {
                reason: format!(
                    "this replica's log is {log_bytes} bytes, above the \
                     {MAX_STATE_TRANSFER_BYTES} byte ceiling on one state transfer"
                ),
                log_id: ours,
            };
        }
        // The size is the serialised log before compression: what the ceiling
        // above was checked against, and the one number that says whether a
        // transfer tracks its own log's size or the sum of everything hosted.
        eprintln!(
            "[{}] serving a state transfer to {} for log {}: {} bytes",
            self.replica_id, requester, ours, log_bytes
        );
        TransportMessage::StateResponse {
            snapshot: log.replica.snapshot(),
            log: LogPayload::encode(exported, &raw),
            log_id: ours,
        }
    }

    /// Install a donor's state for `log_id`, or explain why not.
    fn adopt_state(
        &mut self,
        log_id: &LogId,
        from: &PeerId,
        snapshot: moirai_protocol::broadcast::tcsb::StateSnapshot<L::Op>,
        payload: LogPayload,
    ) {
        let Some(import) = self.import_log else {
            eprintln!(
                "[{}] {} sent a state transfer but this replica cannot accept one",
                self.replica_id, from
            );
            return;
        };
        let Some(log) = self.logs.get(log_id) else {
            return;
        };
        // A second donor's answer to the same request must not undo the first.
        // `adopt` replaces rather than merges, so re-adopting after delivering
        // anything would silently roll the log back.
        if !log.has_no_history() {
            self.request_delta_sync(log_id, from);
            return;
        }
        // Adopting takes over the donor's index ordering, and the ordering is
        // the node's, shared by every hosted log. That is safe while no other
        // log here holds anything, delivered or merely received, and it is
        // free of any rebuild when the donor's ordering agrees with ours on
        // every index both know. Otherwise the snapshot cannot be installed
        // without rewriting another log's clock, so it is turned away in
        // favour of a delta sync — loudly, since the compacted prefix of
        // `log_id` then stays out of reach.
        let siblings_with_history = self
            .logs
            .iter()
            .any(|(id, log)| id != log_id && !log.holds_nothing());
        if siblings_with_history
            && !lock_interner(&self.interner)
                .resolver()
                .agrees_with(snapshot.resolver())
        {
            eprintln!(
                "[{}] cannot adopt {}'s state for log {}: it orders the members differently \
                 from this node, which already holds history in another log; falling back \
                 to a delta sync",
                self.replica_id, from, log_id
            );
            self.request_delta_sync(log_id, from);
            return;
        }
        match payload.decode().and_then(import) {
            Some(state) => {
                let members = snapshot.resolver().len();
                let Some(log) = self.logs.get_mut(log_id) else {
                    return;
                };
                log.replica.adopt(snapshot, state);
                let stability = log.replica.stability();
                eprintln!(
                    "[{}] adopted state from {} for log {}: {} members, stable prefix {}, {} events above it",
                    self.replica_id,
                    from,
                    log_id,
                    members,
                    stability.stable_prefix,
                    stability.retained,
                );
                // The snapshot is a point in time, and `adopt` discards
                // whatever this log had buffered before it. A delta sync
                // closes both gaps in one round trip.
                self.request_delta_sync(log_id, from);
            }
            None => {
                eprintln!(
                    "[{}] could not decode the log {} sent; falling back to a delta sync",
                    self.replica_id, from
                );
                self.request_delta_sync(log_id, from);
            }
        }
    }

    /// Connect to known peers, then pull history from each new link.
    ///
    /// The pull is what makes a late joiner work. `Hello` travels dialer ->
    /// acceptor and the *acceptor* answers it with a `SyncRequest`, so without
    /// this the dialer pushes its history and receives none. A symmetric
    /// `PEERS` list hides that — both replicas are dialer and acceptor at once
    /// — but a node that starts after the others is only ever a dialer.
    pub fn connect(&mut self) {
        match self.transport.connect_to_peers() {
            Ok(new_peers) => {
                for peer in new_peers {
                    self.request_sync(&peer);
                }
            }
            Err(e) => {
                eprintln!("[{}] Some peer connections failed: {}", self.replica_id, e);
            }
        }
    }

    /// Apply an operation to the default log: send to the CRDT, then broadcast
    /// to peers.
    pub fn apply_op(&mut self, op: L::Op) -> OpResult {
        let default_log = self.default_log.clone();
        self.apply_op_to(&default_log, op)
    }

    /// The intake for operations submitted through an adapter: the guard,
    /// when one is installed, runs before a registered log applies anything,
    /// and is handed the descriptor that log was registered under. `None`
    /// names the default log, which is never guarded; neither is a log hosted
    /// with no binding.
    fn submit_op(&mut self, log_id: Option<LogId>, op: L::Op) -> OpResult {
        let log_id = log_id.unwrap_or_else(|| self.default_log.clone());
        if let Some(guard) = self.op_guard_fn {
            let key = self
                .logs
                .get(&log_id)
                .and_then(|log| log.binding.as_ref())
                .map(|binding| binding.key.clone());
            if let Some(key) = key {
                // The descriptor `register` matched for this log, looked up
                // again by its key rather than remembered by index, so the
                // guard can never be handed another descriptor's text.
                let held = self
                    .descriptors()
                    .iter()
                    .find(|held| held.key == key)
                    .cloned();
                let Some(descriptor) = held else {
                    return OpResult {
                        success: false,
                        message: format!(
                            "log {log_id} is bound to descriptor {key}, which this node no \
                             longer holds"
                        ),
                    };
                };
                if let Err(reason) = guard(&log_id, &descriptor, &op) {
                    return OpResult {
                        success: false,
                        message: reason,
                    };
                }
            }
        }
        self.apply_op_to(&log_id, op)
    }

    /// Apply an operation to the log named `log_id`: send to the CRDT, then
    /// broadcast to peers. An id this node does not host is answered with a
    /// failed [`OpResult`] and applies nothing. This is the primitive under
    /// [`submit_op`] and the registration hook; the adapter intake is
    /// guarded, this is not.
    ///
    /// [`submit_op`]: GenericNode::submit_op
    pub fn apply_op_to(&mut self, log_id: &LogId, op: L::Op) -> OpResult {
        let Some(log) = self.logs.get_mut(log_id) else {
            return OpResult {
                success: false,
                message: format!("log {log_id} is not hosted here"),
            };
        };
        let Some(event_msg) = log.replica.send(op.clone()) else {
            // A pending binding is the one refusal whose reason this node
            // knows without reading the log: it joined the model under a
            // metamodel it holds no descriptor for, and nothing can be
            // written into a log whose semantics have not arrived.
            let pending = log
                .binding
                .as_ref()
                .filter(|binding| binding.state != BindingState::Held)
                .map(|binding| binding.key.clone());
            return OpResult {
                success: false,
                message: match pending {
                    Some(key) => format!(
                        "model {log_id} was joined under metamodel {key}, which this node holds \
                         no descriptor for: the model's metamodel has not arrived yet, and a \
                         local write waits for it"
                    ),
                    None => "Operation not enabled".to_string(),
                },
            };
        };
        // Record the operation
        log.operation_log.push(op);
        log.local_ops += 1;
        self.remember_op(event_msg.event());

        let transport_msg = TransportMessage::Event { event: event_msg };
        if let Err(e) = self.transport.broadcast(transport_msg) {
            eprintln!("[{}] Broadcast failed: {}", self.replica_id, e);
        }
        OpResult {
            success: true,
            message: "Applied and broadcasted".to_string(),
        }
    }

    /// Handle an inbound transport message: hand it to the log whose id it
    /// carries, or filter it. The inbound primitive, the mirror of
    /// [`apply_op_to`]: what the event loop calls for every frame the
    /// transport yields, public so a harness can deliver frames by hand and
    /// time this path on its own.
    ///
    /// [`apply_op_to`]: GenericNode::apply_op_to
    pub fn handle_transport_message(&mut self, from: PeerId, msg: TransportMessage<L::Op>) {
        match msg {
            TransportMessage::Event { event } => match self.logs.get_mut(event.log_id()) {
                Some(log) => {
                    log.operation_log.push(event.event().op().clone());
                    log.replica.receive(event);
                }
                None => {
                    self.frames_not_hosted += 1;
                    let log_id = event.log_id().clone();
                    self.note_not_hosted(&log_id);
                }
            },
            TransportMessage::Batch { batch } => match self.logs.get_mut(batch.log_id()) {
                Some(log) => {
                    for event in batch.batch().events() {
                        log.operation_log.push(event.op().clone());
                    }
                    log.replica.receive_batch(batch);
                }
                None => {
                    self.frames_not_hosted += 1;
                    let log_id = batch.log_id().clone();
                    self.note_not_hosted(&log_id);
                }
            },
            TransportMessage::SyncRequest { since } => {
                let Some(log) = self.logs.get_mut(since.log_id()) else {
                    self.frames_not_hosted += 1;
                    let log_id = since.log_id().clone();
                    self.note_not_hosted(&log_id);
                    return;
                };
                let batch = log.replica.pull(since);
                let response = TransportMessage::Batch { batch };
                if let Err(e) = self.transport.send(&from, response) {
                    eprintln!(
                        "[{}] Failed to send batch to {}: {}",
                        self.replica_id, from, e
                    );
                }
            }
            TransportMessage::StateRequest { id, log_id } => {
                if !self.logs.contains_key(&log_id) {
                    self.frames_not_hosted += 1;
                    self.note_not_hosted(&log_id);
                }
                let response = self.state_response_for(&id, &log_id);
                if let Err(e) = self.transport.send(&from, response) {
                    eprintln!(
                        "[{}] Failed to answer the state request from {}: {}",
                        self.replica_id, from, e
                    );
                }
            }
            TransportMessage::StateResponse {
                snapshot,
                log,
                log_id,
            } => {
                // The mirror of the donor-side refusal, because the donor is
                // not the only way a foreign snapshot can arrive: an old donor
                // that predates the check, or a misrouted frame, must not make
                // this node adopt a log it does not host.
                if self.logs.contains_key(&log_id) {
                    self.adopt_state(&log_id, &from, snapshot, log);
                } else {
                    self.frames_not_hosted += 1;
                    self.note_not_hosted(&log_id);
                }
            }
            TransportMessage::StateUnavailable { reason, log_id } => {
                eprintln!(
                    "[{}] {} will not serve a state transfer ({}); falling back to a delta sync",
                    self.replica_id, from, reason
                );
                // A refusal is an answer. Free the turn now rather than waiting
                // out the deadline, so cycling through peers that have nothing
                // to give costs a loop iteration each.
                //
                // The refusal is stamped with the log the donor answered for.
                // When this node hosts it, that log's turn with `from` is
                // over. When it does not, the donor turned the request away as
                // one for a log it lacks and stamped its own, so every log
                // here that was waiting on `from` is freed: the refused
                // request was one of theirs.
                let waiting: Vec<LogId> = if self.logs.contains_key(&log_id) {
                    vec![log_id]
                } else {
                    self.logs
                        .iter()
                        .filter(|(_, log)| log.transfer.donor.as_deref() == Some(from.as_str()))
                        .map(|(id, _)| id.clone())
                        .collect()
                };
                for log_id in &waiting {
                    if let Some(log) = self.logs.get_mut(log_id) {
                        if log.transfer.donor.as_deref() == Some(from.as_str()) {
                            log.transfer.donor = None;
                        }
                    }
                    self.request_delta_sync(log_id, &from);
                }
            }
            TransportMessage::Hello { id, .. } => {
                eprintln!("[{}] Peer connected: {}", self.replica_id, id);
                self.request_sync(&id);
            }
            TransportMessage::Goodbye { id } => {
                eprintln!("[{}] Peer disconnected: {}", self.replica_id, id);
            }
            _ => {}
        }
    }

    /// Keep the operation behind `event`, so a delivery record can be joined
    /// with what was actually applied. Local operations only — see
    /// [`OpsByEvent`]. No-op unless a dashboard is attached.
    fn remember_op(&mut self, event: &moirai_protocol::event::Event<L::Op>) {
        if self.dashboard.is_none() {
            return;
        }
        let id = event.id();
        self.ops_by_event.remember(
            format!("{}:{}", id.origin_id(), id.seq()),
            event.op().clone(),
        );
    }

    /// Hand everything the CRDT decided since the last pass to the sender
    /// thread.
    ///
    /// The whole cost paid here is: drain a `Vec`, build one small owned struct
    /// per delivery, and `try_send` it. No JSON is encoded, no HTTP happens,
    /// and the operation is *moved* out of the pending map rather than cloned.
    /// Called once per event-loop iteration, so a record's timestamp trails the
    /// delivery by at most one iteration — 10 ms, which is the resolution of
    /// any propagation time computed from these and is stated rather than
    /// papered over.
    fn collect_deliveries(&mut self) {
        let Some(sink) = &self.dashboard else {
            return;
        };
        let deliveries = moirai_protocol::state::trace::drain();
        if deliveries.is_empty() {
            return;
        }
        let ts_ms = now_ms();
        for delivery in deliveries {
            let origin = delivery.id.origin_id().to_string();
            let seq = delivery.id.seq();
            let id = format!("{origin}:{seq}");
            let local = origin == self.replica_id;
            // Only a local delivery has a payload to attach; see `OpsByEvent`.
            let op = if local {
                self.ops_by_event.take(&id)
            } else {
                None
            };
            sink.offer_event(EventRecord {
                local,
                superseded: delivery
                    .superseded
                    .into_iter()
                    .map(|s| (format!("{}:{}", s.id.origin_id(), s.id.seq()), s.concurrent))
                    .collect(),
                id,
                origin,
                seq,
                lamport: delivery.lamport,
                ts_ms,
                applied: delivery.applied,
                redundant_on_arrival: delivery.redundant_on_arrival,
                reset: delivery.reset,
                op,
            });
        }
    }

    /// Hand over a fresh view of this node's default log, if one is due.
    ///
    /// Two clocks, not one. The counters are a handful of integers and go out
    /// every `interval`; rendering the model is `O(state)` on a state that
    /// grows for as long as the session runs, and goes out every
    /// `state_interval`, which is several times slower. That split is the
    /// difference between reporting costing 13% of sustained throughput and
    /// costing nothing measurable — see `experiments/t-dashboard-overhead/`.
    fn report_to_dashboard(&mut self) {
        let Some(sink) = &self.dashboard else {
            return;
        };
        let interval = sink.interval();
        let state_interval = sink.state_interval();
        let now = Instant::now();
        if self
            .last_report
            .is_some_and(|last| now.duration_since(last) < interval)
        {
            return;
        }
        self.last_report = Some(now);

        let render_due = self
            .last_state_render
            .is_none_or(|last| now.duration_since(last) >= state_interval);
        let state = match (&self.query_fn, render_due, self.logs.get(&self.default_log)) {
            (Some(f), true, Some(log)) => {
                self.last_state_render = Some(now);
                Some(f(&log.replica))
            }
            _ => None,
        };
        let peers = self
            .transport
            .peers()
            .into_iter()
            .map(|p| (p.id, format!("{:?}", p.status)))
            .collect();
        let snapshot = SnapshotRecord {
            uptime_ms: now.duration_since(self.started_at).as_millis() as u64,
            metrics: self.metrics(),
            state,
            peers,
        };
        // Borrow again: `metrics()` needs `&self`, so the sink reference above
        // cannot still be alive.
        if let Some(sink) = &self.dashboard {
            sink.offer_snapshot(snapshot);
        }
    }

    /// Run the main event loop.
    pub fn run(&mut self) {
        // The delivery trace is thread-local and delivery happens on this
        // thread, so it can only be switched on from here.
        if self.dashboard.is_some() {
            moirai_protocol::state::trace::set_enabled(true);
        }
        eprintln!("[{}] Entering main event loop", self.replica_id);
        loop {
            // --- Adapter-submitted operations ---
            while let Ok(OpEnvelope { op, log_id, reply }) = self.adapter_op_rx.try_recv() {
                let result = self.submit_op(log_id, op);
                let _ = reply.send(result);
            }

            // --- Control commands (pause/resume/peers) ---
            while let Ok(cmd) = self.ctrl_rx.try_recv() {
                self.handle_control_cmd(cmd);
            }

            // --- Peers the bootnode has told us about since the last pass ---
            self.reconcile_discovered_peers();

            // --- Still nothing? Ask again. ---
            self.retry_state_transfer();

            // --- A model may have brought its own metamodel by now, and
            //     either it is the one the log was bound under or the
            //     binding is wrong. ---
            self.resolve_bindings();

            // --- Accept new inbound TCP connections ---
            self.transport.accept_connections().ok();

            // --- Inbound network messages ---
            while let Ok(Some((from, msg))) = self.transport.try_recv() {
                self.handle_transport_message(from, msg);
            }

            // --- Monitoring, if anyone asked for it ---
            self.collect_deliveries();
            self.report_to_dashboard();

            thread::sleep(Duration::from_millis(10));
        }
    }

    /// For every hosted log that still has nothing, ask *one* peer for a state
    /// transfer, and move on if it does not deliver.
    ///
    /// This used to ask every connected peer on every round, on the grounds
    /// that a refusal is cheap and that a donor dying mid-transfer must not
    /// strand the joiner. The first half of that is true; the second half does
    /// not need it. A peer that *can* serve sends its whole log, so asking all
    /// of them costs N transfers where one would do — measured on containers, a
    /// joiner at 2 735 operations pulled 3.1 MB across 2 x N answers and threw
    /// all but one away, after decoding each.
    ///
    /// So: one donor at a time per log, with [`STATE_TRANSFER_RETRY`] as its
    /// deadline. Silence past the deadline, or an explicit refusal, passes the
    /// turn to the next peer that has not been asked this round — which is the
    /// T5 property the old comment defended, at one transfer instead of N.
    /// Once everybody has been asked the round restarts, no faster than the
    /// interval, so a session where nobody yet has history does not spin.
    ///
    /// Stops by itself for each log: the moment anything is delivered —
    /// adopted, replayed or locally applied — `has_no_history` goes false and
    /// this becomes one comparison per loop iteration for that log.
    fn retry_state_transfer(&mut self) {
        if self.import_log.is_none() {
            // This node cannot adopt. Nothing below is meaningful in that
            // state and it must not be carried into a later one.
            for log in self.logs.values_mut() {
                log.transfer = TransferState::default();
            }
            return;
        }
        let mut peers: Option<Vec<PeerId>> = None;
        let now = Instant::now();
        let log_ids: Vec<LogId> = self.logs.keys().cloned().collect();
        for log_id in log_ids {
            let Some(log) = self.logs.get_mut(&log_id) else {
                continue;
            };
            if !log.has_no_history() {
                // This log no longer needs a transfer.
                log.transfer = TransferState::default();
                continue;
            }
            let peers = peers.get_or_insert_with(|| {
                self.transport
                    .peers()
                    .into_iter()
                    .filter(|p| p.status == crate::transport::PeerStatus::Connected)
                    .map(|p| p.id)
                    .collect()
            });
            if peers.is_empty() {
                return;
            }

            let transfer = &mut log.transfer;
            let waited = transfer
                .last_request
                .map(|last| now.duration_since(last))
                .unwrap_or(STATE_TRANSFER_RETRY);
            if transfer.donor.is_some() {
                if waited < STATE_TRANSFER_RETRY {
                    // A request is outstanding and still within its deadline.
                    continue;
                }
                // It is not coming. `donors_tried` already holds this peer,
                // so the next choice below is somebody else.
                transfer.donor = None;
            }

            let next = match peers
                .iter()
                .find(|peer| !transfer.donors_tried.contains(peer))
            {
                Some(peer) => peer.clone(),
                None => {
                    // Everybody connected has been asked. Start again — a peer
                    // that had nothing a moment ago may have something now —
                    // but not faster than the interval, or a session in which
                    // nobody yet has history would spin on refusals.
                    if waited < STATE_TRANSFER_RETRY {
                        continue;
                    }
                    transfer.donors_tried.clear();
                    peers[0].clone()
                }
            };

            transfer.donors_tried.push(next.clone());
            transfer.donor = Some(next.clone());
            transfer.last_request = Some(now);
            self.request_state_transfer(&log_id, &next);
        }
    }

    /// Fold the newest bootnode roster into the transport's address book and
    /// dial whatever is new.
    ///
    /// Cheap when nothing arrived, which is every iteration but one per
    /// interval: the poll thread does the waiting, this only drains a channel.
    /// `connect()` skips peers already connected, so a roster that has not
    /// changed costs one `HashMap` lookup per member.
    fn reconcile_discovered_peers(&mut self) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(roster) = discovery.latest_roster() else {
            return;
        };
        let session = discovery.session().to_string();

        // Where a pair that cannot dial each other can meet. The transport
        // decides whether it needs one; a `DirectTransport` ignores this
        // entirely, which is what keeps the whole path additive.
        if let Some(relay) = &roster.relay {
            self.transport.set_relay(&session, relay);
        }

        for peer in roster.peers {
            self.transport.add_peer(peer.id, peer.addr);
        }

        // Dial on *every* roster, not only when the roster changed. A first
        // dial can fail because the peer is not listening yet, and gating the
        // retry on a membership change would reintroduce dial-once for exactly
        // the peers that need retrying. `connect_to_peers()` skips anything
        // already connected, so a settled mesh costs one map lookup per member
        // per interval, and `connect()` carries the `SyncRequest` that gives a
        // newly dialled peer our history and asks for theirs.
        self.connect();
    }

    /// Process a control command from the HTTP thread.
    fn handle_control_cmd(&mut self, cmd: ControlCmd) {
        match cmd {
            ControlCmd::Pause { peer_id, reply } => {
                let result = Self::unit_result_to_op_result(
                    self.transport.pause_peer(&peer_id),
                    format!("Paused peer '{}'", peer_id),
                    format!("Failed to pause '{}'", peer_id),
                );
                let _ = reply.send(result);
            }
            ControlCmd::Resume { peer_id, reply } => {
                let result = match self.transport.resume_peer(&peer_id) {
                    Ok(()) => {
                        let buffered = self.transport.drain_buffer(&peer_id);
                        let count = buffered.len();
                        for msg in buffered {
                            self.handle_transport_message(peer_id.clone(), msg);
                        }
                        // Request delta sync from the peer
                        self.request_sync(&peer_id);
                        OpResult {
                            success: true,
                            message: format!(
                                "Resumed peer '{}', delivered {} buffered msgs, requested sync",
                                peer_id, count
                            ),
                        }
                    }
                    Err(e) => OpResult {
                        success: false,
                        message: format!("Failed to resume '{}': {}", peer_id, e),
                    },
                };
                let _ = reply.send(result);
            }
            ControlCmd::PauseAll { reply } => {
                let result = Self::unit_result_to_op_result(
                    self.transport.pause_all(),
                    "All peers paused".to_string(),
                    "Failed".to_string(),
                );
                let _ = reply.send(result);
            }
            ControlCmd::ResumeAll { reply } => {
                // Collect paused peer IDs first
                let paused: Vec<String> = self
                    .transport
                    .peers()
                    .into_iter()
                    .filter(|p| p.status == crate::transport::PeerStatus::Paused)
                    .map(|p| p.id)
                    .collect();
                let _ = self.transport.resume_all();
                // Drain buffers and sync for each
                let mut total_buffered = 0;
                for peer_id in &paused {
                    let buffered = self.transport.drain_buffer(peer_id);
                    total_buffered += buffered.len();
                    for msg in buffered {
                        self.handle_transport_message(peer_id.clone(), msg);
                    }
                    self.request_sync(peer_id);
                }
                let _ = reply.send(OpResult {
                    success: true,
                    message: format!(
                        "Resumed {} peers, delivered {} buffered msgs",
                        paused.len(),
                        total_buffered
                    ),
                });
            }
            ControlCmd::Peers { reply } => {
                let peers: Vec<serde_json::Value> = self
                    .transport
                    .peers()
                    .into_iter()
                    .map(|p| {
                        json!({
                            "id": p.id,
                            "status": format!("{:?}", p.status),
                            "buffered": self.transport.buffered_count(&p.id),
                        })
                    })
                    .collect();
                let _ = reply.send(json!({ "peers": peers }));
            }
            ControlCmd::Query { reply } => {
                let serialized = match (&self.query_fn, self.logs.get(&self.default_log)) {
                    (Some(f), Some(log)) => f(&log.replica),
                    _ => json!({ "error": "state query not enabled — implement QueryableLog" }),
                };
                let _ = reply.send(serialized);
            }
            ControlCmd::Operations { reply } => {
                // Serialize all logged operations of the default log
                let operations: Vec<serde_json::Value> = self
                    .logs
                    .get(&self.default_log)
                    .into_iter()
                    .flat_map(|log| log.operation_log.iter())
                    .filter_map(|op| serde_json::to_value(op).ok())
                    .collect();

                let _ = reply.send(json!({
                    "operations": operations,
                    "count": operations.len()
                }));
            }
            ControlCmd::Metrics { reply } => {
                let _ = reply.send(self.metrics());
            }
            ControlCmd::Models { reply } => {
                let _ = reply.send(self.models());
            }
            ControlCmd::Register {
                model_id,
                metamodel_id,
                reply,
            } => {
                let _ = reply.send(self.register(model_id, metamodel_id));
            }
            ControlCmd::QueryLog { log_id, reply } => {
                let state = self.logs.get(&log_id).map(|log| match &self.query_fn {
                    Some(f) => f(&log.replica),
                    None => json!({ "error": "state query not enabled — implement QueryableLog" }),
                });
                let _ = reply.send(state);
            }
            ControlCmd::LogMetrics { log_id, reply } => {
                let metrics = self.logs.get(&log_id).map(|log| self.log_metrics(log));
                let _ = reply.send(metrics);
            }
            ControlCmd::Binding { log_id, reply } => {
                let _ = reply.send(self.bound_descriptor(&log_id));
            }
            ControlCmd::AddMetamodel { text, reply } => {
                let _ = reply.send(self.add_metamodel(text));
            }
            ControlCmd::Hosts { log_id, reply } => {
                let _ = reply.send(self.logs.contains_key(&log_id));
            }
            ControlCmd::Leave { reply } => {
                // A replica has no shutdown path — `run()` never returns and
                // the process is killed from outside — so departure cannot be
                // announced on the way out. This is the announcement: stop
                // re-registering and tell the directory to drop us, while the
                // replica keeps running and keeps answering its peers.
                //
                // It is deliberately *only* a directory departure. Every peer
                // still has this replica in its matrix clock, so causal
                // stability keeps waiting for it exactly as it would for a
                // crash. Making a leave advance stability is phase 2.
                let result = match &self.discovery {
                    Some(discovery) => {
                        discovery.leave();
                        self.discovery = None;
                        OpResult {
                            success: true,
                            message: "deregistered from the bootnode session".to_string(),
                        }
                    }
                    None => OpResult {
                        success: false,
                        message: "discovery is not enabled; nothing to leave".to_string(),
                    },
                };
                let _ = reply.send(result);
            }
        }
    }

    /// Everything an observer needs to plot causal stability over time: the
    /// default log's counters ([`log_metrics`]) beside the node's.
    ///
    /// Field notes, because the names are easy to misread:
    ///
    /// - `stable_prefix` only advances when *every* known replica has
    ///   acknowledged. One silent member freezes it, and that freeze is the
    ///   phenomenon the phase-1 rig exists to measure.
    /// - `retained_ops` is the replication buffer, pruned only as
    ///   `stable_prefix` advances. It is the growth curve, and the closest
    ///   observable proxy for PO-Log length — the Arachne-generated composite
    ///   logs do not expose their unstable length through `IsLog`.
    /// - `ops_applied` counts operations *originated* here. Use it, never
    ///   `/api/operations`, which double-counts remote deliveries.
    /// - `routes` says how each peer is reached — `direct`, `relayed`, or
    ///   `unreachable`. Additive: it is `{}` for a transport with one way of
    ///   reaching a peer, and no other field changed to make room for it.
    /// - `hosted_logs` and `frames_not_hosted` are the node's, not any log's:
    ///   how many logs it hosts, and how many frames it filtered for carrying
    ///   an id it hosts nothing under. Additive, like `routes`.
    ///
    /// [`log_metrics`]: GenericNode::log_metrics
    fn metrics(&self) -> serde_json::Value {
        let mut metrics = self
            .logs
            .get(&self.default_log)
            .map(|log| self.log_metrics(log))
            .unwrap_or_else(|| json!({ "replica_id": self.replica_id }));
        let peers = self.transport.peers();
        let routes: serde_json::Map<String, serde_json::Value> = self
            .transport
            .routes()
            .into_iter()
            .map(|(peer, route)| (peer, json!(route)))
            .collect();
        if let Some(fields) = metrics.as_object_mut() {
            fields.insert(
                "peer_count".to_string(),
                json!(peers
                    .iter()
                    .filter(|p| p.status == crate::transport::PeerStatus::Connected)
                    .count()),
            );
            fields.insert("peers_known".to_string(), json!(peers.len()));
            fields.insert("routes".to_string(), json!(routes));
            fields.insert("hosted_logs".to_string(), json!(self.logs.len()));
            fields.insert(
                "frames_not_hosted".to_string(),
                json!(self.frames_not_hosted),
            );
        }
        metrics
    }

    /// One hosted log's counters. `foreign_log_refusals` is the per-log
    /// counter that must stay at zero once frames route by id — see
    /// `frames_not_hosted` on the node for the one that is expected to move.
    fn log_metrics(&self, log: &HostedLog<L>) -> serde_json::Value {
        let stability = log.replica.stability();
        let stable_version: serde_json::Map<String, serde_json::Value> = stability
            .stable_version
            .iter()
            .map(|(id, seq)| (id.clone(), json!(seq)))
            .collect();
        json!({
            "replica_id": self.replica_id,
            "log_id": log.replica.log_id().as_str(),
            "stable_prefix": stability.stable_prefix,
            "stable_version": stable_version,
            "delivered_ops": stability.delivered,
            "retained_ops": stability.retained,
            "pending_ops": stability.pending,
            "known_replicas": stability.known_replicas,
            "ops_applied": log.local_ops,
            "foreign_log_refusals": log.replica.foreign_log_refusals(),
        })
    }
}

// =============================================================================
// Convenience constructor
// =============================================================================

impl<L: IsLog> GenericNode<L, CompositeTransport<L::Op>>
where
    L::Op: NetworkOp,
{
    /// Create a node over the routing composite (convenience wrapper around
    /// [`with_transport`]).
    ///
    /// * `replica_id` — unique identifier for this replica.
    /// * `members` — all replica IDs in the cluster (including self).
    /// * `listen_port` — TCP port for peer connections.
    /// * `peer_addresses` — map of `peer_id → "host:port"` for outbound connections.
    ///
    /// [`with_transport`]: GenericNode::with_transport
    pub fn new(
        replica_id: String,
        members: &[&str],
        listen_port: u16,
        peer_addresses: HashMap<String, String>,
    ) -> Self {
        let transport = CompositeTransport::new(replica_id.clone(), listen_port, peer_addresses)
            .expect("Failed to create TCP transport");
        Self::with_transport(replica_id, members, transport)
    }

    /// [`new`], except the node hosts the log named `log_id` as its default
    /// log instead of minting a fresh one (convenience wrapper around
    /// [`with_transport_and_log_id`]).
    ///
    /// [`new`]: GenericNode::new
    /// [`with_transport_and_log_id`]: GenericNode::with_transport_and_log_id
    pub fn new_with_log_id(
        replica_id: String,
        members: &[&str],
        listen_port: u16,
        peer_addresses: HashMap<String, String>,
        log_id: LogId,
    ) -> Self {
        let transport = CompositeTransport::new(replica_id.clone(), listen_port, peer_addresses)
            .expect("Failed to create TCP transport");
        Self::with_transport_and_log_id(replica_id, members, transport, log_id)
    }
}

impl<L: IsLog + QueryableLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    /// Enable the `GET /api/state` endpoint by wiring up the query function.
    /// Call this after `new()` and before `run()`.
    pub fn enable_state_query(&mut self) {
        self.query_fn = Some(L::query_state_json);
    }
}

impl<L: IsLog + TransferableLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    /// Allow this replica to serve and to accept a state transfer.
    ///
    /// Opt-in, like [`enable_state_query`], and for the same reason: a
    /// hand-written log that is not serialisable must keep working. Without it
    /// a joiner can still catch up — but only on the events its peers have not
    /// yet compacted away, which is the phase-1 behaviour and the gap this
    /// phase closes.
    ///
    /// [`enable_state_query`]: GenericNode::enable_state_query
    pub fn enable_state_transfer(&mut self) {
        self.export_log = Some(L::export_log);
        self.import_log = Some(L::import_log);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use super::*;
    use crate::transport::{PeerInfo, TransportResult};
    use crate::workload::Rng;
    use moirai_crdt::set::ewflag_set::{EWFlagSet, EWFlagSetLog};
    use moirai_protocol::broadcast::message::EventMessage;
    use moirai_protocol::crdt::query::Read;

    type Op = EWFlagSet<String>;
    type Log = EWFlagSetLog<String>;
    type TestNode = GenericNode<Log, RecordingTransport>;

    /// Records everything sent and broadcast, and receives nothing: enough
    /// transport to feed `handle_transport_message` and inspect the node's
    /// answer, or to carry one node's frames to another by hand.
    struct RecordingTransport {
        id: PeerId,
        sent: Vec<(PeerId, TransportMessage<Op>)>,
        broadcast: Vec<TransportMessage<Op>>,
    }

    impl RecordingTransport {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                sent: Vec::new(),
                broadcast: Vec::new(),
            }
        }
    }

    impl CrdtTransport for RecordingTransport {
        type Op = Op;

        fn local_id(&self) -> &PeerId {
            &self.id
        }

        fn send(&mut self, peer: &PeerId, msg: TransportMessage<Op>) -> TransportResult<()> {
            self.sent.push((peer.clone(), msg));
            Ok(())
        }

        fn broadcast(&mut self, msg: TransportMessage<Op>) -> TransportResult<()> {
            self.broadcast.push(msg);
            Ok(())
        }

        fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Op>)>> {
            Ok(None)
        }

        fn peers(&self) -> Vec<PeerInfo> {
            Vec::new()
        }

        fn is_connected(&self, _peer: &PeerId) -> bool {
            false
        }

        fn pause_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
            Ok(())
        }

        fn resume_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
            Ok(())
        }

        fn pause_all(&mut self) -> TransportResult<()> {
            Ok(())
        }

        fn resume_all(&mut self) -> TransportResult<()> {
            Ok(())
        }

        fn buffered_count(&self, _peer: &PeerId) -> usize {
            0
        }

        fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
            Ok(Vec::new())
        }
    }

    /// The behaviour-tree model of the validation plan, `a1b2…`.
    fn bt() -> LogId {
        LogId::parse("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2").unwrap()
    }

    /// The SimpleUML model of the validation plan, `c3d4…`.
    fn uml() -> LogId {
        LogId::parse("c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d4").unwrap()
    }

    /// A node hosting `logs`, the first of them as its default log, over a
    /// recording transport, able to serve and accept a state transfer.
    fn node(id: &str, logs: &[LogId]) -> TestNode {
        let (first, rest) = logs.split_first().expect("a node hosts at least one log");
        let mut node = TestNode::with_transport_and_log_id(
            id.to_string(),
            &[id],
            RecordingTransport::new(id),
            first.clone(),
        );
        for log in rest {
            node.host_log(log.clone()).expect("a fresh id");
        }
        node.enable_state_transfer();
        node
    }

    /// The members of the set a hosted log holds, sorted.
    fn members(node: &TestNode, log_id: &LogId) -> BTreeSet<String> {
        node.hosted(log_id)
            .unwrap_or_else(|| panic!("{} is hosted", log_id))
            .query(Read::<<Log as IsLog>::Value>::new())
            .into_iter()
            .collect()
    }

    /// A bare peer hosting one log, for frames that come from outside a node.
    fn peer(id: &str, log_id: LogId) -> LogReplica<Log> {
        IsReplica::bootstrap_with_log_id(id.to_string(), &[id], log_id)
    }

    fn add(value: &str) -> Op {
        EWFlagSet::Add(value.to_string())
    }

    /// Every frame `node` broadcast since the last call, in order.
    fn take_broadcast(node: &mut TestNode) -> Vec<TransportMessage<Op>> {
        std::mem::take(&mut node.transport.broadcast)
    }

    /// The key the `bt` descriptor is served under in these tests. Any opaque
    /// string does: the node compares it with what `key_of` answers and reads
    /// nothing inside it.
    const BT_KEY: &str = "sha256:bt";

    /// The set member a header occupies in this set-valued log.
    const HEADER_PREFIX: &str = "__model:";

    fn key_of(metamodel_id: &serde_json::Value) -> Option<String> {
        metamodel_id
            .get("digest")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }

    /// The header as a set can carry one: a member naming the model and the
    /// descriptor it is bound to, written by the creator alone.
    fn header(model_id: &LogId, descriptor: &ServedDescriptor) -> Vec<Op> {
        vec![add(&format!(
            "{HEADER_PREFIX}{model_id}:{}",
            descriptor.key
        ))]
    }

    /// An intake guard in the shape the node binary installs: the header is
    /// written once, so a local operation that touches it is refused. The
    /// log and its descriptor are handed over and not needed for this rule.
    fn refuse_header_writes(_: &LogId, _: &ServedDescriptor, op: &Op) -> Result<(), String> {
        match op {
            EWFlagSet::Add(member) | EWFlagSet::Remove(member)
                if member.starts_with(HEADER_PREFIX) =>
            {
                Err(format!(
                    "`{member}` is the model header, written once at creation"
                ))
            }
            EWFlagSet::Clear => Err("clearing the log would erase the model header".into()),
            _ => Ok(()),
        }
    }

    /// A node able to register models under the `bt` descriptor, hosting a
    /// default log of its own beside them.
    fn registering(id: &str) -> TestNode {
        let mut node = node(id, &[LogId::generate()]);
        node.serve_metamodels(vec![ServedDescriptor {
            key: BT_KEY.to_string(),
            listing: json!({ "nsURI": "http://www.example.org/behaviortree", "digest": BT_KEY }),
            text: bt_descriptor_text(),
        }]);
        node.enable_registration(key_of, header);
        node
    }

    /// The `bt` descriptor's text, in the shape [`describe`] reads: the
    /// served text has to describe back to the key it is served under, or
    /// the node's own round trip through the application could not be
    /// exercised at all.
    fn bt_descriptor_text() -> String {
        json!({ "digest": BT_KEY, "nsURI": "http://www.example.org/behaviortree" }).to_string()
    }

    /// Where a log keeps the descriptor it was opened with, in this toy
    /// log: one set member with the text behind a prefix. The interpreted
    /// node keeps it in `ModelLog::descriptor`, written by the opening
    /// `Install`; what matters to the node below is only that the
    /// application can hand it back.
    const DESCRIPTOR_PREFIX: &str = "__descriptor:";

    /// An [`AdoptFn`] in miniature: the descriptor this log carries, if it
    /// carries one.
    fn carried(replica: &LogReplica<Log>) -> Option<String> {
        replica
            .query(Read::<<Log as IsLog>::Value>::new())
            .into_iter()
            .find_map(|member| {
                member
                    .strip_prefix(DESCRIPTOR_PREFIX)
                    .map(str::to_string)
            })
    }

    /// The operation that puts a descriptor into a model's own history.
    fn opened_with(text: &str) -> Op {
        add(&format!("{DESCRIPTOR_PREFIX}{text}"))
    }

    fn bt_id() -> serde_json::Value {
        json!({ "digest": BT_KEY })
    }

    /// The key a descriptor posted while the node runs is served under, in
    /// this test's toy description rule.
    const UML_KEY: &str = "sha256:uml";

    /// An application's [`DescribeFn`] in miniature: the text is a JSON
    /// object carrying its own `digest` and `nsURI`, and anything else is
    /// refused with the reason. The real one parses a descriptor; the point
    /// here is that the node calls it and reads nothing itself.
    fn describe(text: &str) -> Result<ServedDescriptor, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(text).map_err(|err| format!("not JSON: {err}"))?;
        let digest = parsed
            .get("digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "no `digest`".to_string())?;
        let ns_uri = parsed
            .get("nsURI")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(ServedDescriptor {
            key: digest.to_string(),
            listing: json!({ "nsURI": ns_uri, "digest": digest }),
            text: text.to_string(),
        })
    }

    fn uml_descriptor_text() -> String {
        json!({ "digest": UML_KEY, "nsURI": "http://www.example.org/simpleuml" }).to_string()
    }

    /// `ip19`, at the level below the containers: a descriptor the node did
    /// not hold at startup is served while it runs, and a model registers
    /// under it immediately — with nothing restarted and no second list.
    #[test]
    fn a_descriptor_added_while_the_node_runs_is_served_and_registered_under() {
        let mut node = registering("n");
        let uml_id = json!({ "digest": UML_KEY });

        assert_eq!(
            node.register(None, uml_id.clone()),
            Err(RegisterRefused::UnknownMetamodel(uml_id.clone())),
            "the node holds no such descriptor yet"
        );
        assert_eq!(
            node.add_metamodel(uml_descriptor_text()),
            Err(ServeRefused::NotEnabled),
            "and it was started without the hook that could take one"
        );

        node.enable_metamodel_upload(describe);
        let served = node
            .add_metamodel(uml_descriptor_text())
            .expect("the description hook read it");
        assert_eq!(served.key, UML_KEY);
        assert!(served.added);
        assert_eq!(
            node.descriptors()
                .iter()
                .map(|held| held.key.clone())
                .collect::<Vec<_>>(),
            vec![BT_KEY.to_string(), UML_KEY.to_string()],
            "the descriptor the node started with is still served beside it"
        );

        let registered = node
            .register(None, uml_id)
            .expect("a model registers under the new key with nothing restarted");
        assert!(registered.created);
        assert!(node.hosts(&registered.model_id));
        // And the model the node could already host still can be.
        assert!(node.register(None, bt_id()).is_ok());
    }

    #[test]
    fn the_same_descriptor_posted_twice_changes_nothing() {
        let mut node = registering("n");
        node.enable_metamodel_upload(describe);

        assert!(node.add_metamodel(uml_descriptor_text()).unwrap().added);
        let again = node.add_metamodel(uml_descriptor_text()).unwrap();
        assert!(!again.added, "the second post added a second copy");
        assert_eq!(node.descriptors().len(), 2);
    }

    #[test]
    fn a_body_the_application_cannot_describe_is_refused_with_its_reason() {
        let mut node = registering("n");
        node.enable_metamodel_upload(describe);

        match node.add_metamodel("not a descriptor at all".to_string()) {
            Err(ServeRefused::Unreadable(why)) => assert!(why.contains("not JSON"), "{why}"),
            other => panic!("{other:?}"),
        }
        match node.add_metamodel(json!({ "nsURI": "u" }).to_string()) {
            Err(ServeRefused::Unreadable(why)) => assert!(why.contains("digest"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            node.descriptors().len(),
            1,
            "a refused body left the list as it was"
        );
    }

    #[test]
    fn a_state_request_for_a_foreign_log_is_refused_naming_both_ids() {
        let ours = LogId::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let theirs = LogId::parse("ffffffffffffffffffffffffffffffff").unwrap();
        let mut node = GenericNode::<Log, RecordingTransport>::with_transport_and_log_id(
            "a".to_string(),
            &["a"],
            RecordingTransport::new("a"),
            ours.clone(),
        );
        assert_eq!(node.log_id(), &ours, "the node hosts the log it was given");

        node.handle_transport_message(
            "b".to_string(),
            TransportMessage::StateRequest {
                id: "b".to_string(),
                log_id: theirs.clone(),
            },
        );

        let (to, answer) = node.transport.sent.pop().expect("the request was answered");
        assert_eq!(to, "b");
        match answer {
            TransportMessage::StateUnavailable { reason, log_id } => {
                assert_eq!(log_id, ours, "the refusal is stamped with the donor's log");
                // Both ids, because the reason line is the only symptom an
                // operator gets and either id alone is ungreppable on the
                // other side.
                assert!(
                    reason.contains(ours.as_str()) && reason.contains(theirs.as_str()),
                    "the refusal must name both logs, got: {reason}"
                );
            }
            other => panic!("expected StateUnavailable, got {other:?}"),
        }
    }

    /// M-E2's member-table assertion, at the two points the validation plan
    /// reads it: a node hosting sixteen and sixty-four logs holds one
    /// `str_to_int` map and one `Translator`, the ones inside the single
    /// `Interner` every log's handle points at, and not one per log.
    #[cfg(feature = "test_utils")]
    #[test]
    fn at_sixteen_and_sixty_four_hosted_logs_the_node_holds_one_member_table() {
        for n in [16usize, 64] {
            let logs: Vec<LogId> = (0..n).map(|i| LogId::from_bytes([i as u8; 16])).collect();
            let node = node("n", &logs);
            assert_eq!(node.hosted_logs().count(), n);

            assert_eq!(
                node.member_tables(),
                1,
                "at {n} hosted logs the node holds more than one member table"
            );
        }
    }

    #[test]
    fn mp1_dispatch_hands_a_frame_to_the_log_whose_id_matches() {
        let mut node = node("n", &[bt(), uml()]);
        let mut editor = peer("editor", uml());
        // The state and what was delivered into it — not the whole stability
        // snapshot, whose member list is the node's and grows when any
        // hosted log hears from a peer.
        let bt_before = (
            members(&node, &bt()),
            node.hosted(&bt()).unwrap().stability().delivered,
        );

        let event = editor.send(add("Class")).expect("an enabled operation");
        node.handle_transport_message("editor".to_string(), TransportMessage::Event { event });

        assert_eq!(
            members(&node, &uml()),
            BTreeSet::from(["Class".to_string()]),
            "the Class did not land in the UML model"
        );
        assert_eq!(
            members(&node, &bt()),
            BTreeSet::new(),
            "the Class landed in the behaviour tree"
        );
        assert_eq!(
            (
                members(&node, &bt()),
                node.hosted(&bt()).unwrap().stability().delivered
            ),
            bt_before,
            "the behaviour tree changed"
        );
        assert_eq!(node.hosted(&bt()).unwrap().foreign_log_refusals(), 0);
        assert_eq!(node.hosted(&uml()).unwrap().foreign_log_refusals(), 0);
        assert_eq!(node.frames_not_hosted(), 0);
    }

    #[test]
    fn mp2_a_frame_for_an_unhosted_log_is_filtered_and_counted() {
        let mut node = node("n", &[bt()]);
        let bt_before = node.hosted(&bt()).unwrap().stability();
        let frames = frames_for_an_unhosted_log();
        let expected = frames.len() as u64;

        for frame in frames {
            node.handle_transport_message("writer".to_string(), frame);
        }

        assert!(!node.hosts(&uml()), "a frame made the node host a log");
        assert_eq!(
            node.hosted(&bt()).unwrap().stability(),
            bt_before,
            "the behaviour tree changed"
        );
        assert_eq!(members(&node, &bt()), BTreeSet::new());
        assert_eq!(
            node.frames_not_hosted(),
            expected,
            "the filter did not count once per frame"
        );
        assert_eq!(node.hosted(&bt()).unwrap().foreign_log_refusals(), 0);
    }

    /// The filter counts and now also remembers. The id is in hand when the
    /// frame is dropped, and a second editor wanting to join that model would
    /// otherwise have to be told it by hand.
    #[test]
    fn a_frame_for_an_unhosted_log_leaves_its_id_behind_and_hosting_takes_it_back() {
        let mut node = node("n", &[bt()]);
        assert_eq!(
            node.seen_not_hosted().count(),
            0,
            "a node that has seen nothing listed something"
        );
        assert_eq!(
            node.models()["seen"],
            json!([]),
            "the empty case is not an empty list"
        );

        for frame in frames_for_an_unhosted_log() {
            node.handle_transport_message("writer".to_string(), frame);
        }

        assert_eq!(
            node.seen_not_hosted().cloned().collect::<Vec<_>>(),
            vec![uml()],
            "the filter dropped the id with the frame"
        );
        assert_eq!(node.models()["seen"], json!([uml().as_str()]));
        assert!(!node.hosts(&uml()), "a frame made the node host a log");

        node.host_log(uml()).expect("the node did not host it yet");

        assert_eq!(
            node.seen_not_hosted().count(),
            0,
            "a model the node now hosts is still offered as one it could join"
        );
        assert_eq!(node.models()["seen"], json!([]));
        let hosted: Vec<String> = node
            .hosted_logs()
            .map(|id| id.as_str().to_string())
            .collect();
        assert!(
            hosted.contains(&uml().as_str().to_string()),
            "the id left the seen list without joining the hosted one"
        );
    }

    /// The cap is a ceiling on a long-lived process, not a working limit: a
    /// session has a handful of models. Once full, a further new id is
    /// dropped and the frame is still counted.
    #[test]
    fn the_seen_list_stops_growing_at_the_cap_and_the_counter_does_not() {
        let mut node = node("n", &[bt()]);
        let wanted = SEEN_NOT_HOSTED_CAP + 8;
        for i in 0..wanted {
            let id = LogId::parse(&format!("{:032x}", i + 1)).expect("32 hex characters");
            let mut writer = peer("writer", id);
            let event = writer.send(add("Class")).expect("an enabled operation");
            node.handle_transport_message("writer".to_string(), TransportMessage::Event { event });
        }

        assert_eq!(
            node.seen_not_hosted().count(),
            SEEN_NOT_HOSTED_CAP,
            "the seen list grew past its cap"
        );
        assert_eq!(
            node.frames_not_hosted(),
            wanted as u64,
            "the counter stopped counting when the seen list filled"
        );
    }

    #[test]
    fn mp16_a_state_request_for_an_unhosted_log_is_answered_unavailable_naming_the_id() {
        let mut node = node("donor", &[bt(), uml()]);
        node.apply_op(add("Sequence"));
        node.apply_op_to(&uml(), add("Class"));
        let third = LogId::parse("e5f6e5f6e5f6e5f6e5f6e5f6e5f6e5f6").unwrap();
        let before = (
            node.hosted(&bt()).unwrap().stability(),
            node.hosted(&uml()).unwrap().stability(),
        );

        node.handle_transport_message(
            "joiner".to_string(),
            TransportMessage::StateRequest {
                id: "joiner".to_string(),
                log_id: third.clone(),
            },
        );

        let (to, answer) = node.transport.sent.pop().expect("the request was answered");
        assert_eq!(to, "joiner");
        match answer {
            TransportMessage::StateUnavailable { reason, .. } => assert!(
                reason.contains(third.as_str()),
                "the refusal must name the id asked for, got: {reason}"
            ),
            other => panic!("a snapshot was served for a log the donor does not have: {other:?}"),
        }
        assert_eq!(node.frames_not_hosted(), 1);
        assert_eq!(
            (
                node.hosted(&bt()).unwrap().stability(),
                node.hosted(&uml()).unwrap().stability()
            ),
            before,
            "a hosted model changed"
        );
    }

    #[test]
    fn mp17_held_back_frames_for_one_model_do_not_stall_the_other() {
        let mut a = node("a", &[bt(), uml()]);
        let mut b = node("b", &[bt(), uml()]);
        a.apply_op(add("Sequence"));
        a.apply_op_to(&uml(), add("Class"));
        a.apply_op(add("Fallback"));
        a.apply_op_to(&uml(), add("Property"));
        let (held, flowing): (Vec<_>, Vec<_>) = take_broadcast(&mut a).into_iter().partition(
            |frame| matches!(frame, TransportMessage::Event { event } if *event.log_id() == bt()),
        );
        assert_eq!((held.len(), flowing.len()), (2, 2));

        for frame in flowing {
            b.handle_transport_message("a".to_string(), frame);
        }
        assert_eq!(
            members(&b, &uml()),
            members(&a, &uml()),
            "the UML model waited on frames for a model nobody is touching"
        );
        assert_eq!(
            members(&b, &bt()),
            BTreeSet::new(),
            "held frames were delivered"
        );

        for frame in held {
            b.handle_transport_message("a".to_string(), frame);
        }
        assert_eq!(members(&b, &bt()), members(&a, &bt()));
        assert_eq!(members(&b, &uml()), members(&a, &uml()));
        for log in [bt(), uml()] {
            assert_eq!(b.hosted(&log).unwrap().foreign_log_refusals(), 0);
        }
    }

    /// Two nodes, four models, a seeded workload tagged with a model, and
    /// frames delivered in an interleaved order drawn from the seed.
    ///
    /// The single-model reference runs are bare peers that host one model
    /// each and see only that model's frames: what a node hosting the four
    /// ends with must equal what a node hosting only one would have.
    #[test]
    fn mp14_two_nodes_four_models_converge_per_model_with_no_cross_talk() {
        let seed = std::env::var("MP14_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x5eed_0000_0000_0014u64);
        let mut rng = Rng::new(seed);
        let models = [
            bt(),
            LogId::parse("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b3").unwrap(),
            uml(),
            LogId::parse("c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d5").unwrap(),
        ];
        let vocabulary = [
            ["Sequence", "Fallback", "Action", "Condition"],
            ["Sequence", "Fallback", "Action", "Condition"],
            ["Class", "Property", "Operation", "Package"],
            ["Class", "Property", "Operation", "Package"],
        ];
        let mut nodes = [node("a", &models), node("b", &models)];
        let mut references: Vec<[LogReplica<Log>; 2]> = models
            .iter()
            .map(|log| [peer("a", log.clone()), peer("b", log.clone())])
            .collect();

        // Rounds. Within a round every step is applied on one node and on
        // that node's bare twin for the same model with nothing delivered,
        // so both runs give each operation the same causal past — enable
        // wins between concurrent operations and not between sequential
        // ones, so the two runs must agree on which is which. At the end of
        // the round everything pending is delivered: to the nodes in an
        // interleaving drawn from the seed, in which frames of different
        // models cross and a model's own frames arrive out of order, and to
        // the twins in order. Both are then caught up, and the next round
        // starts from equal states.
        for _round in 0..4 {
            let mut frames: Vec<(usize, TransportMessage<Op>)> = Vec::new();
            let mut reference_frames: Vec<(usize, usize, EventMessage<Op>)> = Vec::new();
            for _ in 0..10 {
                let model = rng.below(models.len());
                let writer = rng.below(2);
                let word = vocabulary[model][rng.below(4)];
                let op = if rng.below(4) == 0 {
                    EWFlagSet::Remove(word.to_string())
                } else {
                    add(word)
                };
                let result = nodes[writer].apply_op_to(&models[model], op.clone());
                assert!(result.success, "seed {seed}: {}", result.message);
                let event = references[model][writer]
                    .send(op)
                    .expect("the reference accepts what the node accepted");
                reference_frames.push((model, 1 - writer, event));
                for frame in take_broadcast(&mut nodes[writer]) {
                    frames.push((1 - writer, frame));
                }
            }

            for i in (1..frames.len()).rev() {
                frames.swap(i, rng.below(i + 1));
            }
            for (receiver, frame) in frames {
                let from = if receiver == 0 { "b" } else { "a" };
                nodes[receiver].handle_transport_message(from.to_string(), frame);
            }
            for (model, receiver, event) in reference_frames {
                references[model][receiver].receive(event);
            }
        }

        for (model, log) in models.iter().enumerate() {
            let on_a = members(&nodes[0], log);
            let on_b = members(&nodes[1], log);
            let expected: BTreeSet<String> = references[model][0]
                .query(Read::<<Log as IsLog>::Value>::new())
                .into_iter()
                .collect();
            assert_eq!(
                on_a, on_b,
                "seed {seed}: model {log} differs between the nodes"
            );
            assert_eq!(
                on_a, expected,
                "seed {seed}: model {log} differs from its single-model run"
            );
            for node in &nodes {
                assert_eq!(
                    node.hosted(log).unwrap().foreign_log_refusals(),
                    0,
                    "seed {seed}: a frame was handed to a log that does not own it"
                );
            }
        }
        for node in &nodes {
            assert_eq!(node.frames_not_hosted(), 0, "seed {seed}");
        }
    }

    /// mp2's frames: one of every id-carrying variant, all stamped with a log
    /// the node under test does not host.
    /// The creator registers the model and writes into it; the joiner
    /// registers the same id and writes nothing; one transfer, driven by hand,
    /// carries the header across.
    #[test]
    fn mp18_the_header_written_by_the_creator_arrives_intact_at_a_joiner() {
        let mut creator = registering("creator");
        let mut joiner = registering("joiner");
        let Registered { model_id, created } = creator
            .register(None, bt_id())
            .expect("the creator holds the bt descriptor");
        assert!(created);
        creator.apply_op_to(&model_id, add("Sequence"));

        let joined = joiner
            .register(Some(model_id.clone()), bt_id())
            .expect("the joiner holds the bt descriptor");
        assert!(!joined.created);
        assert_eq!(
            joiner.hosted(&model_id).unwrap().stability().delivered,
            0,
            "a joiner wrote something at registration"
        );

        creator.handle_transport_message(
            "joiner".to_string(),
            TransportMessage::StateRequest {
                id: "joiner".to_string(),
                log_id: model_id.clone(),
            },
        );
        let (to, response) = creator
            .transport
            .sent
            .pop()
            .expect("the request was answered");
        assert_eq!(to, "joiner");
        assert!(
            matches!(response, TransportMessage::StateResponse { .. }),
            "the creator did not serve its log: {response:?}"
        );
        joiner.handle_transport_message("creator".to_string(), response);

        let creators = members(&creator, &model_id);
        assert_eq!(members(&joiner, &model_id), creators);
        let headers: Vec<&String> = creators
            .iter()
            .filter(|member| member.starts_with(HEADER_PREFIX))
            .collect();
        assert_eq!(
            headers,
            vec![&format!("{HEADER_PREFIX}{model_id}:{BT_KEY}")],
            "one header, the creator's, naming the model and its metamodel"
        );
        let joiners_log = &joiner.logs[&model_id];
        assert_eq!(
            joiners_log.local_ops, 0,
            "the joiner originated an operation"
        );
        assert!(
            !joiners_log.operation_log.iter().any(
                |op| matches!(op, EWFlagSet::Add(member) if member.starts_with(HEADER_PREFIX))
            ),
            "the joiner's own operation log holds a header write"
        );
    }

    /// The guard runs on the adapter intake of a registered log and nowhere
    /// else: the opening operations went through, the default log stays as
    /// it was.
    #[test]
    fn the_intake_guard_refuses_on_a_registered_log_and_not_on_the_default_log() {
        let mut node = registering("n");
        node.enable_op_guard(refuse_header_writes);
        let Registered { model_id, .. } = node.register(None, bt_id()).expect("created");
        let before = members(&node, &model_id);
        assert_eq!(before.len(), 1, "the header was not written at creation");

        let refused = node.submit_op(
            Some(model_id.clone()),
            add(&format!("{HEADER_PREFIX}rewritten")),
        );
        assert!(!refused.success, "{}", refused.message);
        assert_eq!(members(&node, &model_id), before, "the header moved");
        assert!(
            node.submit_op(Some(model_id.clone()), add("Sequence"))
                .success,
            "an ordinary operation on the registered log was refused"
        );
        let default_log = node.log_id().clone();
        assert!(
            node.submit_op(None, add(&format!("{HEADER_PREFIX}unbound")))
                .success,
            "the default log is unbound and must stay unguarded"
        );
        assert_eq!(members(&node, &default_log).len(), 1);
    }

    /// **MP33**, the Moirai half — the guard is consulted for a registered
    /// log and for nothing else: not the default log, and not a log hosted
    /// with no binding. The generated crate holds the other half, over HTTP
    /// with the real descriptor.
    #[test]
    fn mp33_the_default_log_and_a_log_with_no_binding_are_never_checked() {
        static CALLS: Mutex<Vec<LogId>> = Mutex::new(Vec::new());
        fn counting(log_id: &LogId, descriptor: &ServedDescriptor, _: &Op) -> Result<(), String> {
            assert_eq!(
                descriptor.key, BT_KEY,
                "the guard was handed a descriptor the log was not registered under"
            );
            CALLS.lock().unwrap().push(log_id.clone());
            Ok(())
        }
        let mut node = registering("n");
        node.host_log(uml()).expect("a fresh id");
        node.enable_op_guard(counting);
        let Registered { model_id, .. } = node.register(None, bt_id()).expect("created");

        assert!(
            node.submit_op(Some(model_id.clone()), add("Sequence"))
                .success
        );
        assert!(node.submit_op(None, add("anything")).success);
        assert!(node.submit_op(Some(uml()), add("Class")).success);
        assert_eq!(
            *CALLS.lock().unwrap(),
            vec![model_id],
            "the guard ran for the default log or for the unbound log"
        );
    }

    /// **MP35** — the guard runs at the intake only, and two operations that
    /// each pass it survive on both nodes. `a` submits one malformed
    /// operation and a `Sequence` named `patrol`, `b` a `Fallback` named
    /// `patrol`; every frame is delivered both ways by hand. The guard saw
    /// exactly the three submissions and no received frame, the refused
    /// operation left no frame on the transport, and both `patrol` members
    /// are on both nodes with the two states equal.
    #[test]
    fn mp35_the_guard_runs_at_the_intake_only_and_two_conforming_writes_both_survive() {
        static CALLS: Mutex<Vec<String>> = Mutex::new(Vec::new());
        fn refuse_malformed(_: &LogId, _: &ServedDescriptor, op: &Op) -> Result<(), String> {
            let member = match op {
                EWFlagSet::Add(member) | EWFlagSet::Remove(member) => member.as_str(),
                EWFlagSet::Clear => "",
            };
            CALLS.lock().unwrap().push(member.to_string());
            if member.starts_with("malformed:") {
                Err(format!("`{member}` is malformed"))
            } else {
                Ok(())
            }
        }
        let mut a = registering("a");
        let mut b = registering("b");
        a.enable_op_guard(refuse_malformed);
        b.enable_op_guard(refuse_malformed);
        let Registered { model_id, .. } = a.register(None, bt_id()).expect("created");
        b.register(Some(model_id.clone()), bt_id()).expect("joined");
        for frame in take_broadcast(&mut a) {
            b.handle_transport_message("a".to_string(), frame);
        }
        assert_eq!(
            members(&a, &model_id),
            members(&b, &model_id),
            "the header did not arrive"
        );

        let refused = a.submit_op(
            Some(model_id.clone()),
            add("malformed:Class under BehaviorTree.child"),
        );
        assert!(!refused.success, "the malformed operation was applied");
        assert!(
            take_broadcast(&mut a).is_empty(),
            "a frame was recorded for the refused operation"
        );
        assert!(
            a.submit_op(Some(model_id.clone()), add("Sequence:patrol"))
                .success
        );
        assert!(
            b.submit_op(Some(model_id.clone()), add("Fallback:patrol"))
                .success
        );
        for frame in take_broadcast(&mut a) {
            b.handle_transport_message("a".to_string(), frame);
        }
        for frame in take_broadcast(&mut b) {
            a.handle_transport_message("b".to_string(), frame);
        }

        assert_eq!(
            *CALLS.lock().unwrap(),
            vec![
                "malformed:Class under BehaviorTree.child".to_string(),
                "Sequence:patrol".to_string(),
                "Fallback:patrol".to_string(),
            ],
            "the guard ran for something other than the three submissions"
        );
        let on_a = members(&a, &model_id);
        assert_eq!(on_a, members(&b, &model_id), "the two nodes disagree");
        assert!(on_a.contains("Sequence:patrol") && on_a.contains("Fallback:patrol"));
        assert!(!on_a.iter().any(|member| member.starts_with("malformed:")));
        assert_eq!(
            a.hosted(&model_id).unwrap().stability().delivered,
            b.hosted(&model_id).unwrap().stability().delivered,
        );
    }

    fn frames_for_an_unhosted_log() -> Vec<TransportMessage<Op>> {
        let mut writer = peer("writer", uml());
        let reader = peer("reader", uml());
        writer.send(add("Class")).expect("an enabled operation");
        let event = writer.send(add("Property")).expect("an enabled operation");
        let batch = writer.pull(reader.since());
        vec![
            TransportMessage::Event { event },
            TransportMessage::Batch { batch },
            TransportMessage::SyncRequest {
                since: reader.since(),
            },
            TransportMessage::StateRequest {
                id: "reader".to_string(),
                log_id: uml(),
            },
            TransportMessage::StateResponse {
                snapshot: writer.snapshot(),
                log: LogPayload::Plain(serde_json::Value::Null),
                log_id: uml(),
            },
        ]
    }

    /// A sibling that has received a frame it cannot yet deliver holds no
    /// history by the `delivered` count and still cannot survive the member
    /// table being rebuilt under it: `Tcsb::follow_shared_interner` re-seats
    /// a clock only while its inbox and outbox are empty. Found by M-E3's
    /// first run, where two of five replicas died at that assertion while
    /// joining sixteen models. The node refuses the transfer for the
    /// sibling's sake, asks for a delta sync instead, and the held frame
    /// still delivers once its predecessor arrives.
    #[test]
    fn a_reordering_transfer_is_refused_while_a_sibling_holds_an_undelivered_frame() {
        // `n` joined both models empty; its table orders itself first.
        let mut n = node("n", &[bt(), uml()]);
        // A writer of the UML model `n` has never met: its first event is
        // withheld, its second arrives and waits in the inbox.
        let mut writer = peer("writer", uml());
        let first = writer.send(add("Class")).expect("an enabled operation");
        let second = writer.send(add("Property")).expect("an enabled operation");
        n.handle_transport_message(
            "writer".to_string(),
            TransportMessage::Event { event: second },
        );
        let waiting = n.hosted(&uml()).unwrap().stability();
        assert_eq!(
            (waiting.delivered, waiting.pending),
            (0, 1),
            "the second event should wait for the first"
        );

        // A donor of the behaviour tree with history, ordering [donor]: it
        // disagrees with `n`'s at index 0, so adopting would rebuild the table.
        let mut donor = node("donor", &[bt()]);
        donor.apply_op(add("Sequence"));
        donor.handle_transport_message(
            "n".to_string(),
            TransportMessage::StateRequest {
                id: "n".to_string(),
                log_id: bt(),
            },
        );
        let (_, response) = donor
            .transport
            .sent
            .pop()
            .expect("the request was answered");
        assert!(matches!(response, TransportMessage::StateResponse { .. }));
        n.handle_transport_message("donor".to_string(), response);

        // The held frame's predecessor arrives. Under a table rebuilt by the
        // adoption this is where the UML log dies (`follow_shared_interner`'s
        // assertion), which is the symptom the rig showed.
        n.handle_transport_message(
            "writer".to_string(),
            TransportMessage::Event { event: first },
        );

        assert_eq!(
            members(&n, &bt()),
            BTreeSet::new(),
            "the snapshot was adopted over a sibling holding an undelivered frame"
        );
        assert!(
            n.transport.sent.iter().any(|(to, msg)| to == "donor"
                && matches!(msg, TransportMessage::SyncRequest { since } if *since.log_id() == bt())),
            "the refusal did not fall back to a delta sync: {:?}",
            n.transport.sent
        );
        assert_eq!(
            members(&n, &uml()),
            BTreeSet::from(["Class".to_string(), "Property".to_string()]),
            "the held frame and its predecessor did not both deliver"
        );
        assert_eq!(n.hosted(&uml()).unwrap().stability().delivered, 2);
    }

    /// **IP33** — a binding a node happens to hold is a claim about the
    /// model, and the model's own history is what checks it.
    ///
    /// Found in the browser, rehearsing the demo. `a` holds two languages
    /// and creates a model in `uml`. `b` holds only `bt`, and joins that
    /// model naming `bt`, because `bt` is what `b`'s dropdown could offer.
    /// Nothing refuses it and nothing can: the log is empty at that moment,
    /// so there is nothing to check the key against. The binding went
    /// straight to `Held` — no pending state, no adoption, no comparison —
    /// and `GET /api/model/{id}/metamodel` then answered `bt` for a `uml`
    /// model, which is what an editor types the document against, and it
    /// reports the model's own classes as unknown.
    ///
    /// So: once `a`'s history lands, `b` must stop answering for that model.
    /// The control is the model beside it, joined under the same key and
    /// genuinely written in it, whose route must go on answering exactly as
    /// it did — the fix is a check, not a refusal to serve.
    ///
    /// Both assertions are made through the real event loop and the real
    /// route, because the route's answer is the defect.
    #[test]
    fn ip33_a_held_binding_the_model_contradicts_stops_serving_its_descriptor() {
        use crate::http_api::testing::{free_port, request};

        let mut a = registering("a");
        a.enable_metamodel_upload(describe);
        a.add_metamodel(uml_descriptor_text()).expect("uml is read");

        let mut b = registering("b");
        b.enable_metamodel_upload(describe);
        b.enable_descriptor_adoption(carried);

        // `a` opens one model in each language, each carrying the
        // descriptor it was opened with, as an `Install` does.
        let Registered { model_id: wrong, .. } = a
            .register(None, json!({ "digest": UML_KEY }))
            .expect("a holds uml");
        assert!(a.apply_op_to(&wrong, opened_with(&uml_descriptor_text())).success);
        let Registered { model_id: right, .. } = a.register(None, bt_id()).expect("a holds bt");
        assert!(a.apply_op_to(&right, opened_with(&bt_descriptor_text())).success);

        // `b` joins both naming `bt`, the one language it holds. The second
        // is true, the first is not, and nothing here can tell them apart.
        b.register(Some(wrong.clone()), bt_id()).expect("joined");
        b.register(Some(right.clone()), bt_id()).expect("joined");
        for log_id in [&wrong, &right] {
            assert_eq!(
                b.bound_descriptor(log_id),
                Some(BoundDescriptor::Key(BT_KEY.to_string())),
                "a join under a key this node holds binds straight to it, unchecked"
            );
        }

        for frame in take_broadcast(&mut a) {
            b.handle_transport_message("a".to_string(), frame);
        }

        let port = free_port();
        b.start_http(port);
        std::thread::spawn(move || b.run());

        // The loop resolves on its first pass; poll so the test does not
        // race the thread that owns the node now.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (mut status, mut body) = (0u16, String::new());
        while std::time::Instant::now() < deadline {
            (status, body) = request(port, &format!("GET /api/model/{wrong}/metamodel"), None);
            if status == 404 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            status, 404,
            "the node went on serving a descriptor for a model it cannot say the language of: \
             {body}"
        );

        let (status, body) = request(port, &format!("GET /api/model/{right}/metamodel"), None);
        assert_eq!(status, 200, "the model that really is a bt model: {body}");
        assert_eq!(
            body, bt_descriptor_text(),
            "the control model stopped being served, so this is a refusal and not a check"
        );

        // The language `b` could not read is served by `b` now, listed
        // beside the one it started with: a wrong binding is a fact about
        // one model, not about the descriptor it turned out to carry.
        let (status, body) = request(port, "GET /api/metamodels", None);
        assert_eq!(status, 200);
        assert!(
            body.contains(UML_KEY) && body.contains(BT_KEY),
            "b should serve both descriptors by now: {body}"
        );

        // And the model is still hosted and still converging: the binding is
        // wrong, the log is not.
        let (status, _) = request(port, &format!("GET /api/model/{wrong}/state"), None);
        assert_eq!(status, 200, "the mis-bound model stopped reading out");
    }

    /// mp2's setup, read back through the real HTTP adapter and the real
    /// event loop: the per-log counter on the model route, the node's two on
    /// the unscoped one.
    #[test]
    fn mp4_the_metrics_expose_the_filter_and_the_refusal_counters() {
        use crate::http_api::testing::{free_port, request};

        let mut node = node("n", &[bt()]);
        let frames = frames_for_an_unhosted_log();
        let filtered = frames.len() as u64;
        for frame in frames {
            node.handle_transport_message("writer".to_string(), frame);
        }
        let port = free_port();
        node.start_http(port);
        std::thread::spawn(move || node.run());
        let parse =
            |body: &str| serde_json::from_str::<serde_json::Value>(body).expect("json body");

        let (status, body) = request(port, &format!("GET /api/model/{}/metrics", bt()), None);
        let per_log = parse(&body);
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            per_log["foreign_log_refusals"].as_u64(),
            Some(0),
            "{per_log}"
        );
        assert_eq!(per_log["log_id"].as_str(), Some(bt().as_str()));

        let (status, body) = request(port, "GET /api/metrics", None);
        let node_wide = parse(&body);
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            node_wide["frames_not_hosted"].as_u64(),
            Some(filtered),
            "{node_wide}"
        );
        assert_eq!(node_wide["hosted_logs"].as_u64(), Some(1), "{node_wide}");
        assert_eq!(
            node_wide["foreign_log_refusals"].as_u64(),
            Some(0),
            "{node_wide}"
        );

        let (status, body) = request(port, &format!("GET /api/model/{}/metrics", uml()), None);
        assert_eq!(
            status, 404,
            "metrics were served for a log the node does not host: {body}"
        );
    }
}
