//! The log: the table, the header, the model, and the one number that says
//! how often a peer sent something this table could not route.
//!
//! # The table arrives inside the log (decision D2)
//!
//! [`ModelOp::Install`] is the first operation of a model's life. Its
//! `effect` parses the descriptor it carries and stores the table;
//! [`ModelLog::is_enabled`] refuses a second one and refuses every instance
//! operation until one has landed. Three consequences, and they are why this
//! design touches neither `replica.rs` nor `generic.rs`:
//!
//! - causal delivery puts `Install` before every other operation its creator
//!   wrote, so a peer catching up by delta replays it first;
//! - the table is a field of this struct and serializes with it, so a joiner
//!   by state transfer gets it without replaying anything;
//! - one log is opened with one table, so two replicas of one log cannot
//!   disagree about the schema, which is why the design's `Bind`
//!   cross-check is not built in this phase (decision D3). The one way a
//!   log changes table afterwards is its own operation, a rebase (SI.14,
//!   below), which every replica that keeps it applies the same way.
//!
//! # Two refusals, and only one of them is a refusal
//!
//! Criterion I-A9 has two halves and they pull in opposite directions. A
//! *local* operation the descriptor alone shows to be malformed is refused,
//! with a sentence naming the class or the feature, by
//! [`ModelLog::is_enabled`] — which is the interpreted form of the phase 4
//! node's `check_structure` guard, and it is why the interpreted node needs
//! no separate guard at all. A *remote* operation is never refused: it has
//! already happened somewhere, refusing it would fork the state, and the
//! only honest answers are to apply it or to count it. One that will not
//! route increments [`ModelLog::unresolved`] and changes nothing else, and
//! nothing here panics on one.
//!
//! # The cut (SI.12)
//!
//! [`ModelOp::Cut`] closes a model that was migrated to another
//! ([`crate::migrate`]). A replica that has delivered it names the successor
//! ([`ModelLog::migration`]), refuses every local operation naming it, and
//! serves, in both reads, the documents the migrating replica served when it
//! cut, which the cut carries. Operations it delivers that the cut had not
//! seen are still applied to the tree, as every received operation is, but
//! the tree is no longer read: they are in neither the source's read-out nor
//! the successor, and the status lists them by origin and sequence number.
//!
//! # Two cuts of one model (SI.13, D3)
//!
//! Two replicas can each migrate the model before either has the other's
//! cut. A replica that delivers several cuts keeps one by a rule that reads
//! the cuts alone, so every replica that delivered the same cuts keeps the
//! same one whatever the order: the greatest Lamport stamp (the sum of the
//! cut event's version, the events of the model in its causal past, itself
//! included), then the least origin replica id compared as bytes, then the
//! least sequence number. The kept cut names the successor, freezes the
//! documents and decides what is refused; the successors of every other cut
//! are listed under `superseded`. Keeping the cut that had seen the most
//! refuses the fewest edits once every replica has every event.
//!
//! # The rebase (SI.14)
//!
//! [`ModelOp::Rebase`] is the one way a log changes table: it puts the model
//! under a revision of its descriptor in place ([`crate::rebase`]), the
//! revision's text travelling inside it as `Install`'s does. Cuts and rebases
//! are ordered by the one rule above (M22): a replica keeps the greatest of
//! the migrations it delivered. A rebase kept puts the model under its
//! revision and holds the model under the old descriptor beside it until
//! every member has been heard to deliver the rebase; an instance operation
//! is then routed by the rebase it was written under, the greatest its
//! writer had delivered (R2): one written under the rebase kept is applied
//! as written, one written under the old descriptor concurrently with it is
//! applied to the old model and translated, and any other was written under
//! a migration set aside and is discarded, listed in the status. A cut that
//! outranks the rebase in force puts the model back under the old
//! descriptor, frozen at the cut's documents.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    event::Event,
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
};
use moirai_semantics::{MetamodelSemantics, from_descriptor};
use serde_json::{Value, json};

#[cfg(feature = "sink")]
use moirai_protocol::state::sink::SinkCollector;

use crate::eval::{identities, read_identified};
use crate::leaf::LeafOp;
use crate::node::{At, Ctx, Emit, Mode, Node, Refusal, Shaped, Site, Target, apply, check};
use crate::op::{InstanceOp, MapOp, ModelOp, OptOp, SeqOp};
use crate::rebase::{Revision, Translated};

/// What a model says it is.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ModelHeader {
    /// The model's own id, as the node registered it.
    pub model_id: String,
    /// The metamodel's id: the digest of the descriptor that opened this log.
    pub metamodel_id: String,
}

/// One model, merged by the table its first operation installed.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ModelLog {
    /// Serialized, not skipped: a joiner by state transfer has no `Install`
    /// to replay and this is where it gets its table.
    sem: Option<Arc<MetamodelSemantics>>,
    header: Option<ModelHeader>,
    /// The descriptor `Install` carried, verbatim, kept for the same reason
    /// the table is: a node that joined this model under a metamodel it holds
    /// no descriptor for gets the bytes here and nowhere else, and it is the
    /// bytes rather than the table that `GET /api/model/{id}/metamodel`
    /// serves and that the digest is taken over. Serialized with the table so
    /// that a joiner by state transfer has it too.
    descriptor: Option<String>,
    /// The descriptor the log was opened with, once a rebase has put the
    /// model under another; `None` while `descriptor` is still that one.
    /// What a node checks the key a model was joined under against (SI.14).
    opened: Option<String>,
    root: Node,
    unresolved: u64,
    /// The greatest sequence number delivered from each origin, by replica
    /// id. Causal delivery takes each origin's events in order, so this is
    /// the set of events this replica has delivered, and what the events a
    /// cut had not seen are counted against.
    delivered: BTreeMap<String, u64>,
    /// The cut this replica keeps, once it has delivered one: of every cut
    /// it delivered, the one [`Cut::outranks`] puts first (SI.13, D3).
    cut: Option<Cut>,
    /// The successors of every other cut this replica delivered, sorted and
    /// each once, so that replicas that delivered the same cuts list the
    /// same array whatever order they set them aside in.
    superseded: Vec<String>,
    /// Every rebase this replica delivered (SI.14), by origin and sequence
    /// number, so that replicas that delivered the same rebases hold the
    /// same list whatever order they delivered them in.
    rebases: Vec<Rebase>,
    /// The rebase whose revision `sem`, `header`, `descriptor` and `root`
    /// are under, by origin and sequence number; `None` while they are under
    /// the descriptor the model was opened with.
    in_force: Option<(String, u64)>,
    /// The model under the descriptor the rebase in force replaced, kept
    /// until every member has delivered the rebase (SI.14, M23): what the
    /// operations written under that descriptor concurrently with the rebase
    /// are applied to before they are translated, and what another rebase
    /// that outranks this one is mapped from.
    held: Option<Box<Form>>,
    /// What the rebase in force hid, dropped and discarded (M20, M22).
    report: Report,
    /// The instance operations delivered and not yet causally stable, with
    /// their causal past: a rebase delivered later lists those written
    /// concurrently with it that it drops, or that it discards. An operation
    /// stable here is in the causal past of every migration still to be
    /// delivered, so it is let go of then. Not serialized: a migration that
    /// reaches a joiner by state transfer after its join is not claimed
    /// (SI.16, L4).
    ///
    /// The cost: every instance operation is held here, a copy beside the
    /// one the broadcast layer keeps until it is stable, whether or not a
    /// migration is ever written, since one concurrent with it can still
    /// arrive. So it is as long as the unstable part of the history, and
    /// grows without bound while a member stays silent (NW.05), as that part
    /// does.
    #[cfg_attr(feature = "serde", serde(skip))]
    pending: Vec<Pending>,
    /// Per member, the greatest sequence number per origin of the events in
    /// the causal past of the events delivered from that member: the
    /// replica's matrix-clock row of the member, as its deliveries set it,
    /// which says whom a rebase still waits for (SI.14, R3).
    heard: BTreeMap<String, BTreeMap<String, u64>>,
    /// The members of the log, sorted, as the delivered events' member table
    /// names them.
    members: Vec<String>,
    /// This replica, named by its host independently of the member table order.
    #[cfg_attr(feature = "serde", serde(skip))]
    me: Option<String>,
    /// How many times the descriptor in force changed here: a node watches
    /// it to serve the model under the descriptor in force (SI.14, M21). A
    /// count of this replica's own changes, which depend on its delivery
    /// order, so not part of the state that travels: a joiner by state
    /// transfer counts the adoption as one change when the state it adopted
    /// is under a rebase's revision (SI.16).
    #[cfg_attr(feature = "serde", serde(skip))]
    generation: u64,
}

/// The model under one descriptor: the table, what the model says it is,
/// the descriptor's text, and the tree.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Form {
    sem: Arc<MetamodelSemantics>,
    header: ModelHeader,
    descriptor: String,
    root: Node,
}

/// A delivered [`ModelOp::Rebase`] (SI.14).
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Rebase {
    /// The replica that wrote it, and its sequence number there.
    origin: String,
    seq: u64,
    /// Its Lamport stamp, the sum of its version, as a cut's (M22).
    stamp: u64,
    /// Its causal past, as the greatest sequence number per origin: its
    /// version, the base it is defined by (R1).
    seen: BTreeMap<String, u64>,
    /// The rebase in force for its writer when it wrote it, which its
    /// revision is a revision of: `None` for the descriptor the model was
    /// opened with.
    under: Option<(String, u64)>,
    /// The revision's digest, its text, and its table.
    to: String,
    descriptor: String,
    sem: Arc<MetamodelSemantics>,
    /// What its writer's read served at its version that the revision's
    /// does not, as the operation carries it ([`ModelOp::Rebase`]).
    hidden: Vec<String>,
}

impl Rebase {
    fn id(&self) -> (String, u64) {
        (self.origin.clone(), self.seq)
    }

    fn rank(&self) -> (u64, &str, u64) {
        (self.stamp, &self.origin, self.seq)
    }
}

/// What the rebase in force hid, dropped and discarded, as the status lists
/// them (M20, M22): each list sorted, so that replicas that delivered the
/// same events hold the same lists whatever order they delivered them in.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Report {
    /// The identities of the objects the old read served at the rebase's
    /// version and the revision's read does not, sorted.
    hidden: Vec<String>,
    /// The operations written concurrently under the old descriptor whose
    /// path reaches a removed class or feature, by origin, sequence number
    /// and the first removed name on the path.
    dropped: Vec<(String, u64, String)>,
    /// The operations written under a migration that does not stand.
    discarded: Vec<(String, u64)>,
}

/// One delivered instance operation, as long as a migration still to be
/// delivered may have been written concurrently with it, with the rebase it
/// was written under.
#[derive(Clone, Debug)]
struct Pending {
    origin: String,
    seq: u64,
    /// The rebase it was written under ([`Rebase::under`]).
    under: Option<(String, u64)>,
    op: InstanceOp,
}

/// Which migration a replica keeps of those it delivered (M22).
enum Kept<'a> {
    Nothing,
    Cut(&'a Cut),
    Rebase(&'a Rebase),
}

/// SI.13's D3 order, over migration operations of both kinds (SI.14, M22):
/// whether `a` is kept over `b`, by the greater stamp, then the lesser
/// origin by bytes, then the lesser sequence number.
fn outranks(a: (u64, &str, u64), b: (u64, &str, u64)) -> bool {
    a.0.cmp(&b.0)
        .then_with(|| b.1.as_bytes().cmp(a.1.as_bytes()))
        .then_with(|| b.2.cmp(&a.2))
        .is_gt()
}

/// A version as the greatest sequence number per origin, by replica id.
fn seen(version: &Version) -> BTreeMap<String, u64> {
    version
        .iter()
        .filter(|(_, seq)| *seq > 0)
        .filter_map(|(idx, seq)| {
            let origin = version.resolver().resolve(idx)?;
            Some((origin.to_string(), seq as u64))
        })
        .collect()
}

/// Put `item` into the sorted `list`, once.
fn insert_sorted<T: Ord>(list: &mut Vec<T>, item: T) {
    if let Err(at) = list.binary_search(&item) {
        list.insert(at, item);
    }
}

/// Whether the event `origin@seq` is in the causal past `seen`.
fn within(seen: &BTreeMap<String, u64>, origin: &str, seq: u64) -> bool {
    seen.get(origin).is_some_and(|last| seq <= *last)
}

/// A delivered [`ModelOp::Cut`]: the successor, what the cut had seen, and
/// the two documents served from then on.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Cut {
    to: String,
    /// The cut's causal past, as the greatest sequence number per origin.
    seen: BTreeMap<String, u64>,
    /// Served by `model_node` once the model is cut (EC.03c).
    #[allow(dead_code)]
    identified: Value,
    plain: Value,
    /// The replica that wrote the cut.
    origin: String,
    /// Its sequence number at that replica.
    seq: u64,
    /// Its Lamport stamp: the sum of its version, which is the number of
    /// events of the model in its causal past, itself included.
    stamp: u64,
}

impl Cut {
    /// Whether this cut is kept over `other` (D3): the greater stamp, then
    /// the lesser origin by bytes, then the lesser sequence number.
    fn outranks(&self, other: &Cut) -> bool {
        outranks(self.rank(), other.rank())
    }

    fn rank(&self) -> (u64, &str, u64) {
        (self.stamp, &self.origin, self.seq)
    }

    /// Whether `other` is this very cut, delivered again.
    fn is(&self, other: &Cut) -> bool {
        self.origin == other.origin && self.seq == other.seq
    }
}

impl ModelLog {
    /// The table, once one is installed.
    pub fn semantics(&self) -> Option<&Arc<MetamodelSemantics>> {
        self.sem.as_ref()
    }

    /// What the model says it is, once it has been opened.
    pub fn header(&self) -> Option<&ModelHeader> {
        self.header.as_ref()
    }

    /// The descriptor in force, verbatim, once the log has been opened: the
    /// one it was opened with, or the revision of the rebase in force
    /// (SI.14).
    ///
    /// The text and not the table, because this is what a node serves on
    /// `GET /api/model/{id}/metamodel` and what its digest is taken over. It
    /// is how a replica that joined the model holding no descriptor for it
    /// comes to hold one: `moirai-network` asks the application for it
    /// through its adoption hook and adds it to the served set, having
    /// re-derived the digest from these bytes.
    pub fn descriptor(&self) -> Option<&str> {
        self.descriptor.as_deref()
    }

    /// The descriptor this log was opened with, verbatim, once it has been
    /// opened, whatever rebase has put it under since: what the key a model
    /// was joined under names, which a rebase does not contradict (SI.14).
    pub fn opened_descriptor(&self) -> Option<&str> {
        self.opened.as_deref().or(self.descriptor.as_deref())
    }

    /// The model.
    pub fn root(&self) -> &Node {
        &self.root
    }

    /// How many remote operations this table could not route.
    ///
    /// A metric and never a panic. With one table per log it stays at zero;
    /// a peer that installed a different metamodel into the same log is the
    /// only way to move it, and this is what that looks like from here.
    pub fn unresolved(&self) -> u64 {
        self.unresolved
    }

    /// The successor this replica has seen the model migrated to, once it
    /// has delivered a cut: that of the cut it keeps (D3).
    pub fn migrated_to(&self) -> Option<&str> {
        self.kept_cut().map(|cut| cut.to.as_str())
    }

    /// The successors of the cuts this replica delivered and does not keep.
    pub fn superseded(&self) -> &[String] {
        &self.superseded
    }

    /// The migration status (SI.12, SI.13, SI.14).
    ///
    /// Before any migration
    /// `{"migrated_to": null, "refused": [], "superseded": [], "strategy":
    /// null, "superseded_rebases": [], "rebase": null}`. Once a cut is kept
    /// (D3, M22): the successor of the cut kept, every event this replica
    /// has delivered that the kept cut had not seen, as `{"origin", "seq"}`,
    /// the successors of every other cut delivered, and every rebase
    /// delivered under `superseded_rebases`. Once a rebase is kept:
    /// `strategy` `"rebase"`, no successor and nothing refused, the
    /// successors of every cut delivered under `superseded`, every other
    /// rebase under `superseded_rebases`, and under `rebase` the rebase kept
    /// (`origin`, `seq`, `to` its revision's digest), what it hid, dropped
    /// and discarded, whether the old descriptor is still held, and the
    /// members not yet heard to deliver it (M17, M20, M22, M23).
    pub fn migration(&self) -> Value {
        let rebases = |except: Option<&Rebase>| -> Vec<Value> {
            let mut ids: Vec<(&str, u64)> = self
                .rebases
                .iter()
                .filter(|rebase| except.is_none_or(|kept| kept.id() != rebase.id()))
                .map(|rebase| (rebase.origin.as_str(), rebase.seq))
                .collect();
            ids.sort_unstable();
            ids.into_iter()
                .map(|(origin, seq)| json!({ "origin": origin, "seq": seq }))
                .collect()
        };
        match self.kept() {
            Kept::Nothing => json!({
                "migrated_to": Value::Null,
                "refused": [],
                "superseded": [],
                "strategy": Value::Null,
                "superseded_rebases": [],
                "rebase": Value::Null,
            }),
            Kept::Cut(cut) => {
                let refused: Vec<Value> = self
                    .delivered
                    .iter()
                    .flat_map(|(origin, last)| {
                        let seen = cut.seen.get(origin).copied().unwrap_or(0);
                        (seen + 1..=*last).map(move |seq| json!({ "origin": origin, "seq": seq }))
                    })
                    .collect();
                json!({
                    "migrated_to": cut.to,
                    "refused": refused,
                    "superseded": self.superseded,
                    "strategy": "copy",
                    "superseded_rebases": rebases(None),
                    "rebase": Value::Null,
                })
            }
            Kept::Rebase(kept) => {
                let mut superseded: Vec<&str> = self
                    .cut
                    .iter()
                    .map(|cut| cut.to.as_str())
                    .chain(self.superseded.iter().map(String::as_str))
                    .collect();
                superseded.sort_unstable();
                superseded.dedup();
                let waiting = self.waiting_for(kept);
                let dropped: Vec<Value> = self
                    .report
                    .dropped
                    .iter()
                    .map(|(origin, seq, feature)| {
                        json!({ "origin": origin, "seq": seq, "feature": feature })
                    })
                    .collect();
                let discarded: Vec<Value> = self
                    .report
                    .discarded
                    .iter()
                    .map(|(origin, seq)| json!({ "origin": origin, "seq": seq }))
                    .collect();
                json!({
                    "migrated_to": Value::Null,
                    "refused": [],
                    "superseded": superseded,
                    "strategy": "rebase",
                    "superseded_rebases": rebases(Some(kept)),
                    "rebase": {
                        "origin": kept.origin,
                        "seq": kept.seq,
                        "to": kept.to,
                        "hidden": self.report.hidden,
                        "dropped": dropped,
                        "discarded": discarded,
                        "d1_held": !waiting.is_empty(),
                        "waiting_for": waiting,
                    },
                })
            }
        }
    }

    /// How many times the descriptor in force changed (SI.14): a rebase kept
    /// puts the model under its revision, and a cut that outranks it puts
    /// the model back. A node watches this to serve the descriptor in force
    /// on the model's metamodel route and to list the model under it.
    pub fn descriptor_generation(&self) -> u64 {
        self.generation
    }

    /// Whether a rebase of the model is in its window: some member has not
    /// been heard to deliver it, and the model under the descriptor it
    /// replaced is still held (SI.14, M23).
    pub(crate) fn in_window(&self) -> bool {
        self.held.is_some()
    }

    /// The identities of the objects this replica's read serves that the
    /// read of the same model under `to`, a revision of the table in force,
    /// does not, sorted: what a rebase written here to `to` hides (M20).
    pub(crate) fn hidden_by(&self, to: &MetamodelSemantics) -> Vec<String> {
        let Some(from) = self.sem.as_deref() else {
            return Vec::new();
        };
        let root = Revision::new(from, to).map_root(&self.root);
        let mut before = HashSet::new();
        identities(&read_identified(from, &self.root), &mut before);
        let mut after = HashSet::new();
        identities(&read_identified(to, &root), &mut after);
        let mut hidden: Vec<String> = before.difference(&after).cloned().collect();
        hidden.sort_unstable();
        hidden
    }

    /// The identified read once the model is cut: what the migrating
    /// replica served when it cut.
    #[cfg(feature = "network")]
    pub(crate) fn frozen_identified(&self) -> Option<&Value> {
        self.kept_cut().map(|cut| &cut.identified)
    }

    /// The default read once the model is cut.
    pub(crate) fn frozen_plain(&self) -> Option<&Value> {
        self.kept_cut().map(|cut| &cut.plain)
    }

    /// Where the model sits: one object of whichever class the descriptor
    /// names as a root.
    pub(crate) fn site() -> Shaped {
        Shaped::Bare(Site::Object(Target::Roots))
    }

    /// Would this operation be accepted from a local writer, and why not?
    ///
    /// The whole path is resolved against the table before this answers,
    /// which is what makes it the structural check: an operation naming a
    /// feature its class does not declare, or writing a leaf with the wrong
    /// kind of operation, comes back with a sentence naming the class or the
    /// feature.
    pub fn refusal(&self, op: &ModelOp) -> Result<(), Refusal> {
        if let Some(cut) = self.kept_cut() {
            return Err(Refusal::Migrated { to: cut.to.clone() });
        }
        match op {
            ModelOp::Install { descriptor, .. } => {
                if self.sem.is_some() {
                    return Err(Refusal::AlreadyInstalled);
                }
                parse(descriptor).map(|_| ())
            }
            ModelOp::Instance(op) => {
                let sem = self.sem.as_deref().ok_or(Refusal::NoTable)?;
                check(
                    sem,
                    Some(&self.root),
                    Self::site(),
                    op,
                    &At::root(),
                    Mode::Local,
                )?;
                // SI.07, I1: a local link to an object of the wrong class.
                // A peer's link is never held to it (`effect` routes it).
                crate::refs::check_link_class(sem, &self.root, op)
            }
            // Accepted from any local writer, because the replica that sends
            // a migration's plan is one. A node's adapter intake must refuse
            // a hand-built cut before it gets here: see
            // [`crate::migrate::adapter_refusal`]. Not while a rebase is in
            // its window (SI.14).
            ModelOp::Cut { .. } => {
                self.sem.as_ref().ok_or(Refusal::NoTable)?;
                if self.in_window() {
                    return Err(Refusal::Rebasing);
                }
                Ok(())
            }
            // The same, and the revision held to what a rebase translates
            // ([`crate::migrate::rebase`]).
            ModelOp::Rebase {
                descriptor, drops, ..
            } => {
                let sem = self.sem.as_deref().ok_or(Refusal::NoTable)?;
                if self.in_window() {
                    return Err(Refusal::Rebasing);
                }
                crate::migrate::revise(sem, descriptor, drops)
                    .map(|_| ())
                    .map_err(|why| Refusal::Rebase(why.to_string()))
            }
        }
    }

    /// Take a delivered cut: keep it when it outranks the cut kept so far
    /// (D3), and list the successor of the one set aside under `superseded`.
    ///
    /// The rule reads the cuts alone, their stamps, origins and sequence
    /// numbers, which every replica reads the same, so replicas that
    /// delivered the same cuts keep the same one in any delivery order.
    fn take_cut(&mut self, event: &Event<ModelOp>, to: &str, identified: &Value, plain: &Value) {
        let version = event.version();
        let seen = seen(version);
        let id = event.id();
        let taken = Cut {
            to: to.to_string(),
            stamp: version.sum() as u64,
            seen,
            identified: identified.clone(),
            plain: plain.clone(),
            origin: id.origin_id().to_string(),
            seq: id.seq() as u64,
        };
        match self.cut.take() {
            None => self.cut = Some(taken),
            Some(kept) if kept.is(&taken) => self.cut = Some(kept),
            Some(kept) if taken.outranks(&kept) => {
                self.set_aside(kept.to);
                self.cut = Some(taken);
            }
            Some(kept) => {
                self.set_aside(taken.to);
                self.cut = Some(kept);
            }
        }
    }

    /// List the successor of a cut set aside under `superseded`, in its
    /// sorted place, once.
    fn set_aside(&mut self, successor: String) {
        if let Err(at) = self.superseded.binary_search(&successor) {
            self.superseded.insert(at, successor);
        }
    }

    /// Count `event` as delivered from its origin.
    fn note_delivered(&mut self, event: &Event<ModelOp>) {
        let id = event.id();
        let seq = id.seq() as u64;
        match self.delivered.get_mut(id.origin_id().as_ref()) {
            Some(last) => *last = (*last).max(seq),
            None => {
                self.delivered.insert(id.origin_id().to_string(), seq);
            }
        }
    }

    // -----------------------------------------------------------------
    // Rebase (SI.14)
    // -----------------------------------------------------------------

    /// The migration this replica keeps of those it delivered, cuts and
    /// rebases alike, by [`outranks`] (M22).
    fn kept(&self) -> Kept<'_> {
        let rebase = self.rebases.iter().reduce(|kept, other| {
            if outranks(other.rank(), kept.rank()) {
                other
            } else {
                kept
            }
        });
        match (&self.cut, rebase) {
            (None, None) => Kept::Nothing,
            (Some(cut), None) => Kept::Cut(cut),
            (None, Some(rebase)) => Kept::Rebase(rebase),
            (Some(cut), Some(rebase)) if outranks(cut.rank(), rebase.rank()) => Kept::Cut(cut),
            (Some(_), Some(rebase)) => Kept::Rebase(rebase),
        }
    }

    /// The cut this replica keeps, when it keeps one.
    fn kept_cut(&self) -> Option<&Cut> {
        match self.kept() {
            Kept::Cut(cut) => Some(cut),
            _ => None,
        }
    }

    /// The rebase an event of causal past `seen` was written under: the one
    /// its writer kept of the rebases it had delivered, which is the
    /// greatest of them, a cut that outranked it leaving its writer nothing
    /// to write.
    fn under(&self, seen: &BTreeMap<String, u64>) -> Option<(String, u64)> {
        self.rebases
            .iter()
            .filter(|rebase| within(seen, &rebase.origin, rebase.seq))
            .reduce(|kept, other| {
                if outranks(other.rank(), kept.rank()) {
                    other
                } else {
                    kept
                }
            })
            .map(Rebase::id)
    }

    /// Note what delivering `event` says: the members, and what the event's
    /// origin had delivered when it wrote it (R3).
    fn hear(&mut self, event: &Event<ModelOp>) {
        let version = event.version();
        let resolver = version.resolver();
        if self.members.len() != resolver.len() {
            self.members = (0..resolver.len())
                .filter_map(|idx| resolver.resolve(ReplicaIdx(idx)).map(ToString::to_string))
                .collect();
            self.members.sort_unstable();
        }
        let row = self
            .heard
            .entry(event.id().origin_id().to_string())
            .or_default();
        for (idx, seq) in version.iter().filter(|(_, seq)| *seq > 0) {
            let Some(origin) = resolver.resolve(idx) else {
                continue;
            };
            let seq = seq as u64;
            match row.get_mut(origin.as_ref()) {
                Some(last) => *last = (*last).max(seq),
                None => {
                    row.insert(origin.to_string(), seq);
                }
            }
        }
    }

    /// The members other than this replica not yet heard to deliver
    /// `rebase`: no event of theirs this replica delivered is the rebase or
    /// has it in its causal past (R3, M23). Sorted.
    fn waiting_for(&self, rebase: &Rebase) -> Vec<String> {
        self.members
            .iter()
            .filter(|member| self.me.as_ref() != Some(*member))
            .filter(|member| {
                !self
                    .heard
                    .get(*member)
                    .is_some_and(|row| within(row, &rebase.origin, rebase.seq))
            })
            .cloned()
            .collect()
    }

    /// Take a delivered rebase. One whose revision is not a table is a
    /// peer's mistake: counted, and not a migration here.
    fn take_rebase(
        &mut self,
        event: &Event<ModelOp>,
        metamodel_id: &str,
        descriptor: &str,
        hidden: &[String],
    ) {
        let id = event.id();
        let (origin, seq) = (id.origin_id().to_string(), id.seq() as u64);
        if self
            .rebases
            .iter()
            .any(|rebase| rebase.origin == origin && rebase.seq == seq)
        {
            return;
        }
        let Ok(sem) = parse(descriptor) else {
            self.unresolved += 1;
            return;
        };
        let seen = seen(event.version());
        let under = self.under(&seen);
        let at = self.rebases.partition_point(|rebase| {
            (rebase.origin.as_str(), rebase.seq) < (origin.as_str(), seq)
        });
        self.rebases.insert(
            at,
            Rebase {
                origin,
                seq,
                stamp: event.version().sum() as u64,
                seen,
                under,
                to: metamodel_id.to_string(),
                descriptor: descriptor.to_string(),
                sem: Arc::new(sem),
                hidden: hidden.to_vec(),
            },
        );
    }

    /// Put the model under the migration kept, once a migration was
    /// delivered: under the revision of a rebase kept, back under the old
    /// descriptor for a cut that outranks the rebase that was in force.
    fn settle(&mut self) {
        match self.kept() {
            Kept::Nothing => {}
            Kept::Cut(_) => {
                if self.in_force.is_some() {
                    if let Some(held) = self.held.take() {
                        self.put(*held);
                    }
                    self.in_force = None;
                    self.report = Report::default();
                    self.generation += 1;
                }
            }
            Kept::Rebase(rebase) => {
                if self.in_force.as_ref() != Some(&rebase.id()) {
                    let rebase = rebase.clone();
                    self.rebase_to(&rebase);
                }
            }
        }
    }

    /// The model, in force, under one descriptor.
    fn put(&mut self, form: Form) {
        self.sem = Some(form.sem);
        self.header = Some(form.header);
        self.descriptor = Some(form.descriptor);
        self.root = form.root;
    }

    /// The model in force, taken out.
    fn take_form(&mut self) -> Option<Form> {
        let form = Form {
            sem: self.sem.take()?,
            header: self.header.take()?,
            descriptor: self.descriptor.take()?,
            root: std::mem::take(&mut self.root),
        };
        Some(form)
    }

    /// Put the model under `rebase`'s revision (M17, M18, M20): the model
    /// under the descriptor it replaced, mapped by name, and that model held
    /// beside it. What the rebase hid, and of the operations delivered so
    /// far, those it drops and those it discards, are listed.
    fn rebase_to(&mut self, rebase: &Rebase) {
        // A rebase of the revision in force, written once its writer had
        // heard every member deliver that one (SI.14, L6): the old
        // descriptor is no longer wanted.
        if rebase.under.is_some() && rebase.under == self.in_force {
            self.held = None;
        }
        let base = match self.held.take() {
            Some(held) => *held,
            None => match self.take_form() {
                Some(form) => form,
                None => return,
            },
        };
        // The first rebase here is taken from the model as it was opened.
        if self.opened.is_none() {
            self.opened = Some(base.descriptor.clone());
        }
        let revision = Revision::new(&base.sem, &rebase.sem);
        let root = revision.map_root(&base.root);
        // Read at the rebase's version, by its writer: the old model held
        // here may already hold an operation concurrent with the rebase, a
        // removal of an object the rebase hid among them (M20).
        let mut report = Report {
            hidden: rebase.hidden.clone(),
            ..Report::default()
        };
        for pending in &self.pending {
            if pending.under == rebase.under {
                if !within(&rebase.seen, &pending.origin, pending.seq)
                    && let Some(name) = revision.first_removed(&pending.op)
                {
                    insert_sorted(
                        &mut report.dropped,
                        (pending.origin.clone(), pending.seq, name),
                    );
                }
            } else if pending.under != Some(rebase.id()) {
                insert_sorted(&mut report.discarded, (pending.origin.clone(), pending.seq));
            }
        }
        self.put(Form {
            sem: Arc::clone(&rebase.sem),
            header: ModelHeader {
                model_id: base.header.model_id.clone(),
                metamodel_id: rebase.to.clone(),
            },
            descriptor: rebase.descriptor.clone(),
            root,
        });
        self.held = Some(Box::new(base));
        self.in_force = Some(rebase.id());
        self.report = report;
        self.generation += 1;
    }

    /// Let go of the old descriptor once every member has been heard to
    /// deliver the rebase in force: no operation written under it can
    /// arrive any more (M23). Nothing but a delivery moves this.
    fn close_window(&mut self) {
        if self.held.is_none() {
            return;
        }
        if let Kept::Rebase(rebase) = self.kept()
            && self.in_force == Some(rebase.id())
            && self.waiting_for(rebase).is_empty()
        {
            self.held = None;
        }
    }

    /// Apply a delivered instance operation by the migration it was written
    /// under (R2, M22): as written when that is the migration kept, or when
    /// none was delivered; applied to the old model and translated when it
    /// was written under the descriptor the rebase kept replaced; not at all
    /// when it was written under a migration set aside.
    fn route(&mut self, event: &Event<ModelOp>, op: &InstanceOp, out: &mut Out<'_>) {
        let Some(sem) = self.sem.clone() else {
            self.unresolved += 1;
            return;
        };
        // The rebase it was written under: none while none was delivered.
        let under = if self.rebases.is_empty() {
            None
        } else {
            self.under(&seen(event.version()))
        };
        let id = event.id();
        let (origin, seq) = (id.origin_id().to_string(), id.seq() as u64);
        let (kept, kept_under) = match self.kept() {
            Kept::Nothing => (None, None),
            Kept::Cut(_) => (Some(None), None),
            Kept::Rebase(rebase) => (Some(Some(rebase.id())), rebase.under.clone()),
        };
        self.pending.push(Pending {
            origin: origin.clone(),
            seq,
            under: under.clone(),
            op: op.clone(),
        });
        let plain = match kept {
            None => true,
            // Frozen: the tree is not read, and it takes what the old
            // descriptor routes, which a rebase that outranks the cut maps.
            Some(None) => under.is_none(),
            Some(Some(kept)) if under.as_ref() == Some(&kept) => true,
            Some(Some(_)) if under == kept_under => {
                self.late(event, op, out);
                false
            }
            Some(Some(_)) => {
                insert_sorted(&mut self.report.discarded, (origin, seq));
                false
            }
        };
        if plain && !apply_instance(&sem, &mut self.root, event, op, How::Written, out.emit()) {
            self.unresolved += 1;
        }
    }

    /// An operation written under the descriptor the rebase in force
    /// replaced, delivered after the rebase: applied to the old model as
    /// written, then translated into the revision (M18), or dropped by its
    /// path (M20).
    fn late(&mut self, event: &Event<ModelOp>, op: &InstanceOp, out: &mut Out<'_>) {
        let (Some(held), Some(sem)) = (self.held.as_deref_mut(), self.sem.clone()) else {
            self.unresolved += 1;
            return;
        };
        let from = Arc::clone(&held.sem);
        let revision = Revision::new(&from, &sem);
        let mut translated = revision.translate(op, None);
        let text = RefCell::new(None);
        let how = match translated {
            Translated::NeedsText => How::Reading(&text),
            _ => How::Written,
        };
        #[cfg(feature = "sink")]
        let mut scratch = SinkCollector::new();
        let emit = Emit::new(
            #[cfg(feature = "sink")]
            out.path.clone(),
            #[cfg(feature = "sink")]
            &mut scratch,
        );
        if !apply_instance(&from, &mut held.root, event, op, how, emit) {
            self.unresolved += 1;
            return;
        }
        if matches!(translated, Translated::NeedsText) {
            translated = revision.translate(op, text.borrow().as_deref());
        }
        let id = event.id();
        let (origin, seq) = (id.origin_id().to_string(), id.seq() as u64);
        match translated {
            Translated::Nothing(name) => {
                insert_sorted(&mut self.report.dropped, (origin, seq, name))
            }
            Translated::Op(translated, dropped) => {
                if let Some(name) = dropped {
                    insert_sorted(&mut self.report.dropped, (origin, seq, name));
                }
                if !apply_instance(
                    &sem,
                    &mut self.root,
                    event,
                    &translated,
                    How::Translated,
                    out.emit(),
                ) {
                    self.unresolved += 1;
                }
            }
            Translated::NeedsText => self.unresolved += 1,
        }
    }
}

/// How [`apply_instance`] applies an operation.
#[derive(Clone, Copy)]
enum How<'a> {
    /// As written: checked first, so that one this table cannot route
    /// leaves the model exactly as it was.
    Written,
    /// As written, the text it writes reported to the cell.
    Reading(&'a RefCell<Option<String>>),
    /// Translated from the descriptor a rebase replaced: not checked, since
    /// it routed as written, and its positions count every entry.
    Translated,
}

/// Apply one instance operation of `event` to `root` under `sem`; `false`
/// when it did not route, which the caller counts.
fn apply_instance(
    sem: &MetamodelSemantics,
    root: &mut Node,
    event: &Event<ModelOp>,
    op: &InstanceOp,
    how: How<'_>,
    emit: Emit<'_>,
) -> bool {
    let removed = RefCell::new(HashSet::new());
    let mut ctx = Ctx::top(event.id(), event.lamport(), event.version())
        .silently(links(op))
        .reporting(&removed);
    match how {
        How::Written => {}
        How::Reading(text) => ctx = ctx.reading_text(text),
        How::Translated => ctx = ctx.translated(),
    }
    // Asked before anything is written, so that an operation this table
    // cannot route leaves the model exactly as it was.
    if !matches!(how, How::Translated)
        && check(
            sem,
            Some(root),
            ModelLog::site(),
            op,
            &At::root(),
            Mode::Routing,
        )
        .is_err()
    {
        return false;
    }
    let outcome = apply(
        sem,
        root,
        ModelLog::site(),
        ctx,
        op.clone(),
        &At::root(),
        emit,
    );
    // SI.05: a removal takes away the links to what it removed that it had
    // seen, wherever in the model they are held.
    let removed = removed.into_inner();
    if !removed.is_empty() && crate::refs::has_identity_references(sem) {
        root.drop_links(&removed, event.version());
    }
    outcome.is_ok()
}

/// Where the effects of one delivered operation are reported, with the
/// `sink` feature on; nothing without it.
struct Out<'s> {
    #[cfg(feature = "sink")]
    path: moirai_protocol::state::object_path::ObjectPath,
    #[cfg(feature = "sink")]
    sink: &'s mut SinkCollector,
    #[cfg(not(feature = "sink"))]
    _marker: std::marker::PhantomData<&'s ()>,
}

impl Out<'_> {
    fn emit(&mut self) -> Emit<'_> {
        Emit::new(
            #[cfg(feature = "sink")]
            self.path.clone(),
            #[cfg(feature = "sink")]
            self.sink,
        )
    }
}

/// A link target that names an object by the writer's own event: `@<seq>.<step>`,
/// a served identity with its origin left out.
///
/// A migration plans the successor's operations before anyone knows which
/// replica will send them ([`crate::migrate`]), so the links among the
/// copied objects cannot name their writer. Every served identity starts
/// with its origin's id, so a target starting with `@` names none, and is
/// read here as the identity `<origin>@<seq>.<step>` of the event's own
/// origin.
///
/// The rewrite runs on every model log, a successor or not, and is a pure
/// function of the event, so every replica reads the same target. What it
/// opens is at local intake: SI.07's check by class cannot resolve the
/// short form, so a link written in it is not held to the target's class.
/// A local writer that is not a migration's plan never needs the form, and
/// a node refuses it from its adapters ([`crate::migrate::adapter_refusal`]).
fn own_target(target: &str, origin: &str) -> Option<String> {
    let rest = target.strip_prefix('@')?;
    let (seq, step) = rest.split_once('.')?;
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    (digits(seq) && digits(step)).then(|| format!("{origin}{target}"))
}

/// Whether `op` ends in a link or an unlink to an own target
/// ([`own_target`]).
pub(crate) fn holds_own_target(op: &InstanceOp) -> bool {
    resolve_own(op, "").is_some()
}

/// `op` with every link and unlink to an own target ([`own_target`])
/// resolved against `origin`; `None` when it holds none.
fn resolve_own(op: &InstanceOp, origin: &str) -> Option<InstanceOp> {
    let boxed = |inner: &InstanceOp| resolve_own(inner, origin).map(Box::new);
    Some(match op {
        InstanceOp::Field(slot, inner) => InstanceOp::Field(*slot, boxed(inner)?),
        InstanceOp::Variant(class, inner) => InstanceOp::Variant(*class, boxed(inner)?),
        InstanceOp::Seq(SeqOp::Insert { pos, op }) => InstanceOp::Seq(SeqOp::Insert {
            pos: *pos,
            op: boxed(op)?,
        }),
        InstanceOp::Seq(SeqOp::Update { pos, op }) => InstanceOp::Seq(SeqOp::Update {
            pos: *pos,
            op: boxed(op)?,
        }),
        InstanceOp::Opt(OptOp::Set(op)) => InstanceOp::Opt(OptOp::Set(boxed(op)?)),
        InstanceOp::Map(MapOp::Update { key, op }) => InstanceOp::Map(MapOp::Update {
            key: key.clone(),
            op: boxed(op)?,
        }),
        InstanceOp::Leaf(LeafOp::Link(target)) => {
            InstanceOp::Leaf(LeafOp::Link(own_target(target, origin)?))
        }
        InstanceOp::Leaf(LeafOp::Unlink(target)) => {
            InstanceOp::Leaf(LeafOp::Unlink(own_target(target, origin)?))
        }
        _ => return None,
    })
}

/// Whether the operation ends in a link or an unlink of a reference by
/// identity: the operations that reach their source without touching it
/// (see [`crate::refs`]).
pub(crate) fn links(op: &InstanceOp) -> bool {
    match op {
        InstanceOp::Field(_, inner) | InstanceOp::Variant(_, inner) => links(inner),
        InstanceOp::Seq(SeqOp::Update { op, .. })
        | InstanceOp::Opt(OptOp::Set(op))
        | InstanceOp::Map(MapOp::Update { op, .. }) => links(op),
        InstanceOp::Leaf(LeafOp::Link(_) | LeafOp::Unlink(_)) => true,
        InstanceOp::Seq(SeqOp::Insert { .. } | SeqOp::Delete { .. })
        | InstanceOp::Opt(OptOp::Unset)
        | InstanceOp::Map(MapOp::Remove { .. } | MapOp::Clear)
        | InstanceOp::New
        | InstanceOp::Leaf(_) => false,
    }
}

/// A descriptor's text as a table, or the sentence that says why not.
fn parse(descriptor: &str) -> Result<MetamodelSemantics, Refusal> {
    let value: Value = serde_json::from_str(descriptor)
        .map_err(|err| Refusal::Descriptor(format!("it is not JSON: {err}")))?;
    from_descriptor(&value).map_err(|err| Refusal::Descriptor(err.to_string()))
}

impl IsLog for ModelLog {
    type Command = ModelOp;
    type Update = ModelOp;
    /// The sentence [`ModelLog::refusal`] already produced. Moirai v0.6 gives
    /// `IsLog` a rejection type, and this log had one before the trait did:
    /// the structural check of criterion I-A9 names the class or the feature
    /// it stopped at, and `send` now hands that to the caller instead of a
    /// bare `None`.
    type Rejection = Refusal;

    fn hosted_by(&mut self, id: &ReplicaId) {
        self.me = Some(id.to_string());
    }

    fn adopted_from(&mut self, donor: &Version) {
        self.members = donor
            .resolver()
            .into_vec()
            .iter()
            .map(ToString::to_string)
            .collect();
        self.members.sort_unstable();
        self.heard
            .insert(donor.origin_id().to_string(), seen(donor));
        self.root.adopted_from(donor);
        if let Some(held) = self.held.as_deref_mut() {
            held.root.adopted_from(donor);
        }
        self.generation = u64::from(self.in_force.is_some());
        self.close_window();
    }

    fn prepare(&self, command: Self::Command) -> Self::Update {
        command
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        self.refusal(op)
    }

    #[allow(unused_variables)]
    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        #[cfg(feature = "sink")]
        let path = ctx.path().clone();
        #[cfg(feature = "sink")]
        let mut discarded = SinkCollector::new();
        #[cfg(feature = "sink")]
        let sink = match ctx.sink_mut() {
            Some(sink) => sink,
            None => &mut discarded,
        };
        let mut out = Out {
            #[cfg(feature = "sink")]
            path,
            #[cfg(feature = "sink")]
            sink,
            #[cfg(not(feature = "sink"))]
            _marker: std::marker::PhantomData,
        };
        self.note_delivered(&event);
        self.hear(&event);
        match event.op() {
            ModelOp::Cut {
                migrated_to,
                identified,
                plain,
            } => {
                if self.sem.is_none() {
                    self.unresolved += 1;
                } else {
                    self.take_cut(&event, migrated_to, identified, plain);
                    self.settle();
                }
            }
            ModelOp::Rebase {
                metamodel_id,
                descriptor,
                hidden,
                ..
            } => {
                if self.sem.is_none() {
                    self.unresolved += 1;
                } else {
                    self.take_rebase(&event, metamodel_id, descriptor, hidden);
                    self.settle();
                }
            }
            ModelOp::Install {
                model_id,
                metamodel_id,
                descriptor,
            } => {
                // The descriptor the log was opened with, which a rebase in
                // its window holds beside the revision.
                let opened = self
                    .held
                    .as_ref()
                    .map(|held| &held.header)
                    .or(self.header.as_ref());
                if let Some(header) = opened {
                    // Two replicas opened one log at once. Identical
                    // installations are the same table twice and are simply
                    // the second one landing; a different one is a peer this
                    // log disagrees with, and that is what the counter is
                    // for.
                    if header.metamodel_id != *metamodel_id {
                        self.unresolved += 1;
                    }
                } else {
                    match parse(descriptor) {
                        Ok(sem) => {
                            self.sem = Some(Arc::new(sem));
                            self.descriptor = Some(descriptor.clone());
                            self.header = Some(ModelHeader {
                                model_id: model_id.clone(),
                                metamodel_id: metamodel_id.clone(),
                            });
                            self.root = Node::for_shaped(Self::site());
                        }
                        // A descriptor that will not parse is a peer's
                        // mistake and not this replica's crash.
                        Err(_) => self.unresolved += 1,
                    }
                }
            }
            ModelOp::Instance(op) => {
                let op = match resolve_own(op, event.id().origin_id().as_ref()) {
                    Some(resolved) => Cow::Owned(resolved),
                    None => Cow::Borrowed(op),
                };
                self.route(&event, op.as_ref(), &mut out);
            }
        }
        self.close_window();
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.root.stabilize(frontier);
        if let Some(held) = self.held.as_deref_mut() {
            held.root.stabilize(frontier);
        }
        if !self.pending.is_empty() {
            // `seen` reads the entries and the member table, not the origin.
            let stable = seen(&Version::from((frontier, ReplicaIdx(0))));
            self.pending
                .retain(|pending| !within(&stable, &pending.origin, pending.seq));
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.root.redundant_by_parent(version, conservative);
        if let Some(held) = self.held.as_deref_mut() {
            held.root.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.sem.is_none() && self.root.is_default()
    }
}

#[cfg(test)]
mod tests {
    //! `ip16` and `ip17`, the two halves of criterion I-A9, plus the rules
    //! that make decision D2 hold: one table, arriving first, for the life of
    //! the log. `ip7`'s second half is here too, where an operation crosses a
    //! real replica boundary between two interners that disagree.

    use moirai_protocol::{
        broadcast::tcsb::Tcsb,
        replica::{IsReplica, Replica},
        state::log::IsLog,
    };
    use moirai_semantics::{ClassSlot, FeatureSlot};
    use serde_json::{Value, json};

    use super::{ModelLog, ModelOp};
    use crate::leaf::{LeafOp, Scalar};
    use crate::node::Refusal;
    use crate::op::InstanceOp;
    use crate::testing::{
        self, BT_DESCRIPTOR, bt, class_slot, deliver, feature_slot, field, install, mini,
        mini_descriptor, object, opened, ordered, sequence, text_of,
    };

    fn bt_descriptor() -> Value {
        serde_json::from_str(BT_DESCRIPTOR).expect("the fixture is JSON")
    }

    /// `Variant(Root, Field(feature, inner))`, the shape every operation on a
    /// behaviour tree starts with.
    fn on_root(feature: &str, inner: InstanceOp) -> InstanceOp {
        let sem = bt();
        let root = class_slot(&sem, "Root");
        InstanceOp::variant(
            root,
            InstanceOp::field(feature_slot(&sem, root, feature), inner),
        )
    }

    fn field_of(class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        let sem = bt();
        InstanceOp::field(feature_slot(&sem, class_slot(&sem, class), feature), inner)
    }

    fn class(name: &str) -> ClassSlot {
        class_slot(&bt(), name)
    }

    /// Put a behaviour tree in `Root.behaviortrees[0]` and give it an id.
    fn add_tree(id: char) -> InstanceOp {
        on_root(
            "behaviortrees",
            InstanceOp::insert(
                0,
                InstanceOp::variant(
                    class("BehaviorTree"),
                    field_of(
                        "BehaviorTree",
                        "ID",
                        InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: id }),
                    ),
                ),
            ),
        )
    }

    // ------------------------------------------------------ decision D2

    #[test]
    fn a_log_takes_one_install_and_refuses_a_second() {
        let descriptor = mini_descriptor();
        let (mut a, _b) = opened("m1", &descriptor);
        assert_eq!(
            a.state().header().map(|header| header.model_id.as_str()),
            Some("m1")
        );
        assert!(a.state().semantics().is_some());

        assert!(a.send(install("m2", &descriptor)).is_err());
        assert_eq!(
            a.state().refusal(&install("m2", &descriptor)),
            Err(Refusal::AlreadyInstalled)
        );
    }

    #[test]
    fn every_instance_operation_waits_for_the_table() {
        let log = ModelLog::default();
        let op = ModelOp::Instance(InstanceOp::variant(ClassSlot(0), InstanceOp::New));
        assert_eq!(log.refusal(&op), Err(Refusal::NoTable));
        assert!(log.is_enabled(&op).is_err());
        assert!(log.is_default());

        // And the sentence says what to do about it.
        assert!(
            Refusal::NoTable
                .to_string()
                .contains("its first operation opens it")
        );
    }

    #[test]
    fn a_descriptor_that_will_not_parse_is_refused_locally_by_its_reason() {
        let log = ModelLog::default();
        let op = ModelOp::Install {
            model_id: "m1".to_string(),
            metamodel_id: "sha256:nothing".to_string(),
            descriptor: "{\"formatVersion\": 1}".to_string(),
        };
        match log.refusal(&op) {
            Err(Refusal::Descriptor(reason)) => {
                assert!(reason.contains("formatVersion"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        assert!(log.is_enabled(&op).is_err());
    }

    #[test]
    fn the_checked_in_fixture_is_the_descriptor_arachne_emits() {
        let descriptor = bt_descriptor();
        assert_eq!(descriptor["formatVersion"], json!(2));
        assert_eq!(descriptor["package"], json!("behaviortree"));
        assert_eq!(descriptor["classes"].as_object().unwrap().len(), 21);
        let sem = bt();
        assert_eq!(sem.classes.len(), 21);
        assert_eq!(sem.roots.len(), 1);
        assert_eq!(&*sem.classes[sem.roots[0].index()].name, "Root");
        // `TreeNode` is abstract and its concrete closure is the eight
        // subclasses `02 Validation Plan` names.
        let tree_node = class_slot(&sem, "TreeNode");
        assert_eq!(sem.classes[tree_node.index()].concrete.len(), 8);
    }

    // ----------------------------------------------------------------- ip16

    /// Every way an operation can be malformed against `bt.metamodel.json`,
    /// with the sentence each one comes back with.
    fn malformed() -> Vec<(&'static str, InstanceOp, &'static str)> {
        vec![
            (
                "a feature the class does not declare",
                InstanceOp::variant(
                    class("Root"),
                    InstanceOp::field(FeatureSlot(99), InstanceOp::New),
                ),
                "`Root` declares no feature at slot 99",
            ),
            (
                "a class the containment cannot hold",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(
                        0,
                        InstanceOp::variant(class("Blackboard"), InstanceOp::New),
                    ),
                ),
                "holds no `Blackboard`",
            ),
            (
                "a leaf write into a containment",
                on_root(
                    "behaviortrees",
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'x' }),
                ),
                "`Root.behaviortrees` is a sequence",
            ),
            (
                "a counter operation on a text attribute",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(
                        0,
                        InstanceOp::variant(
                            class("BehaviorTree"),
                            field_of(
                                "BehaviorTree",
                                "ID",
                                InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1))),
                            ),
                        ),
                    ),
                ),
                "`BehaviorTree.ID`",
            ),
            (
                "a class that is not a root, at the root",
                InstanceOp::variant(class("Blackboard"), InstanceOp::New),
                "`model.root` holds no `Blackboard`",
            ),
            (
                "an abstract class, which is in nobody's concrete closure",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(0, InstanceOp::variant(class("TreeNode"), InstanceOp::New)),
                ),
                "holds no `TreeNode`",
            ),
        ]
    }

    #[test]
    fn ip16_a_malformed_operation_is_refused_locally_with_a_reason_naming_the_feature() {
        let (mut a, mut b) = opened("m1", &bt_descriptor());

        // A model with something in it, so the refusals are not answered by
        // an empty tree.
        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();
        b.receive(event);
        let before = format!("{:?}", a.state().root());

        for (what, op, sentence) in malformed() {
            let op = ModelOp::Instance(op);
            assert!(a.send(op.clone()).is_err(), "{what} was accepted");

            let from_a = a.state().refusal(&op).unwrap_err();
            let from_b = b.state().refusal(&op).unwrap_err();
            assert_eq!(from_a, from_b, "{what}: two replicas, two verdicts");
            assert!(
                from_a.to_string().contains(sentence),
                "{what}: `{from_a}` does not name what it stopped at"
            );
        }

        assert_eq!(
            format!("{:?}", a.state().root()),
            before,
            "a refused operation changed the model"
        );
        assert_eq!(a.state().unresolved(), 0, "a local refusal is not a miss");
    }

    // ----------------------------------------------------------------- ip17

    #[test]
    fn ip17_a_remote_operation_with_no_table_is_counted_and_never_refused() {
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        // The same operation, arriving at a log that never saw the `Install`.
        // Causal delivery makes this impossible in a session; a peer that is
        // wrong must still not bring this one down.
        let mut fresh = ModelLog::default();
        deliver(&mut fresh, event.event().clone());
        assert_eq!(fresh.unresolved(), 1);
        assert!(fresh.is_default(), "nothing was applied");
    }

    #[test]
    fn ip17_a_remote_operation_this_table_cannot_route_is_counted_and_applies_nothing() {
        // Two logs, two different metamodels, one log id: the only way one
        // table meets another's operations, and what it has to survive.
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let bt_event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        let mut other = ModelLog::default();
        let install_mini = install("m1", &mini_descriptor());
        let (mut c, _d) = opened("m1", &mini_descriptor());
        let install_event = c
            .send(ModelOp::Instance(InstanceOp::variant(
                class_slot(&mini(), "Root"),
                InstanceOp::field(
                    feature_slot(&mini(), class_slot(&mini(), "Root"), "title"),
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'm' }),
                ),
            )))
            .unwrap();

        // Open `other` on the mini table by hand, then feed it both a valid
        // operation of its own and one written against `bt`.
        deliver(
            &mut other,
            moirai_protocol::event::Event::new(
                install_event.event().id().clone(),
                *install_event.event().lamport(),
                install_mini,
                install_event.event().version().clone(),
            ),
        );
        assert!(other.semantics().is_some());
        assert_eq!(other.unresolved(), 0);

        deliver(&mut other, install_event.event().clone());
        assert_eq!(other.unresolved(), 0, "its own operation routed");
        let after_its_own = format!("{:?}", other.root());

        deliver(&mut other, bt_event.event().clone());
        assert_eq!(other.unresolved(), 1, "the foreign operation was counted");
        assert_eq!(
            format!("{:?}", other.root()),
            after_its_own,
            "and it applied nothing"
        );
    }

    #[test]
    fn ip17_a_second_install_of_another_metamodel_is_counted_rather_than_applied() {
        let (a, _b) = opened("m1", &bt_descriptor());
        // `New` at the root names no class, so it is not an operation on an
        // object and the table says so before anything else does.
        let (c, _d) = opened("m1", &mini_descriptor());
        assert!(
            c.state()
                .refusal(&ModelOp::Instance(InstanceOp::New))
                .is_err()
        );

        // Take a well-formed `Install` of the other metamodel and deliver it
        // to a log that already holds one.
        let (mut e, _f) = moirai_crdt::utils::bootstrap::twins_log::<ModelLog>();
        let other_install = e.send(install("m1", &mini_descriptor())).unwrap();

        let mut log = a.state().clone();
        deliver(&mut log, other_install.event().clone());
        assert_eq!(log.unresolved(), 1);
        assert_eq!(
            log.header().map(|header| header.metamodel_id.clone()),
            a.state().header().map(|header| header.metamodel_id.clone()),
            "the table it already had is the table it kept"
        );
    }

    #[test]
    fn ip17_the_same_install_twice_is_not_a_miss() {
        let descriptor = mini_descriptor();
        let (a, _b) = opened("m1", &descriptor);
        let (c, _d) = opened("m1", &descriptor);
        let duplicate = c.state().clone();
        assert!(duplicate.semantics().is_some());

        // Two replicas opening one log at once is the only way this happens,
        // and both of them installed the same bytes.
        let (mut e, _f) = moirai_crdt::utils::bootstrap::twins_log::<ModelLog>();
        let event = e.send(install("m1", &descriptor)).unwrap();
        let mut log = a.state().clone();
        deliver(&mut log, event.event().clone());
        assert_eq!(log.unresolved(), 0);
    }

    // ------------------------------------------------------- ip7, in flight

    #[test]
    fn ip7_a_deep_operation_crosses_two_interners_that_disagree_about_their_members() {
        // The two replicas index their members in opposite orders, so every
        // event id that crosses between them is re-indexed on arrival, and a
        // recursion that skipped a level would land a list child on the wrong
        // object.
        let mut a: Replica<ModelLog, Tcsb<ModelOp>> = Replica::bootstrap("a".into(), &["a", "b"]);
        let mut b: Replica<ModelLog, Tcsb<ModelOp>> = Replica::bootstrap("b".into(), &["b", "a"]);

        let event = a.send(install("m1", &bt_descriptor())).unwrap();
        b.receive(event);

        let sem = bt();
        let a1 = a.send(ModelOp::Instance(add_tree('a'))).unwrap();
        b.receive(a1);
        let b1 = b.send(ModelOp::Instance(add_tree('b'))).unwrap();
        a.receive(b1);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(replica.state().root());
            let trees = sequence(field(&sem, root, "behaviortrees").expect("minted"));
            let names: Vec<String> = ordered(trees)
                .iter()
                .map(|tree| text_of(&sem, object(tree), "ID"))
                .collect();
            assert_eq!(names, vec!["b".to_string(), "a".to_string()], "{who}");
        }
    }

    // ------------------------------------------------------ birth identity

    #[test]
    fn an_object_is_born_of_one_event_and_two_replicas_agree_on_which() {
        // The point of `ObjectNode::born`, and the only thing that makes it
        // an identity rather than a local note. The two replicas index their
        // members in opposite orders, so the id `a` stores for the operation
        // it wrote and the id `b` stores for the same operation carry
        // different `ReplicaIdx`es. `EventId` compares by origin *name* and
        // sequence number (`event/id.rs:100-103`), which is what makes one
        // birth identity mean one object on both.
        let mut a: Replica<ModelLog, Tcsb<ModelOp>> = Replica::bootstrap("a".into(), &["a", "b"]);
        let mut b: Replica<ModelLog, Tcsb<ModelOp>> = Replica::bootstrap("b".into(), &["b", "a"]);

        let event = a.send(install("m1", &bt_descriptor())).unwrap();
        b.receive(event);

        let created = a.send(ModelOp::Instance(add_tree('t'))).unwrap();
        let written = created.event().id().clone();
        assert_eq!(written.origin_id(), "a");
        b.receive(created);

        let sem = bt();
        let born_here = |replica: &Replica<ModelLog, Tcsb<ModelOp>>| {
            let root = object(replica.state().root());
            let trees = sequence(field(&sem, root, "behaviortrees").expect("minted by the insert"));
            (
                root.born().clone(),
                object(ordered(trees)[0]).born().clone(),
            )
        };
        let (root_at_a, tree_at_a) = born_here(&a);
        let (root_at_b, tree_at_b) = born_here(&b);

        assert_eq!(
            tree_at_a, written,
            "`a` gave the object the id of the operation that created it"
        );
        assert_eq!(
            tree_at_b, tree_at_a,
            "and `b` names the same birth for the same object"
        );
        assert_eq!(root_at_b, root_at_a, "and so does the root object");

        // Two index spaces, one identity: without this the equality above
        // could be comparing two copies of one number.
        assert_ne!(
            tree_at_a.idx(),
            tree_at_b.idx(),
            "the two replicas were supposed to disagree about indices"
        );
    }

    // ------------------------------------------- the real behaviour tree

    #[test]
    fn a_behaviour_tree_converges_under_the_descriptor_arachne_emits() {
        let sem = bt();
        let (mut a, mut b) = opened("m1", &bt_descriptor());

        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();
        b.receive(event);

        // Each writer puts a different control node under the tree's `child`,
        // which is a single-valued containment of the abstract `TreeNode`.
        let put_child = |class_name: &str, id: char| {
            ModelOp::Instance(on_root(
                "behaviortrees",
                InstanceOp::at(
                    0,
                    InstanceOp::variant(
                        class("BehaviorTree"),
                        field_of(
                            "BehaviorTree",
                            "child",
                            InstanceOp::variant(
                                class(class_name),
                                field_of(
                                    "TreeNode",
                                    "ID",
                                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: id }),
                                ),
                            ),
                        ),
                    ),
                ),
            ))
        };

        let from_a = a.send(put_child("Sequence", 's')).unwrap();
        let from_b = b.send(put_child("Fallback", 'f')).unwrap();
        a.receive(from_b);
        b.receive(from_a);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(replica.state().root());
            let trees = sequence(field(&sem, root, "behaviortrees").unwrap());
            let tree = object(ordered(trees)[0]);
            assert_eq!(text_of(&sem, tree, "ID"), "t", "{who}");
            let child = field(&sem, tree, "child").expect("both writers set it");
            let mut held: Vec<String> = crate::testing::objects(child)
                .iter()
                .map(|object| sem.classes[object.class().index()].name.to_string())
                .collect();
            held.sort();
            assert_eq!(held, vec!["Fallback", "Sequence"], "{who}");
            assert_eq!(replica.state().unresolved(), 0, "{who}");
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_log_and_its_table_survive_a_round_trip_through_serde() {
        // The half of decision D2 that a joiner by state transfer rests on:
        // no `Install` is replayed, so the table has to be in the bytes.
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let _ = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        let bytes = serde_json::to_vec(a.state()).expect("a log serializes");
        let back: ModelLog = serde_json::from_slice(&bytes).expect("and comes back");

        assert_eq!(back.header(), a.state().header());
        assert_eq!(
            back.semantics().map(|sem| sem.digest.clone()),
            a.state().semantics().map(|sem| sem.digest.clone())
        );
        assert_eq!(
            format!("{:?}", back.root()),
            format!("{:?}", a.state().root())
        );

        let sem = bt();
        let root = object(back.root());
        let trees = sequence(field(&sem, root, "behaviortrees").unwrap());
        assert_eq!(text_of(&sem, object(ordered(trees)[0]), "ID"), "t");
    }

    #[test]
    fn the_fixture_module_builds_both_tables() {
        let _ = testing::bench();
        let _ = testing::bt();
    }
}
