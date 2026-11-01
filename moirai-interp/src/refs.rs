//! A reference carried by store identity (SI.05): the links one reference
//! feature of one object holds, each naming the served identity of the object
//! it was linked to.
//!
//! # The rule, and where it comes from
//!
//! The generated runtime carries a non-containment reference as an arc of a
//! `typed_graph!` CRDT (`moirai-macros/src/typed_graph.rs`), between two
//! vertices that name objects by their creation. This node is that rule over
//! the interpreted tree, written so that the two read the same:
//!
//! - a link (`AddArc`) makes a link to the same target it had seen redundant,
//!   and an unlink (`RemoveArc`) removes the links to its target it had seen
//!   and nothing concurrent with it: add-wins;
//! - a removal above the source (`DeleteSubtree` over the source's path)
//!   removes the links it had seen, which is what
//!   [`RefNode::redundant_by_parent`] does when the removal resets the
//!   subtree the source sits in;
//! - a removal of the target, or of one of its ancestors, removes the links to
//!   it that it had seen ([`RefNode::drop_targets`], driven by the log once
//!   the removal is applied), even when an update concurrent with the removal
//!   keeps the target;
//! - a link is read only while its source holds a write no removal has seen
//!   (its vertex is in the graph: [`crate::node::ObjectNode`]'s presence,
//!   read in `eval`), even when a text the removal reset keeps the source
//!   served (SI.07, P1);
//! - a link is read only while its target is in the read-out, and among the
//!   links of a single-valued reference the greatest by `FairPolicy` is read,
//!   the policy the generated reference graph is built with, so that a tie of
//!   concurrent retargets is broken the same way on both paths.
//!
//! Foreign model coordinates and library values are opaque targets: they
//! remain readable without a local target and are copied without rewriting.
//!
//! A link reaches its source without touching it: the generated runtime
//! writes an arc into the reference graph and nothing into the containment,
//! so here a link is routed to its source silently (no update of a sequence
//! position, no touch of an object on the way), and a link concurrent with
//! the source's removal keeps no object alive.

use std::collections::{BTreeMap, HashSet};

use moirai_protocol::{
    clock::version_vector::Version,
    crdt::policy::{FairPolicy, Policy},
    event::{
        id::{EventId, ResolvedEventId},
        lamport::Lamport,
        tag::Tag,
    },
};
use moirai_semantics::{ClassSlot, MergeRule, MetamodelSemantics};
use serde_json::Value;

use crate::eval::{ECLASS, ID, read_identified};
use crate::leaf::LeafOp;
use crate::node::{Node, Refusal, Target, visible};
use crate::op::{InstanceOp, MapOp, OptOp, SeqOp};

/// Recognize the foreign namespace without changing the value's bytes.
/// Malformed coordinates retain the ordinary local served-target rule.
pub(crate) fn is_foreign(target: &str) -> bool {
    let Some((coordinate, identity)) = target.split_once('#') else {
        return false;
    };
    if moirai_protocol::log_id::LogId::parse(coordinate).is_ok() {
        let Some((replica, creation)) = identity.split_once('@') else {
            return false;
        };
        if moirai_protocol::replica::validate_replica_id(replica).is_err() {
            return false;
        }
        let Some((event, step)) = creation.split_once('.') else {
            return false;
        };
        let (seq, extra) = match event.split_once(':') {
            Some((seq, extra)) => (seq, Some(extra)),
            None => (event, None),
        };
        return decimal(seq)
            && seq.parse::<usize>().is_ok()
            && decimal(step)
            && step.parse::<u32>().is_ok()
            && extra.is_none_or(|extra| decimal(extra) && extra.parse::<u32>().is_ok());
    }
    let Some((class, resource)) = coordinate.split_once(' ') else {
        return false;
    };
    let Some((prefix, class)) = class.split_once(':') else {
        return false;
    };
    let Some(uuid) = resource
        .strip_prefix("sysmllibrary:///")
        .or_else(|| resource.strip_prefix("kermllibrary:///"))
    else {
        return false;
    };
    identifier(prefix) && identifier(class) && library_uuid(uuid) && library_uuid(identity)
}

fn decimal(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

fn identifier(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn library_uuid(text: &str) -> bool {
    text.len() == 36
        && text.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

/// Whether any feature of the metamodel is a reference by identity: when
/// none is, a removal has no link to let go of and a read has no link to
/// filter, and neither walks the model for one.
pub fn has_identity_references(sem: &MetamodelSemantics) -> bool {
    sem.classes.iter().any(|class| {
        class
            .declared
            .iter()
            .any(|feature| matches!(feature.merge, MergeRule::IdentityReference { .. }))
    })
}

/// One link: the identity it names, and the event that made it.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Link {
    pub(crate) target: String,
    pub(crate) id: EventId,
    pub(crate) lamport: Lamport,
}

impl Link {
    fn key(&self) -> (&str, ResolvedEventId) {
        (self.target.as_str(), ResolvedEventId::from(&self.id))
    }

    fn tag(&self) -> Tag {
        Tag::new(self.id.clone(), self.lamport)
    }
}

/// The links of one reference feature of one object.
///
/// Kept in the order of `(target, resolved event id)`, so that two replicas
/// holding one state serialize it to the same bytes.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RefNode {
    pub(crate) links: Vec<Link>,
}

impl RefNode {
    /// `Link(target)` at one event: the links to `target` this event had seen
    /// are made redundant by it, and it is kept.
    pub(crate) fn link(&mut self, target: &str, id: &EventId, lamport: Lamport, version: &Version) {
        self.unlink(target, version);
        let link = Link {
            target: target.to_string(),
            id: id.clone(),
            lamport,
        };
        let key = link.key();
        let at = self.links.partition_point(|other| other.key() < key);
        self.links.insert(at, link);
    }

    /// `Unlink(target)` at one event: the links to `target` it had seen go,
    /// the ones concurrent with it stay.
    pub(crate) fn unlink(&mut self, target: &str, version: &Version) {
        self.links
            .retain(|link| !(link.target == target && link.id.is_predecessor_of(version)));
    }

    /// A removal above the source: the links it had seen go.
    pub(crate) fn redundant_by_parent(&mut self, version: &Version) {
        self.links
            .retain(|link| !link.id.is_predecessor_of(version));
    }

    /// A removal of the objects whose identities are `removed`: the links to
    /// them it had seen go, and do not come back even if the target survives.
    pub(crate) fn drop_targets(&mut self, removed: &HashSet<String>, version: &Version) {
        self.links
            .retain(|link| !(removed.contains(&link.target) && link.id.is_predecessor_of(version)));
    }

    /// Whether this node holds no link.
    pub fn is_default(&self) -> bool {
        self.links.is_empty()
    }

    /// The identities this reference holds, linked or not read.
    pub fn targets(&self) -> impl Iterator<Item = &str> {
        self.links.iter().map(|link| link.target.as_str())
    }

    /// What the reference reads as: a single-valued one as the identity its
    /// greatest link by `FairPolicy` names, `null` when it holds none; a
    /// many-valued one as the identities it holds, each once, in order.
    /// `served`, when given, is the set of identities the read-out holds, and
    /// a local link to anything else is not read. Foreign values are opaque.
    pub fn read(&self, many: bool, served: Option<&HashSet<String>>) -> Value {
        let readable = self.links.iter().filter(|link| {
            is_foreign(&link.target) || served.is_none_or(|served| served.contains(&link.target))
        });
        if many {
            let targets: BTreeMap<&str, ()> =
                readable.map(|link| (link.target.as_str(), ())).collect();
            Value::Array(
                targets
                    .into_keys()
                    .map(|target| Value::String(target.to_string()))
                    .collect(),
            )
        } else {
            let mut best: Option<(&Link, Tag)> = None;
            for link in readable {
                let tag = link.tag();
                let better = match &best {
                    None => true,
                    Some((_, held)) => FairPolicy::compare(held, &tag).is_lt(),
                };
                if better {
                    best = Some((link, tag));
                }
            }
            best.map_or(Value::Null, |(link, _)| Value::String(link.target.clone()))
        }
    }
}

/// SI.07, I1: refuse a local `Link` whose identity the writer's identified
/// read-out serves on an object whose class does not conform to the
/// reference's target class (the class itself or a subclass at any depth),
/// as the generated reference graph has no arc kind for it.
///
/// An identity the read-out does not serve is accepted (SI.05, ruling R3.1,
/// reading B): it may name an object a peer created and this writer has not
/// delivered yet. Only a local writer is held to this; a peer's link is
/// applied whatever the receiver serves.
pub(crate) fn check_link_class(
    sem: &MetamodelSemantics,
    root: &Node,
    op: &InstanceOp,
) -> Result<(), Refusal> {
    let Some(link) = linked(sem, None, op) else {
        return Ok(());
    };
    if is_foreign(link.identity) {
        return Ok(());
    }
    // The class the read-out offers under a class name, by the lookup the
    // refusal below makes; `None` when no class of the table is so named.
    let offered_slot = |name: &str| {
        sem.classes
            .iter()
            .find(|class| *class.name == *name)
            .map(|class| class.slot)
    };
    let conforms = |offered: Option<ClassSlot>| {
        offered.is_none_or(|offered| Target::Class(link.target).allows(sem, offered))
    };
    // A shortcut with the answer the read below gives, without building the
    // whole identified document for every link (a migration's copy writes
    // thousands of them): the read serves an identity only on an object that
    // carries it, under that object's class, so when every object of the
    // tree that carries it is of a conforming class, or none does, whatever
    // the read serves under it conforms.
    let mut carriers = Vec::new();
    root.classes_of(link.identity, &mut carriers);
    if carriers.iter().all(|slot| {
        conforms(
            sem.classes
                .get(slot.index())
                .and_then(|class| offered_slot(&class.name)),
        )
    }) {
        return Ok(());
    }
    let doc = read_identified(sem, root);
    let Some(offered) = served_class(&doc, link.identity) else {
        return Ok(());
    };
    let Some(offered) = offered_slot(offered) else {
        return Ok(());
    };
    if conforms(Some(offered)) {
        return Ok(());
    }
    let name = |slot: ClassSlot| {
        sem.classes
            .get(slot.index())
            .map_or_else(|| std::sync::Arc::from("?"), |class| class.name.clone())
    };
    Err(Refusal::LinkClass {
        class: name(link.class),
        feature: link.feature,
        identity: link.identity.to_string(),
        offered: name(offered),
        target: name(link.target),
    })
}

/// The reference a `Link` ends in, as [`linked`] finds it.
struct Linked<'a> {
    class: ClassSlot,
    feature: std::sync::Arc<str>,
    target: ClassSlot,
    identity: &'a str,
}

/// The reference by identity an operation links, and the identity it links,
/// when the operation ends in a `Link`; `class` is the class the path has
/// reached.
fn linked<'a>(
    sem: &MetamodelSemantics,
    class: Option<ClassSlot>,
    op: &'a InstanceOp,
) -> Option<Linked<'a>> {
    match op {
        InstanceOp::Variant(class, inner) => linked(sem, Some(*class), inner),
        InstanceOp::Field(slot, inner) => {
            let class = class?;
            if let InstanceOp::Leaf(LeafOp::Link(identity)) = inner.as_ref() {
                let (name, rule) = visible(sem, class, *slot)?;
                return match rule {
                    MergeRule::IdentityReference { target, .. } => Some(Linked {
                        class,
                        feature: name.clone(),
                        target: *target,
                        identity,
                    }),
                    _ => None,
                };
            }
            linked(sem, Some(class), inner)
        }
        InstanceOp::Seq(SeqOp::Insert { op, .. } | SeqOp::Update { op, .. })
        | InstanceOp::Opt(OptOp::Set(op))
        | InstanceOp::Map(MapOp::Update { op, .. }) => linked(sem, class, op),
        _ => None,
    }
}

/// The class name of the element an identified document serves under
/// `identity`, if it serves one.
fn served_class<'a>(doc: &'a Value, identity: &str) -> Option<&'a str> {
    match doc {
        Value::Object(map) => {
            if map.get(ID).and_then(Value::as_str) == Some(identity) {
                return map.get(ECLASS).and_then(Value::as_str);
            }
            map.values().find_map(|item| served_class(item, identity))
        }
        Value::Array(items) => items.iter().find_map(|item| served_class(item, identity)),
        _ => None,
    }
}
