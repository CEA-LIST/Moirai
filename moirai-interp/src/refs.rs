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
//! - a link is read only while its target is in the read-out, and among the
//!   links of a single-valued reference the greatest by `FairPolicy` is read,
//!   the policy the generated reference graph is built with, so that a tie of
//!   concurrent retargets is broken the same way on both paths.
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
use moirai_semantics::{MergeRule, MetamodelSemantics};
use serde_json::Value;

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
    /// a link to anything else is not read.
    pub fn read(&self, many: bool, served: Option<&HashSet<String>>) -> Value {
        let readable = self
            .links
            .iter()
            .filter(|link| served.is_none_or(|served| served.contains(&link.target)));
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
