//! Rebase migration (SI.14): one model put under a revision of its
//! descriptor in place, keeping its id and every object's identity.
//!
//! # What a replica does with a rebase
//!
//! [`crate::op::ModelOp::Rebase`] carries the revision's text. A replica that
//! keeps it ([`crate::log`] says which one it keeps of several) maps the
//! model it holds to the revision and serves that from then on, and keeps
//! the model under the old descriptor beside it until every member has
//! delivered the rebase, for the operations written concurrently under the
//! old descriptor: each of them is applied to the old model, as it was
//! written, and translated into the revision.
//!
//! The result is defined as the state at the rebase's version mapped to the
//! revision, then every other delivered operation translated. A replica
//! that delivered some of those operations before the rebase cannot take
//! them back out of its tree, and it does not need to: mapping a state and
//! translating its operations commute ([`Revision`] keeps every event id,
//! Lamport stamp, sequence graph and tag as it is, and translates an
//! operation by its path and its own causal past alone), so mapping the
//! tree held is the base at the rebase's version with the operations
//! delivered before it translated.
//!
//! # The translation, by name
//!
//! An operation names features and classes by slot, and a revision moves
//! slots. Both the tree and an operation are relabelled by name:
//!
//! - a class or a feature both descriptors have keeps what it holds under
//!   its slot in the revision;
//! - a text the revision makes a fair or a last-writer-wins register becomes
//!   one write: the text's greatest event's translation in the tree
//!   ([`crate::leaf::LeafLog::text_to_register`]), and for an operation the text read at
//!   its own version with it applied, tagged with its own id and stamp;
//! - a class or a feature the revision removes is hidden: nothing of it is
//!   served, an entry of a sequence inserted with an object of a removed
//!   class stays in the sequence's order and holds nothing
//!   ([`crate::node::SeqNode`]'s hidden entries), and an operation whose path
//!   reaches one is applied as far as the last object above it, which it
//!   only touches, and reported by the first removed name on its path.

use std::collections::BTreeMap;

use moirai_semantics::{ClassSlot, FeatureSlot, LeafRule, MergeRule, MetamodelSemantics};

use crate::leaf::{LeafOp, Scalar};
use crate::log::links;
use crate::node::{
    LeafSite, MapNode, Node, ObjectNode, OptNode, SeqNode, Shaped, Site, SlotNode, shaped,
};
use crate::op::{InstanceOp, MapOp, OptOp, SeqOp};

/// What one visible feature of a class of the old descriptor becomes.
#[derive(Clone, Copy, Debug)]
struct Feature {
    /// Its visible slot on the same class in the revision.
    slot: FeatureSlot,
    /// Its shape under the old descriptor.
    shape: Shaped,
    /// The register rule a text becomes, when the revision makes it one.
    register: Option<LeafRule>,
}

/// The translation from one descriptor to a revision of it, by name.
pub(crate) struct Revision<'a> {
    from: &'a MetamodelSemantics,
    /// The revision's slot of each class of `from`; `None` once removed.
    classes: Vec<Option<ClassSlot>>,
    /// Per class of `from`, per visible slot: what the feature becomes;
    /// `None` once removed, or for a feature no node is minted for.
    features: Vec<Vec<Option<Feature>>>,
}

/// What an operation written under the old descriptor becomes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Translated {
    /// The operation under the revision, and the first removed name on the
    /// path when it was cut there.
    Op(InstanceOp, Option<String>),
    /// Nothing at all: its path names a removed class at the root, or it is
    /// a link whose path reaches a removed name, which touches nothing.
    Nothing(String),
    /// It writes a text the revision makes a register, and the write is the
    /// text as it reads at the operation's version: ask again with it.
    NeedsText,
}

impl<'a> Revision<'a> {
    /// The translation from `from` to `to`.
    pub(crate) fn new(from: &'a MetamodelSemantics, to: &'a MetamodelSemantics) -> Self {
        let class_of = |name: &str| {
            to.classes
                .iter()
                .find(|class| &*class.name == name)
                .map(|class| class.slot)
        };
        let classes: Vec<Option<ClassSlot>> = from
            .classes
            .iter()
            .map(|class| class_of(&class.name))
            .collect();
        let features = from
            .classes
            .iter()
            .zip(&classes)
            .map(|(class, revised)| {
                class
                    .visible
                    .iter()
                    .map(|(name, owner, declared)| {
                        let revised = &to.classes[(*revised)?.index()];
                        let (index, (_, to_owner, to_declared)) = revised
                            .visible
                            .iter()
                            .enumerate()
                            .find(|(_, (visible, _, _))| visible == name)?;
                        let rule = from.rule(*owner, *declared)?;
                        let to_rule = to.rule(*to_owner, *to_declared)?;
                        let register = match (rule, to_rule) {
                            (
                                MergeRule::Attribute {
                                    leaf: LeafRule::Text,
                                    ..
                                },
                                MergeRule::Attribute {
                                    leaf: leaf @ LeafRule::Register { .. },
                                    ..
                                },
                            ) => Some(*leaf),
                            _ => None,
                        };
                        Some(Feature {
                            slot: FeatureSlot(index as u16),
                            shape: shaped(rule).ok()?,
                            register,
                        })
                    })
                    .collect()
            })
            .collect();
        Revision {
            from,
            classes,
            features,
        }
    }

    fn feature(&self, class: ClassSlot, slot: FeatureSlot) -> Option<Feature> {
        *self.features.get(class.index())?.get(slot.index())?
    }

    fn class_name(&self, class: ClassSlot) -> String {
        self.from.classes.get(class.index()).map_or_else(
            || format!("<class {}>", class.0),
            |class| class.name.to_string(),
        )
    }

    /// `<Class>.<feature>` of the class that declares it.
    fn feature_name(&self, class: ClassSlot, slot: FeatureSlot) -> String {
        self.from
            .classes
            .get(class.index())
            .and_then(|holder| holder.visible.get(slot.index()))
            .map_or_else(
                || format!("<feature {}>", slot.0),
                |(name, owner, _)| format!("{}.{name}", self.class_name(*owner)),
            )
    }

    // -----------------------------------------------------------------
    // The tree
    // -----------------------------------------------------------------

    /// The model `root` (an object at the roots) under the revision.
    pub(crate) fn map_root(&self, root: &Node) -> Node {
        match root {
            Node::Slot(slot) => Node::Slot(self.map_slot(slot)),
            other => other.clone(),
        }
    }

    fn map_slot(&self, slot: &SlotNode) -> SlotNode {
        let mut objects: Vec<ObjectNode> = slot
            .objects()
            .into_iter()
            .filter_map(|object| self.map_object(object))
            .collect();
        match objects.len() {
            0 => SlotNode::Unset,
            1 => SlotNode::Value(Box::new(objects.remove(0))),
            _ => SlotNode::Conflicts(objects),
        }
    }

    /// One object under the revision, with its birth, its creations and its
    /// touches, so its identity; `None` for an object of a removed class.
    fn map_object(&self, object: &ObjectNode) -> Option<ObjectNode> {
        let class = (*self.classes.get(object.class.index())?)?;
        let fields: BTreeMap<FeatureSlot, Node> = object
            .fields
            .iter()
            .filter_map(|(slot, node)| {
                let feature = self.feature(object.class, *slot)?;
                Some((feature.slot, self.map_node(node, feature)))
            })
            .collect();
        Some(ObjectNode {
            class,
            born: object.born.clone(),
            creators: object.creators.clone(),
            touches: object.touches.clone(),
            fields,
        })
    }

    /// Whether the entry `id` of a sequence of objects was inserted with an
    /// object of a removed class.
    fn hides(&self, id: &moirai_protocol::event::id::EventId, slot: &SlotNode) -> bool {
        slot.objects().into_iter().any(|object| {
            object.born == *id
                && self
                    .classes
                    .get(object.class.index())
                    .is_none_or(Option::is_none)
        })
    }

    fn map_node(&self, node: &Node, feature: Feature) -> Node {
        match (feature.shape, node) {
            (Shaped::Sequence(site), Node::Seq(seq)) => {
                let mut mapped = SeqNode {
                    positions: seq.positions.clone(),
                    children: Default::default(),
                    hidden: seq.hidden.clone(),
                };
                for (id, child) in &seq.children {
                    match (site, child) {
                        (Site::Object(_), Node::Slot(slot)) if self.hides(id, slot) => {
                            mapped.hide(id);
                        }
                        (Site::Object(_), Node::Slot(slot)) => {
                            mapped
                                .children
                                .insert(id.clone(), Node::Slot(self.map_slot(slot)));
                        }
                        _ => {
                            mapped
                                .children
                                .insert(id.clone(), self.map_leaf(child, feature.register));
                        }
                    }
                }
                Node::Seq(mapped)
            }
            (Shaped::Optional(_), Node::Opt(opt)) => {
                let child = opt.child.as_deref().and_then(|child| match child {
                    Node::Slot(slot) => match self.map_slot(slot) {
                        SlotNode::Unset => None,
                        slot => Some(Box::new(Node::Slot(slot))),
                    },
                    other => Some(Box::new(self.map_leaf(other, feature.register))),
                });
                let mut ghost: Vec<ObjectNode> = opt
                    .ghost
                    .iter()
                    .filter_map(|object| self.map_object(object))
                    .collect();
                // The ghost is kept in class order, and the revision may
                // number the classes otherwise.
                ghost.sort_by_key(|object| object.class);
                Node::Opt(OptNode { child, ghost })
            }
            (Shaped::Keyed(_), Node::Map(map)) => Node::Map(MapNode {
                children: map
                    .children
                    .iter()
                    .map(|(key, child)| {
                        let child = match child {
                            Node::Slot(slot) => Node::Slot(self.map_slot(slot)),
                            other => self.map_leaf(other, feature.register),
                        };
                        (key.clone(), child)
                    })
                    .collect(),
            }),
            (Shaped::Bare(Site::Object(_)), Node::Slot(slot)) => Node::Slot(self.map_slot(slot)),
            (_, other) => self.map_leaf(other, feature.register),
        }
    }

    fn map_leaf(&self, node: &Node, register: Option<LeafRule>) -> Node {
        match (node, register) {
            (Node::Leaf(leaf), Some(rule)) => leaf
                .text_to_register(rule)
                .map_or_else(|| node.clone(), Node::Leaf),
            _ => node.clone(),
        }
    }

    // -----------------------------------------------------------------
    // An operation
    // -----------------------------------------------------------------

    /// `op`, written under the old descriptor, under the revision. `text` is
    /// the text a write into a text the revision makes a register leaves,
    /// read at the operation's own version; without it such a write answers
    /// [`Translated::NeedsText`].
    pub(crate) fn translate(&self, op: &InstanceOp, text: Option<&str>) -> Translated {
        let mut dropped = None;
        match self.variant(op, text, &mut dropped) {
            Err(NeedsText) => Translated::NeedsText,
            Ok(None) => Translated::Nothing(dropped.unwrap_or_default()),
            Ok(Some(_)) if dropped.is_some() && links(op) => {
                Translated::Nothing(dropped.unwrap_or_default())
            }
            Ok(Some(translated)) => Translated::Op(translated, dropped),
        }
    }

    /// The first removed name on `op`'s path from the root, if any.
    pub(crate) fn first_removed(&self, op: &InstanceOp) -> Option<String> {
        match self.translate(op, Some("")) {
            Translated::Op(_, dropped) => dropped,
            Translated::Nothing(name) => Some(name),
            Translated::NeedsText => None,
        }
    }

    /// A class step; `None` when the class is removed, the step and all
    /// below it then being nothing.
    fn variant(
        &self,
        op: &InstanceOp,
        text: Option<&str>,
        dropped: &mut Option<String>,
    ) -> Result<Option<InstanceOp>, NeedsText> {
        let InstanceOp::Variant(class, inner) = op else {
            return Ok(Some(op.clone()));
        };
        match self.classes.get(class.index()).copied().flatten() {
            None => {
                *dropped = Some(self.class_name(*class));
                Ok(None)
            }
            Some(revised) => Ok(Some(InstanceOp::variant(
                revised,
                self.object(*class, inner, text, dropped)?,
            ))),
        }
    }

    /// What an object of `class` is told: a feature step, or `New`. A step
    /// into a removed feature, or one whose remainder is nothing, ends here
    /// as `New`, which touches the object and writes nothing.
    fn object(
        &self,
        class: ClassSlot,
        op: &InstanceOp,
        text: Option<&str>,
        dropped: &mut Option<String>,
    ) -> Result<InstanceOp, NeedsText> {
        let InstanceOp::Field(slot, inner) = op else {
            return Ok(op.clone());
        };
        let Some(feature) = self.feature(class, *slot) else {
            *dropped = Some(self.feature_name(class, *slot));
            return Ok(InstanceOp::New);
        };
        Ok(match self.shaped(feature, inner, text, dropped)? {
            Some(translated) => InstanceOp::field(feature.slot, translated),
            None => InstanceOp::New,
        })
    }

    /// What a feature is told, by its shape; `None` when it is nothing.
    fn shaped(
        &self,
        feature: Feature,
        op: &InstanceOp,
        text: Option<&str>,
        dropped: &mut Option<String>,
    ) -> Result<Option<InstanceOp>, NeedsText> {
        Ok(match (feature.shape, op) {
            (Shaped::Sequence(site), InstanceOp::Seq(SeqOp::Insert { pos, op })) => {
                match self.site(feature, site, op, text, dropped)? {
                    Some(inner) => Some(InstanceOp::insert(*pos, inner)),
                    // An object of a removed class: the entry is still put
                    // in the order, hidden, so that every later position
                    // means what it meant to its writer.
                    None => Some(InstanceOp::insert(*pos, InstanceOp::New)),
                }
            }
            (Shaped::Sequence(site), InstanceOp::Seq(SeqOp::Update { pos, op })) => self
                .site(feature, site, op, text, dropped)?
                .map(|inner| InstanceOp::at(*pos, inner)),
            (Shaped::Optional(site), InstanceOp::Opt(OptOp::Set(inner))) => self
                .site(feature, site, inner, text, dropped)?
                .map(InstanceOp::set),
            (Shaped::Keyed(site), InstanceOp::Map(MapOp::Update { key, op })) => self
                .site(feature, site, op, text, dropped)?
                .map(|inner| InstanceOp::entry(key.clone(), inner)),
            (Shaped::Bare(site), _) => self.site(feature, site, op, text, dropped)?,
            // A delete, an unset, a removal: no slot or rule below them.
            _ => Some(op.clone()),
        })
    }

    /// What one element of a feature is told.
    fn site(
        &self,
        feature: Feature,
        site: Site,
        op: &InstanceOp,
        text: Option<&str>,
        dropped: &mut Option<String>,
    ) -> Result<Option<InstanceOp>, NeedsText> {
        match (site, op) {
            (Site::Object(_), _) => self.variant(op, text, dropped),
            (Site::Leaf(LeafSite::Scalar(_)), InstanceOp::Leaf(leaf)) => {
                Ok(Some(match (feature.register, leaf) {
                    (
                        Some(_),
                        LeafOp::InsertChar { .. }
                        | LeafOp::DeleteChar { .. }
                        | LeafOp::DeleteRange { .. },
                    ) => InstanceOp::Leaf(LeafOp::Write(Scalar::Str(
                        text.ok_or(NeedsText)?.to_string(),
                    ))),
                    _ => op.clone(),
                }))
            }
            _ => Ok(Some(op.clone())),
        }
    }
}

/// A translation is waiting for the text the operation leaves.
#[derive(Debug)]
struct NeedsText;
