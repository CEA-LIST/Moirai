use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::event::{Event, id::EventId, lamport::Lamport, tag::Tag};

#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
pub struct TaggedOp<O> {
    op: O,
    tag: Tag,
}

impl<O> TaggedOp<O> {
    pub fn new(id: EventId, lamport: Lamport, op: O) -> Self {
        Self {
            op,
            tag: Tag::new(id, lamport),
        }
    }

    pub fn id(&self) -> &EventId {
        self.tag.id()
    }

    pub fn lamport(&self) -> &Lamport {
        self.tag.lamport()
    }

    pub fn op(&self) -> &O {
        &self.op
    }

    pub fn tag(&self) -> &Tag {
        &self.tag
    }
}

impl<O> From<&Event<O>> for TaggedOp<O>
where
    O: Clone,
{
    fn from(event: &Event<O>) -> Self {
        Self {
            op: event.op().clone(),
            tag: Tag::new(event.id().clone(), *event.lamport()),
        }
    }
}

impl<O> Display for TaggedOp<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}@{}]", self.op, self.tag.id())
    }
}
