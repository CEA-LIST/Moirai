use crate::protocol::event::{tag::Tag, tagged_op::TaggedOp};

pub type RedundancyRelation<O> = fn(
    _old_op: &O,
    _old_event_id: Option<&Tag>,
    is_conc: bool,
    new_tagged_op: &TaggedOp<O>,
) -> bool;
