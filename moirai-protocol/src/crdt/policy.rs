use std::{cmp::Ordering, fmt::Debug};

use crate::event::tag::Tag;

pub trait Policy: Ord + Clone + Debug {
    fn compare(a: &Tag, b: &Tag) -> Ordering
    where
        Self: Sized;
}
