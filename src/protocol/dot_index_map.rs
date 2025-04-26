use std::fmt::Debug;

use bimap::BiMap;
use petgraph::graph::NodeIndex;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use crate::clocks::dot::Dot;

#[derive(Debug, Clone, PartialEq)]
pub struct DotIndexMap(pub BiMap<Dot, NodeIndex>);

impl DotIndexMap {
    pub fn new() -> Self {
        Self(BiMap::new())
    }

    pub fn contains_left(&self, dot: &Dot) -> bool {
        self.0.contains_left(dot)
    }

    pub fn insert(&mut self, dot: Dot, ni: NodeIndex) {
        self.0.insert(dot, ni);
    }

    pub fn get_by_left(&self, dot: &Dot) -> Option<&NodeIndex> {
        self.0.get_by_left(dot)
    }

    pub fn get_by_right(&self, ni: &NodeIndex) -> Option<&Dot> {
        self.0.get_by_right(ni)
    }

    pub fn remove_by_left(&mut self, dot: &Dot) -> Option<(Dot, NodeIndex)> {
        self.0.remove_by_left(dot)
    }

    pub fn remove_by_right(&mut self, ni: &NodeIndex) -> Option<(Dot, NodeIndex)> {
        self.0.remove_by_right(ni)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(feature = "serde")]
impl Serialize for DotIndexMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // turn into Vec<(Dot, usize)> so JSON comes out like:
        // [
        //   [{"view": …, "origin": 0, "counter": 1}, 42],
        //   …
        // ]
        let vec: Vec<(_, _)> = self
            .0
            .iter()
            .map(|(dot, &ni)| (dot.clone(), ni.index()))
            .collect();
        vec.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for DotIndexMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<(Dot, usize)> = Vec::deserialize(deserializer)?;
        let mut bimap = BiMap::new();
        for (dot, idx) in vec {
            bimap.insert(dot, NodeIndex::new(idx));
        }
        Ok(DotIndexMap(bimap))
    }
}
