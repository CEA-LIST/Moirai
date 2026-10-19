use std::fmt::{Display, Error, Formatter};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::event::id::ResolvedEventId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PathSegment {
    Field(String),
    ListElement(ResolvedEventId),
    MapEntry(String),
    Variant(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ObjectPath {
    root: String,
    segments: Vec<PathSegment>,
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.root)?;
        for segment in &self.segments {
            match segment {
                PathSegment::Field(name) => write!(f, "/{}[f]", name)?,
                PathSegment::ListElement(id) => write!(f, "/{}[l]", id)?,
                PathSegment::MapEntry(key) => write!(f, "/{}[m]", key)?,
                PathSegment::Variant(name) => write!(f, "/{}[v]", name)?,
            }
        }
        Ok(())
    }
}

impl ObjectPath {
    pub fn new<'a>(root: &'a str) -> Self {
        Self {
            root: String::from(root),
            segments: Vec::new(),
        }
    }

    pub fn field(mut self, name: &'static str) -> Self {
        self.segments.push(PathSegment::Field(String::from(name)));
        self
    }

    pub fn list_element(mut self, id: impl Into<ResolvedEventId>) -> Self {
        self.segments.push(PathSegment::ListElement(id.into()));
        self
    }

    pub fn map_entry(mut self, key: String) -> Self {
        self.segments.push(PathSegment::MapEntry(key));
        self
    }

    pub fn variant(mut self, name: &'static str) -> Self {
        self.segments.push(PathSegment::Variant(String::from(name)));
        self
    }

    pub fn is_prefix_of(&self, other: &Self) -> bool {
        if self.root != other.root {
            return false;
        }
        if self.segments.len() > other.segments.len() {
            return false;
        }
        self.segments
            .iter()
            .zip(&other.segments)
            .all(|(a, b)| a == b)
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }
}
