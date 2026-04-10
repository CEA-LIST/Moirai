use std::fmt::{Display, Error, Formatter};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    event::id::EventId,
    utils::intern_str::{InternalizeOp, Interner},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum PathSegment {
    Field(&'static str),
    ListElement(EventId),
    MapEntry(String),
    Variant(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct ObjectPath {
    root: &'static str,
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
    pub fn new(root: &'static str) -> Self {
        Self {
            root,
            segments: Vec::new(),
        }
    }

    pub fn field(mut self, name: &'static str) -> Self {
        self.segments.push(PathSegment::Field(name));
        self
    }

    pub fn list_element(mut self, id: EventId) -> Self {
        self.segments.push(PathSegment::ListElement(id));
        self
    }

    pub fn map_entry(mut self, key: String) -> Self {
        self.segments.push(PathSegment::MapEntry(key));
        self
    }

    pub fn variant(mut self, name: &'static str) -> Self {
        self.segments.push(PathSegment::Variant(name));
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

    pub fn root(&self) -> &'static str {
        self.root
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }
}

impl InternalizeOp for ObjectPath {
    fn internalize(self, interner: &Interner) -> Self {
        let segments = self
            .segments
            .into_iter()
            .map(|segment| match segment {
                PathSegment::Field(name) => PathSegment::Field(name),
                PathSegment::ListElement(id) => PathSegment::ListElement(id.internalize(interner)),
                PathSegment::MapEntry(key) => PathSegment::MapEntry(key.clone()),
                PathSegment::Variant(name) => PathSegment::Variant(name),
            })
            .collect();
        ObjectPath {
            root: self.root,
            segments,
        }
    }
}
