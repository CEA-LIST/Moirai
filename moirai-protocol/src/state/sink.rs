use std::fmt::{Debug, Display};

use crate::state::object_path::ObjectPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkEffect {
    Create,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sink {
    object_path: ObjectPath,
    effect: SinkEffect,
}

impl Sink {
    pub fn new(object_path: ObjectPath, effect: SinkEffect) -> Self {
        Self {
            object_path,
            effect,
        }
    }

    pub fn create(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Create)
    }

    pub fn delete(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Delete)
    }

    pub fn update(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Update)
    }

    pub fn path(&self) -> &ObjectPath {
        &self.object_path
    }

    pub fn effect(&self) -> &SinkEffect {
        &self.effect
    }
}

impl Display for Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.path(), self.effect())
    }
}

#[derive(Debug, Clone)]
pub struct SinkCollector {
    sinks: Vec<Sink>,
}

impl Display for SinkCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut first = true;
        for sink in &self.sinks {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", sink)?;
            first = false;
        }
        write!(f, "]")
    }
}

impl Default for SinkCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkCollector {
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    pub fn collect(&mut self, sink: Sink) {
        // Ensure no duplicate sinks for the same path
        if let Some(idx) = self.sinks.iter().position(|s| s.path() == sink.path()) {
            // TODO: not ideal...
            self.sinks.remove(idx);
        }
        self.sinks.push(sink);
    }

    pub fn into_sinks(self) -> Vec<Sink> {
        self.sinks
    }
}

/// Define the interface of a log structure for CRDTs that store events.
#[cfg(feature = "sink")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkOwnership {
    Owned,
    Delegated,
}
