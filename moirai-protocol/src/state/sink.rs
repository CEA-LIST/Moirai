use crate::{
    crdt::pure_crdt::PureCRDT,
    event::{Event, id::EventId},
    state::{event_graph::EventGraph, log::IsLog, po_log::POLog, unstable_state::IsUnstableState},
};
use std::fmt::{Debug, Display};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathSegment {
    Field(&'static str),
    ListElement(EventId),
    MapEntry(String),
    Variant(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath {
    root: &'static str,
    segments: Vec<PathSegment>,
}

impl std::fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.root)?;
        for segment in &self.segments {
            match segment {
                PathSegment::Field(name) => write!(f, "/{}", name)?,
                PathSegment::ListElement(id) => write!(f, "/{}", id)?,
                PathSegment::MapEntry(key) => write!(f, "/{}", key)?,
                PathSegment::Variant(name) => write!(f, "/{}", name)?,
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

    pub fn object_path(&self) -> &ObjectPath {
        &self.object_path
    }

    pub fn effect(&self) -> &SinkEffect {
        &self.effect
    }
}

impl Display for Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.object_path(), self.effect())
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
        if let Some(idx) = self
            .sinks
            .iter()
            .position(|s| s.object_path() == sink.object_path())
        {
            // TODO: not ideal...
            // println!(
            //     "Duplicate sink for path: [{} ({:?})],\n existing sinks:\n       {}",
            //     sink.object_path(),
            //     sink.effect(),
            //     self.sinks
            //         .iter()
            //         .map(|s| format!("[{} ({:?})]", s.object_path(), s.effect()))
            //         .collect::<Vec<_>>()
            //         .join("\n       ")
            // );
            self.sinks.remove(idx);
        }
        self.sinks.push(sink);
    }

    pub fn into_sinks(self) -> Vec<Sink> {
        self.sinks
    }
}

pub trait IsLogSink
where
    Self: IsLog,
{
    #[allow(unused_variables)]
    fn effect_with_sink(
        &mut self,
        event: Event<Self::Op>,
        path: ObjectPath,
        sink: &mut SinkCollector,
    ) {
        self.effect(event);
    }
}

impl<O> IsLogSink for EventGraph<O> where O: PureCRDT + Clone {}

impl<O, U> IsLogSink for POLog<O, U>
where
    O: PureCRDT + Clone,
    U: IsUnstableState<O> + Default + Debug,
{
}

impl<L: IsLogSink> IsLogSink for Box<L> {
    fn effect_with_sink(
        &mut self,
        event: Event<Self::Op>,
        path: ObjectPath,
        sink: &mut SinkCollector,
    ) {
        let inner_op = *event.op().clone();
        let inner_event = event.unfold(inner_op);
        (**self).effect_with_sink(inner_event, path, sink);
    }
}
