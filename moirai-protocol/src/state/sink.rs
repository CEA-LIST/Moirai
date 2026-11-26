/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

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
    kind: Option<&'static str>,
}

impl Sink {
    pub const fn new(object_path: ObjectPath, effect: SinkEffect) -> Self {
        Self::new_with_kind(object_path, effect, None)
    }

    pub const fn new_with_kind(
        object_path: ObjectPath,
        effect: SinkEffect,
        kind: Option<&'static str>,
    ) -> Self {
        Self {
            object_path,
            effect,
            kind,
        }
    }

    pub const fn create(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Create)
    }

    pub const fn create_typed(object_path: ObjectPath, kind: &'static str) -> Self {
        Self::new_with_kind(object_path, SinkEffect::Create, Some(kind))
    }

    pub const fn delete(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Delete)
    }

    pub const fn update(object_path: ObjectPath) -> Self {
        Self::new(object_path, SinkEffect::Update)
    }

    pub const fn update_typed(object_path: ObjectPath, kind: &'static str) -> Self {
        Self::new_with_kind(object_path, SinkEffect::Update, Some(kind))
    }

    pub const fn path(&self) -> &ObjectPath {
        &self.object_path
    }

    pub const fn effect(&self) -> &SinkEffect {
        &self.effect
    }

    pub const fn kind(&self) -> Option<&'static str> {
        self.kind
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
    pub const fn new() -> Self {
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
