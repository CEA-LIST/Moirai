use std::{fmt::Debug, fs::File, io::Write, path::Path};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event::Event, log::Log},
};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Tracer {
    pub(super) origin: String,
    pub(super) trace: Vec<TracerEvent>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TracerEvent {
    pub(super) metadata: DependencyClock,
    pub(super) op: String,
}

impl Tracer {
    pub fn new(origin: String) -> Self {
        Self {
            origin,
            trace: Vec::new(),
        }
    }

    pub fn append<L: Log>(&mut self, event: Event<L::Op>) {
        let op_string = format!("{:?}", event.op);
        let metadata = event.metadata.clone();
        self.trace.push(TracerEvent {
            metadata,
            op: op_string,
        });
    }

    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(Into::into)
    }

    pub fn serialize_to_file(&self, path: &Path) -> Result<()> {
        let mut file = File::create(path)?;
        let serialized = self.serialize()?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }

    pub fn deserialize_from_file(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let tracer: Self = serde_json::from_reader(file)?;
        Ok(tracer)
    }

    pub fn clear(&mut self) {
        self.trace.clear();
    }
}
