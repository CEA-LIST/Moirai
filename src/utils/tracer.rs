use crate::protocol::{event::Event, metadata::Metadata, pure_crdt::PureCRDT};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, fs::File, io::Write, path::Path};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tracer {
    pub(super) origin: String,
    pub(super) trace: Vec<TracerEvent>,
}

impl Tracer {
    pub fn new(origin: String) -> Self {
        Self {
            origin,
            trace: Vec::new(),
        }
    }

    pub fn append<C: PureCRDT + Debug>(&mut self, event: Event<C>) {
        let op_string = format!("{:?}", event.op);
        let metadata = event.metadata.clone();
        self.trace.push(TracerEvent::new(metadata, op_string));
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracerEvent {
    pub(super) metadata: Metadata,
    pub(super) op: String,
}

impl TracerEvent {
    pub fn new(metadata: Metadata, op: String) -> Self {
        Self { metadata, op }
    }
}
