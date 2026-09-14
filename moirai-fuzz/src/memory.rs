//! Retained Rust data estimates, not process RSS or transient allocation peaks.
//!
//! Each replica is measured independently, including its log, populated caches, broadcast
//! buffers and test instrumentation. Shared allocations are deduplicated within a replica,
//! not across replicas. The execution graph, query results and sampler storage are excluded.

use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPhase {
    Initial,
    Operations,
    FinalMerge,
    FinalQueries,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySample {
    pub issued_operations: usize,
    pub phase: MemoryPhase,
    /// Index i corresponds to replica i. Includes the inline size of the replica.
    pub bytes_per_replica: Vec<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryUsage {
    pub samples: Vec<MemorySample>,
    /// Maximum observed at sample boundaries, not an allocation high-water mark.
    pub sampled_peak_bytes_per_replica: Vec<usize>,
    pub final_bytes_per_replica: Vec<usize>,
}

impl MemoryUsage {
    pub(crate) fn sample<R: DeepSizeOf>(
        &mut self,
        issued_operations: usize,
        phase: MemoryPhase,
        replicas: &[R],
    ) {
        let bytes: Vec<_> = replicas.iter().map(DeepSizeOf::deep_size_of).collect();
        self.sampled_peak_bytes_per_replica.resize(bytes.len(), 0);
        for (peak, current) in self.sampled_peak_bytes_per_replica.iter_mut().zip(&bytes) {
            *peak = (*peak).max(*current);
        }
        self.final_bytes_per_replica.clone_from(&bytes);
        self.samples.push(MemorySample {
            issued_operations,
            phase,
            bytes_per_replica: bytes,
        });
    }
}
