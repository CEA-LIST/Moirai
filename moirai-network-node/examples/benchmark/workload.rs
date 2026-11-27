use serde::Deserialize;
use std::{
    io,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::mpsc::{SyncSender, TrySendError},
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    pub rate_per_replica: NonZeroU32,
    pub duration_s: NonZeroU64,
    pub query_percent: u8,
    /// Share of updates that add a key. The remaining updates remove a key.
    pub add_percent: u8,
    pub key_space: NonZeroU32,
    pub queue_capacity: NonZeroUsize,
    pub seed: u64,
}

pub enum Operation {
    Add(u32),
    Remove(u32),
    Contains(u32),
}

#[derive(Default)]
pub struct WorkloadStats {
    pub scheduled: u64,
    pub enqueued: u64,
    pub queue_full: u64,
    pub missed_slots: u64,
}

impl Workload {
    pub fn validate(&self) -> io::Result<()> {
        if self.query_percent > 100
            || self.add_percent > 100
            || self.duration_s.get() > 86_400
            || self.rate_per_replica.get() > 1_000_000
            || self.queue_capacity.get() > 1_000_000
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid workload limits",
            ));
        }
        Ok(())
    }

    pub fn produce(
        self,
        replica_id: u16,
        start: Instant,
        queue: SyncSender<Operation>,
    ) -> WorkloadStats {
        let seed = self.seed ^ (u64::from(replica_id) << 32);
        let mut operations = SplitMix64(seed);
        let mut arrivals = SplitMix64(seed ^ 0xe7037ed1a0b428db);
        let end = start + Duration::from_secs(self.duration_s.get());
        let mean_seconds = 1.0 / f64::from(self.rate_per_replica.get());
        let mut stats = WorkloadStats::default();
        let mut due = start + arrivals.delay(mean_seconds);
        while due < end {
            let now = Instant::now();
            if due > now {
                thread::sleep(due - now);
            }
            stats.scheduled += 1;
            let now = Instant::now();
            
            //No operations skipped before ending
            if now >= end {
            //if now >= end || now.duration_since(due) > Duration::from_millis(1) {
                stats.missed_slots += 1;
            } else {
                match queue.try_send(self.operation(&mut operations)) {
                    Ok(()) => stats.enqueued += 1,
                    Err(TrySendError::Full(_)) => stats.queue_full += 1,
                    Err(TrySendError::Disconnected(_)) => return stats,
                }
            }
            due += arrivals.delay(mean_seconds);
        }
        thread::sleep(end.saturating_duration_since(Instant::now()));
        stats
    }

    fn operation(self, rng: &mut SplitMix64) -> Operation {
        let key = (rng.next() % u64::from(self.key_space.get())) as u32;
        if rng.next() % 100 < u64::from(self.query_percent) {
            Operation::Contains(key)
        } else if rng.next() % 100 < u64::from(self.add_percent) {
            Operation::Add(key)
        } else {
            Operation::Remove(key)
        }
    }
}

// SplitMix64 gives each replica a repeatable stream without an extra dependency.
struct SplitMix64(u64);

impl SplitMix64 {
    fn delay(&mut self, mean_seconds: f64) -> Duration {
        // The top 53 bits form a uniform sample in [0, 1).
        let uniform = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        Duration::from_secs_f64((-(-uniform).ln_1p() * mean_seconds).max(1e-9))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let value = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        let value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}
