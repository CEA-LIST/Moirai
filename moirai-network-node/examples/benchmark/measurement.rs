use deepsize::{Context, DeepSizeOf};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::protocol::AtomicBroadcastProtocol,
    event::Event,
    state::effect_context::EffectContext,
};
use serde::Serialize;
use std::{
    collections::VecDeque,
    fs, io,
    os::raw::{c_int, c_long},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use super::commit_set_benchmark::{Child, Update};

#[derive(Default)]
pub struct Observer {
    pub start: Option<Instant>,
    pub dequeued_at: Option<Instant>,
    pub committed_updates: u64,
    pub committed_local_updates: u64,
    pub latency_samples: Vec<(u64, u64, usize)>,
    pending_by_origin: Vec<VecDeque<Pending>>,
    local_origin: Option<usize>,
}

impl Observer {
    pub fn uncommitted_local(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.pending_by_origin
            .iter()
            .flatten()
            .filter_map(|pending| {
                let dequeued = pending.dequeued_at?;
                Some((
                    dequeued.duration_since(self.start?).as_nanos() as u64,
                    pending.seq,
                ))
            })
    }
}

struct Pending {
    seq: usize,
    dequeued_at: Option<Instant>,
}

#[derive(Default)]
pub struct Measured<A> {
    inner: A,
    pub observer: Observer,
}

impl<A> Measured<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            observer: Observer::default(),
        }
    }

    pub fn inner_mut(&mut self) -> &mut A {
        &mut self.inner
    }
}

impl<A: DeepSizeOf> DeepSizeOf for Measured<A> {
    fn deep_size_of_children(&self, ctx: &mut Context) -> usize {
        self.inner.deep_size_of_children(ctx)
    }
}

impl<A: AtomicBroadcastProtocol<Child>> AtomicBroadcastProtocol<Child> for Measured<A> {
    type Update = A::Update;

    fn ab_prepare(&self, op: Update) -> Self::Update {
        self.inner.ab_prepare(op)
    }

    fn child_op(op: &Self::Update) -> &Update {
        A::child_op(op)
    }

    fn is_default(&self) -> bool {
        self.inner.is_default()
    }

    fn ab_handler(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        if matches!(A::child_op(event.op()), Update::Add(_) | Update::Remove(_)) {
            let origin = event.id().idx().0;
            if self.observer.pending_by_origin.len() <= origin {
                self.observer
                    .pending_by_origin
                    .resize_with(origin + 1, VecDeque::new);
            }
            let dequeued_at = self.observer.dequeued_at.take();
            if dequeued_at.is_some() {
                self.observer.local_origin = Some(origin);
            }
            self.observer.pending_by_origin[origin].push_back(Pending {
                seq: event.id().seq(),
                dequeued_at,
            });
        }
        self.inner.ab_handler(event, ctx);
    }

    fn ab_ready_to_commit(&self) -> Vec<&Version> {
        self.inner.ab_ready_to_commit()
    }

    fn ab_prune(&mut self, version: &Version) {
        self.inner.ab_prune(version);
    }

    fn ab_last_committed(&self) -> Option<&CausalFrontier> {
        self.inner.ab_last_committed()
    }

    fn ab_set_last_committed(&mut self, version: &Version) {
        self.inner.ab_set_last_committed(version);
        let Some(frontier) = self.inner.ab_last_committed() else {
            return;
        };
        let now = Instant::now();
        let observer = &mut self.observer;
        for (origin, seq) in frontier.iter() {
            let Some(pending) = observer.pending_by_origin.get_mut(origin.0) else {
                continue;
            };
            // Causal delivery keeps each origin's updates in sequence order.
            while pending.front().is_some_and(|update| update.seq <= seq) {
                let update = pending.pop_front().unwrap();
                let Some(start) = observer.start else {
                    continue;
                };
                observer.committed_updates += 1;
                if let Some(dequeued_at) = update.dequeued_at {
                    observer.committed_local_updates += 1;
                    observer.latency_samples.push((
                        now.duration_since(start).as_nanos() as u64,
                        now.duration_since(dequeued_at).as_nanos() as u64,
                        update.seq,
                    ));
                }
            }
        }
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct Resources {
    pub cpu_time_us: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

impl Resources {
    pub fn read(interface: &str) -> io::Result<Self> {
        let net = fs::read_to_string("/proc/net/dev")?;
        let row = net
            .lines()
            .find_map(|line| {
                let (name, counters) = line.split_once(':')?;
                (name.trim() == interface).then_some(counters)
            })
            .ok_or_else(|| io::Error::other(format!("no network interface {interface}")))?;
        let counter = |index| -> io::Result<u64> {
            row.split_whitespace()
                .nth(index)
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| io::Error::other("invalid /proc/net/dev counter"))
        };
        Ok(Self {
            cpu_time_us: process_cpu_us()?,
            rx_bytes: counter(0)?,
            tx_bytes: counter(8)?,
        })
    }

    pub fn since(self, start: Self) -> io::Result<Self> {
        let delta = |end: u64, begin| {
            end.checked_sub(begin)
                .ok_or_else(|| io::Error::other("resource counter moved backwards"))
        };
        Ok(Self {
            cpu_time_us: delta(self.cpu_time_us, start.cpu_time_us)?,
            rx_bytes: delta(self.rx_bytes, start.rx_bytes)?,
            tx_bytes: delta(self.tx_bytes, start.tx_bytes)?,
        })
    }
}

// Linux process CPU clock measures user + system CPU across the replica's threads.
fn process_cpu_us() -> io::Result<u64> {
    #[repr(C)]
    struct Timespec {
        seconds: c_long,
        nanos: c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock_id: c_int, value: *mut Timespec) -> c_int;
    }
    const CLOCK_PROCESS_CPUTIME_ID: c_int = 2;
    let mut clock = Timespec {
        seconds: 0,
        nanos: 0,
    };
    // clock points to writable storage with the system timespec layout.
    if unsafe { clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mut clock) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(clock.seconds as u64 * 1_000_000 + clock.nanos as u64 / 1000)
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
