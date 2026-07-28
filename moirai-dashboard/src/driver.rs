//! `--random`: apply seeded random operations to a running session.
//!
//! The generator is [`moirai_network::workload::Workload`], the same one the
//! randomised end-to-end scenario (R1) uses. One implementation, so a run the
//! dashboard showed can be replayed by the suite from the seed it printed.

use std::thread;
use std::time::{Duration, Instant};

use moirai_network::workload::Workload;

pub struct Driver {
    /// Base URLs of the replicas, e.g. `http://127.0.0.1:8081`.
    pub targets: Vec<String>,
    pub seed: u64,
    /// Operations per second across the whole session.
    pub rate: f64,
    /// Stop after this many operations; `None` runs until killed.
    pub count: Option<usize>,
}

impl Driver {
    pub fn run(self) {
        if self.targets.is_empty() {
            eprintln!("--random: no --nodes given, nothing to drive");
            return;
        }
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("--random: no HTTP client: {e}");
                return;
            }
        };

        let mut workload = Workload::new(self.seed, self.targets.len());
        let gap = Duration::from_secs_f64(1.0 / self.rate.max(0.001));
        println!(
            "driving {} replicas at {}/s from seed {} — replay with \
             MOIRAI_E2E_SEED={} on the R1 scenario",
            self.targets.len(),
            self.rate,
            self.seed,
            self.seed
        );

        let mut applied = 0usize;
        let mut rejected = 0usize;
        loop {
            if self.count.is_some_and(|n| applied + rejected >= n) {
                break;
            }
            let started = Instant::now();
            let step = workload.next();
            let url = format!(
                "{}/api/op",
                self.targets[step.replica].trim_end_matches('/')
            );
            // `/api/op` answers 200 with `{"success":false}` when the CRDT
            // refuses the operation, so the status code alone would report a
            // refusal as a success — which it did, until a run showed forty
            // operations "applied" and twenty-eight delivered.
            match client
                .post(&url)
                .json(&step.op)
                .send()
                .and_then(|r| r.error_for_status())
                .and_then(|r| r.json::<serde_json::Value>())
            {
                Ok(body) if body.get("success").and_then(|v| v.as_bool()) == Some(true) => {
                    applied += 1
                }
                Ok(body) => {
                    rejected += 1;
                    eprintln!("{url} refused `{}`: {body}", step.label);
                }
                Err(e) => {
                    rejected += 1;
                    eprintln!("{url}: {e}");
                }
            }
            if applied % 50 == 0 && applied > 0 {
                println!("applied {applied}, rejected {rejected}");
            }
            if let Some(remaining) = gap.checked_sub(started.elapsed()) {
                thread::sleep(remaining);
            }
        }
        println!(
            "done: applied {applied}, rejected {rejected}, seed {}",
            self.seed
        );
    }
}
