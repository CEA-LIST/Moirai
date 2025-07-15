use chrono::Local;
use colored::Colorize;
use core::{panic, time};
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error};
use petgraph::{
    algo::has_path_connecting,
    dot::Config,
    graph::{DiGraph, NodeIndex},
};
use rand::{
    seq::{IndexedRandom, IteratorRandom},
    Rng, SeedableRng,
};
use rand_chacha::ChaCha8Rng;
use serde::Serialize;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{create_dir_all, File},
    io::Write,
    time::Instant,
};

use crate::{
    clocks::dot::Dot,
    crdt::test_util::n_members,
    protocol::{
        event::Event,
        log::Log,
        pulling::{Batch, Since},
        tcsb::Tcsb,
    },
};

#[derive(Debug)]
struct WitnessGraph {
    graph: DiGraph<WitnessGraphNode, ()>,
    dot_index_map: HashMap<Dot, NodeIndex>,
}

impl WitnessGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            dot_index_map: HashMap::new(),
        }
    }

    pub fn add_node<Op: Debug>(&mut self, event: &Event<Op>) {
        let dot_event = Dot::from(event);
        let node = WitnessGraphNode::new(&dot_event, &event.op);
        let index = self.graph.add_node(node);
        self.dot_index_map.insert(dot_event.clone(), index);
        for (idx, cnt) in event.metadata().iter() {
            if *cnt == 0 && idx == &event.metadata().origin.expect("Origin not set") {
                // If the count is 0 and the index is the origin, we skip this edge
                continue;
            }
            let dot = if *cnt > 1 && idx == &event.metadata().origin.expect("Origin not set") {
                Dot::new(*idx, *cnt - 1, event.lamport, &event.metadata().view)
            } else {
                Dot::new(*idx, *cnt, event.lamport, &event.metadata().view)
            };
            let parent_idx = self.dot_index_map.get(&dot).unwrap_or_else(|| {
                panic!(
                    "Parent dot {} not found in the witness graph. Event: {}",
                    dot, event
                )
            });
            self.graph.add_edge(*parent_idx, index, ());
        }
    }

    /// Computes the percentage of concurrent pairs of operations
    /// based on the authors of the operations.
    /// WARNING: This function has a complexity of o(n * log n) due to the pairwise checks.
    pub fn concurrency_score(&self, mp: &MultiProgress) -> f64 {
        // Setup concurrency score progress bar
        let conc_1_pb = mp.add(ProgressBar::new(self.graph.node_count() as u64));
        conc_1_pb.set_prefix("Author map".yellow().to_string());
        conc_1_pb.set_style(
            ProgressStyle::with_template(
                "{prefix} {bar:40.magenta/blue} {pos}/{len} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );

        let nodes = self.graph.node_indices().collect::<Vec<_>>();

        // Map author id to vector of NodeIndex
        let mut author_map: HashMap<String, Vec<NodeIndex>> = HashMap::new();
        for &node_idx in &nodes {
            let author = &self.graph[node_idx].dot.origin();
            author_map
                .entry(author.to_string())
                .or_default()
                .push(node_idx);
            conc_1_pb.inc(1);
        }

        conc_1_pb.finish_with_message("Author map completed".green().to_string());

        // Flatten all nodes into a vector for pairwise check
        let mut different_author_pairs = Vec::new();

        let iterations = self.graph.node_count() * (self.graph.node_count() - 1) / 2;
        let conc_2_pb = mp.add(ProgressBar::new(iterations as u64));
        conc_2_pb.set_prefix("Different author pairs".yellow().to_string());
        conc_2_pb.set_style(
            ProgressStyle::with_template(
                "{prefix} {bar:40.magenta/blue} {pos}/{len} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = nodes[i];
                let b = nodes[j];

                let author_a = &self.graph[a].dot.origin();
                let author_b = &self.graph[b].dot.origin();

                if author_a != author_b {
                    different_author_pairs.push((a, b));
                }
                conc_2_pb.inc(1);
            }
        }

        conc_2_pb.finish_with_message("Different author pairs completed".green().to_string());

        if different_author_pairs.is_empty() {
            return 0.0; // No cross-author pairs → treat as fully sequential (or no concurrency)
        }

        let conc_3_pb = mp.add(ProgressBar::new(different_author_pairs.len() as u64));
        conc_3_pb.set_prefix("Checking concurrency".yellow().to_string());
        // TODO: HumanCount(33857009).to_string()
        conc_3_pb.set_style(
            ProgressStyle::with_template(
                "{prefix} {bar:40.magenta/blue} {pos}/{len} ({percent}%) [{elapsed_precise} elapsed, eta {eta_precise}] {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );

        let mut concurrent_count = 0;
        for &(a, b) in &different_author_pairs {
            let a_to_b = has_path_connecting(&self.graph, a, b, None);
            let b_to_a = has_path_connecting(&self.graph, b, a, None);

            // If neither reachable, nodes are concurrent
            if !a_to_b && !b_to_a {
                concurrent_count += 1;
            }
            conc_3_pb.inc(1);
            let eta = conc_3_pb.eta();
            conc_3_pb.set_message(format!("remains {}", HumanDuration(eta)));
        }

        conc_3_pb.finish_with_message("Concurrency check completed".green().to_string());

        concurrent_count as f64 / different_author_pairs.len() as f64
    }
}

struct WitnessGraphNode {
    dot: Dot,
    op: String,
}

impl Debug for WitnessGraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.dot, self.op)
    }
}

impl WitnessGraphNode {
    pub fn new<Op: Debug>(dot: &Dot, op: &Op) -> Self {
        Self {
            dot: dot.clone(),
            op: format!("{:?}", op),
        }
    }
}

pub struct EventGraphConfig<'a, L>
where
    L: Log,
{
    /// Name of the simulation, used for logging
    pub name: &'a str,
    /// Number of iterations to run the simulation
    // num_iterations: usize,
    /// Number of replicas in the system
    pub num_replicas: usize,
    /// Total number of operations to be issued
    pub num_operations: usize,
    /// Set of operations to be performed by the replicas
    pub operations: &'a [L::Op],
    /// Whether to perform a final synchronization after all operations are issued
    pub final_sync: bool,
    /// Churn rate defines the probability of a replica going offline after each operation
    pub churn_rate: f64,
    /// Optional reachability matrix to define which replicas can communicate with each other
    pub reachability: Option<Vec<Vec<bool>>>,
    /// Comparison function to check if the replicas converge
    pub compare: fn(&L::Value, &L::Value) -> bool,
    /// Whether to log the results to a file
    pub record_results: bool,
    /// Whether to record the events in a witness graph for debugging
    pub witness_graph: bool,
    /// Seed for the random number generator
    pub seed: Option<[u8; 32]>,
    pub concurrency_score: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
struct Record {
    name: String,
    operation_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    concurrency_score: Option<f64>,
    distinct_ops: usize,
    cumulated_time_to_deliver: HashMap<String, time::Duration>,
    num_replicas: usize,
    num_operations: usize,
    churn_rate: f64,
    seed: [u8; 32],
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_graph: Option<String>,
    // num_iterations: usize,
}

pub fn op_weaver<L>(config: EventGraphConfig<'_, L>)
where
    L: Log,
    L::Value: Default,
{
    // <--- Configuration Validation --->
    // Ensure that the number of replicas is at least 2
    if config.num_replicas < 2 {
        panic!("The number of replicas must be at least 2.");
    }
    // Ensure that the total number of operations is positive
    if config.num_operations == 0 {
        panic!("The total number of operations must be greater than 0.");
    }
    // Ensure that the operations slice is not empty
    if config.operations.is_empty() {
        panic!("The operations slice cannot be empty.");
    }
    // Ensure that the churn rate is between 0 and 1
    if config.churn_rate < 0.0 || config.churn_rate > 1.0 {
        panic!("The churn rate must be between 0 and 1.");
    }
    if config.concurrency_score && !config.witness_graph {
        panic!("Concurrency score can only be computed if witness graph is enabled.");
    }
    // Ensure that the reachability matrix is square and matches the number of replicas
    if let Some(ref reachability) = config.reachability {
        if reachability.len() != config.num_replicas {
            panic!("The reachability matrix must have the same number of rows as the number of replicas.");
        }
        for row in reachability {
            if row.len() != config.num_replicas {
                panic!("Each row of the reachability matrix must have the same number of columns as the number of replicas.");
            }
        }
    }

    // <--- Initialization --->
    // Setup multi progress display
    let mp = MultiProgress::new();

    // Setup operations progress bar
    let ops_pb = mp.add(ProgressBar::new(config.num_operations as u64));
    ops_pb.set_prefix("📤  Issuing Ops".yellow().to_string());
    ops_pb.set_style(
        ProgressStyle::with_template(
            "{prefix} {bar:40.magenta/blue} {pos}/{len} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    // Setup convergence progress bar
    let conv_pb = mp.add(ProgressBar::new(config.num_replicas as u64));
    conv_pb.set_prefix("🔄 Convergence".yellow().to_string());
    conv_pb.set_style(
        ProgressStyle::with_template(
            "{prefix} {bar:40.magenta/blue} {pos}/{len} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    // Generate a random seed if not provided
    let mut rng = if let Some(seed) = config.seed {
        ChaCha8Rng::from_seed(seed)
    } else {
        ChaCha8Rng::from_os_rng()
    };
    // Create a vector of replicas
    let mut tcsbs: Vec<Tcsb<L>> = n_members::<L>(config.num_replicas);
    // Initialize the reachability matrix if not provided
    let reachability = config
        .reachability
        .unwrap_or_else(|| vec![vec![true; config.num_replicas]; config.num_replicas]);
    // Initialize the online/offline status of replicas
    let mut online = vec![true; config.num_replicas];
    // Number of operations issued
    let mut count = 0;
    // Directed acyclic graph that keep track of the issued operations and their dependencies for debugging
    let mut witness_graph: Option<WitnessGraph> = if config.witness_graph {
        Some(WitnessGraph::new())
    } else {
        None
    };
    // Initialize the total time spent to deliver the operations per replica
    let mut deliver_time = HashMap::<String, time::Duration>::new();

    fn deliver_batch<L: Log>(
        batch: Batch<L::Op>,
        tcsbs: &mut [Tcsb<L>],
        r_idx: usize,
        deliver_time: &mut time::Duration,
    ) {
        for event in batch.events {
            if tcsbs[r_idx].id != event.origin() {
                let start = Instant::now();
                tcsbs[r_idx].try_deliver(event);
                let elapsed = start.elapsed();
                // Update the cumulated time to deliver for the replica
                *deliver_time += elapsed;
            }
        }
    }

    // <--- Main Loop --->

    // While the number of operations issued is less than the total operations specified in the config
    while count < config.num_operations {
        // Randomly select a replica index
        let r_idx = (0..config.num_replicas).choose(&mut rng).unwrap();

        // If the replica is online, deliver any pending events from other replicas
        if online[r_idx] {
            for i in (0..config.num_replicas)
                .filter(|&i| i != r_idx && online[i] && reachability[r_idx][i])
            {
                let batch = tcsbs[i]
                    .events_since(&Since::new_from(&tcsbs[r_idx]))
                    .unwrap();
                let r_idx_deliver_time = deliver_time.entry(tcsbs[r_idx].id.clone()).or_default();
                deliver_batch(batch, &mut tcsbs, r_idx, r_idx_deliver_time);
            }
        }

        let op = config.operations.choose(&mut rng).unwrap();
        ops_pb.inc(1);
        count += 1;
        // Create a new event with the operation
        let event = tcsbs[r_idx].tc_bcast(op.clone());

        if let Some(wg) = &mut witness_graph {
            // Add the event to the witness graph for debugging
            wg.add_node(&event);
        }

        if online[r_idx] {
            for i in (0..config.num_replicas)
                .filter(|&i| i != r_idx && online[i] && reachability[r_idx][i])
            {
                let start = Instant::now();
                tcsbs[i].try_deliver(event.clone());
                let elapsed = start.elapsed();
                // Update the cumulated time to deliver for the replica
                let r_idx_deliver_time = deliver_time.entry(tcsbs[i].id.clone()).or_default();
                *r_idx_deliver_time += elapsed;
            }
        }

        // Randomly decide whether the replicas go offline or not
        for online_flag in &mut online {
            *online_flag = rng.random_bool(1.0 - config.churn_rate);
        }
    }

    ops_pb.finish_with_message("📤 Operations issued".green().to_string());

    if config.final_sync {
        for i in 0..config.num_replicas {
            for j in 0..config.num_replicas {
                if i != j {
                    let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j])).unwrap();
                    let j_deliver_time = deliver_time.entry(tcsbs[j].id.clone()).or_default();
                    deliver_batch(batch, &mut tcsbs, j, j_deliver_time);
                }
            }
        }
    }

    // Verify that all replicas converge to the same value
    let mut reference: L::Value = L::Value::default();
    let mut event_sum = 0;
    for (i, tcsb) in tcsbs.iter().enumerate() {
        if i == 0 {
            reference = tcsb.eval();
            event_sum = tcsb.my_clock().sum();
        }
        assert_eq!(tcsb.my_clock().sum(), event_sum);

        let eval = tcsb.eval();

        if !(config.compare)(&eval, &reference) {
            error!(
                "Replica {} did not converge with the reference. Expected: {:?}, Got: {:?}",
                tcsb.id, reference, eval,
            );
            // error!("Seed: {:?}", config.seed.unwrap_or(rng.get_seed()));
            if let Some(wg) = &witness_graph {
                error!(
                    "Witness graph: {:?}",
                    petgraph::dot::Dot::with_config(&wg.graph, &[Config::EdgeNoLabel])
                );
            }
            panic!("Replicas did not converge.");
        }
        conv_pb.inc(1);
    }

    conv_pb.finish_with_message("🔄 All replicas converged".green().to_string());

    let concurrency_score = if config.concurrency_score && config.witness_graph {
        let wg = witness_graph
            .as_ref()
            .expect("Witness graph should be initialized");
        Some(wg.concurrency_score(&mp))
    } else {
        None
    };

    let _execution_graph = witness_graph.as_ref().map(|wg| {
        format!(
            "{:#?}",
            petgraph::dot::Dot::with_config(&wg.graph, &[Config::EdgeNoLabel])
        )
    });

    if config.record_results {
        // write the results in a JSON file
        create_dir_all("logs").unwrap();
        let file_path = format!(
            "logs/{}_{}.json",
            config.name,
            Local::now().format("%Y-%m-%d_%H-%M-%S")
        );
        let mut file = File::create(&file_path).unwrap();
        let record = Record {
            name: config.name.to_string(),
            seed: rng.get_seed(),
            distinct_ops: config.operations.len(),
            concurrency_score,
            num_replicas: config.num_replicas,
            num_operations: config.num_operations,
            churn_rate: config.churn_rate,
            cumulated_time_to_deliver: deliver_time.clone(),
            operation_seconds: (config.num_replicas * config.num_operations) as f64
                / deliver_time.values().map(|d| d.as_secs_f64()).sum::<f64>(),
            // TODO: fix export of graph to be readable
            execution_graph: None,
        };
        let json = serde_json::to_string_pretty(&record).unwrap();
        if let Err(e) = file.write_all(json.as_bytes()) {
            error!("Failed to write results to file {}: {}", file_path, e);
        } else {
            debug!("Results written to {}", file_path);
        }
    }
}
