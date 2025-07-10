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

struct EventGraphConfig<'a, L>
where
    L: Log,
{
    /// Name of the simulation, used for logging
    name: &'a str,
    /// Number of iterations to run the simulation
    // num_iterations: usize,
    /// Number of replicas in the system
    num_replicas: usize,
    /// Total number of operations to be issued
    num_operations: usize,
    /// Set of operations to be performed by the replicas
    operations: &'a [L::Op],
    /// Whether to perform a final synchronization after all operations are issued
    final_sync: bool,
    /// Churn rate defines the probability of a replica going offline after each operation
    churn_rate: f64,
    /// Optional reachability matrix to define which replicas can communicate with each other
    reachability: Option<Vec<Vec<bool>>>,
    /// Comparison function to check if the replicas converge
    compare: fn(&L::Value, &L::Value) -> bool,
    /// Whether to log the results to a file
    record_results: bool,
    /// Whether to record the events in a witness graph for debugging
    witness_graph: bool,
    /// Seed for the random number generator
    seed: Option<[u8; 32]>,
    concurrency_score: bool,
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

fn op_weaver<L>(config: EventGraphConfig<'_, L>)
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

    let execution_graph = if let Some(wg) = &witness_graph {
        Some(format!(
            "{:#?}",
            petgraph::dot::Dot::with_config(&wg.graph, &[Config::EdgeNoLabel])
        ))
    } else {
        None
    };

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
    // mp.clear().unwrap();
}

#[cfg(feature = "utils")]
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use log::info;
    use petgraph::graph::DiGraph;

    use super::*;
    use crate::{
        crdt::{
            aw_set::AWSet,
            class_diagram::{
                export_fancy_class_diagram, Class, ClassDiagram, ClassDiagramCrdt, Ends, Feature,
                Multiplicity, Operation, PrimitiveType, Relation, RelationType, TypeRef,
                Visibility,
            },
            ew_flag::EWFlag,
            lww_register::LWWRegister,
            multidigraph::Graph,
            mv_register::MVRegister,
            resettable_counter::Counter,
            to_register::TORegister,
            uw_map::{UWMap, UWMapLog},
            uw_multigraph::{Content, UWGraph, UWGraphLog},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn lot_of_iterations() {
        for i in 0..1_000 {
            // percentage completed
            info!("Completed: {:.2}%", (i as f64 / 1_000.0) * 100.0);
            // generate_uw_multigraph_convergence();
            generate_class_diagram();
            // generate_uw_map_convergence();
            // generate_lww_register_convergence();
            // generate_deeply_nested_uw_map_convergence();
            // generate_ewflag_convergence();
            // generate_graph_convergence();
        }
    }

    #[test_log::test]
    fn generate_ewflag_convergence() {
        use crate::crdt::ew_flag::EWFlag;

        let ops = vec![EWFlag::Enable, EWFlag::Disable, EWFlag::Clear];

        let config = EventGraphConfig {
            name: "ewflag",
            num_replicas: 8,
            num_operations: 10_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            compare: |a: &bool, b: &bool| a == b,
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<EWFlag>>(config);
    }

    #[test_log::test]
    fn generate_aw_set_convergence() {
        let mut ops = Vec::with_capacity(10_000);

        // Add operations from 0 to 4999
        for val in 0..5000 {
            ops.push(AWSet::Add(val));
        }

        // Remove operations from 0 to 4999
        for val in 0..5000 {
            ops.push(AWSet::Remove(val));
        }

        let config = EventGraphConfig {
            name: "aw_set",
            num_replicas: 8,
            num_operations: 100_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.4,
            reachability: None,
            compare: |a: &HashSet<i32>, b: &HashSet<i32>| a == b,
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<AWSet<i32>>>(config);
    }

    // #[test_log::test]
    // fn generate_counter_convergence() {
    //     let ops: Vec<Counter<isize>> = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];

    //     let config = EventGraphConfig {
    //         n_replicas: 5,
    //         total_operations: 10_000,
    //         ops: &ops,
    //         final_sync: true,
    //         churn_rate: 0.7,
    //         ..Default::default()
    //     };

    //     let tcsbs = op_weaver::<EventGraph<Counter<isize>>>(config);

    //     // All replicas' eval() should match
    //     let mut reference_val: isize = 0;
    //     for (i, tcsb) in tcsbs.iter().enumerate() {
    //         if i == 0 {
    //             reference_val = tcsb.eval();
    //         }
    //         assert_eq!(tcsb.eval(), reference_val);
    //     }
    // }

    // #[test_log::test]
    // fn generate_uw_map_convergence() {
    //     let ops = vec![
    //         UWMap::Update("a".to_string(), MVRegister::Write(1)),
    //         UWMap::Update("a".to_string(), MVRegister::Write(2)),
    //         UWMap::Update("a".to_string(), MVRegister::Write(3)),
    //         UWMap::Remove("a".to_string()),
    //         UWMap::Update("b".to_string(), MVRegister::Write(5)),
    //         UWMap::Update("b".to_string(), MVRegister::Write(6)),
    //         UWMap::Update("b".to_string(), MVRegister::Write(7)),
    //         UWMap::Remove("b".to_string()),
    //         UWMap::Update("c".to_string(), MVRegister::Write(10)),
    //         UWMap::Update("c".to_string(), MVRegister::Write(20)),
    //         UWMap::Update("c".to_string(), MVRegister::Write(30)),
    //         UWMap::Remove("c".to_string()),
    //     ];

    //     let config = EventGraphConfig {
    //         n_replicas: 8,
    //         total_operations: 100,
    //         ops: &ops,
    //         final_sync: true,
    //         churn_rate: 0.3,
    //         seed: Some([
    //             107, 98, 200, 232, 228, 22, 253, 53, 63, 125, 253, 181, 140, 232, 11, 11, 56, 87,
    //             104, 239, 101, 72, 57, 162, 101, 255, 24, 204, 250, 183, 157, 95,
    //         ]),
    //         ..Default::default()
    //     };

    //     let tcsbs = op_weaver::<UWMapLog<String, EventGraph<MVRegister<i32>>>>(config);

    //     // All replicas' eval() should match
    //     let mut reference_val: HashMap<String, HashSet<i32>> = HashMap::new();
    //     let mut event_sum = 0;
    //     for (i, tcsb) in tcsbs.iter().enumerate() {
    //         if i == 0 {
    //             reference_val = tcsb.eval();
    //             event_sum = tcsb.my_clock().sum();
    //         }
    //         assert_eq!(tcsb.my_clock().sum(), event_sum);
    //         assert_eq!(
    //             tcsb.eval(),
    //             reference_val,
    //             "Replica {} did not converge with the reference.",
    //             tcsb.id,
    //         );
    //         println!("Replica {}: {:?}", tcsb.id, tcsb.eval());
    //     }
    // }

    #[test_log::test]
    fn generate_class_diagram() {
        let ops = vec![
            UWGraph::UpdateVertex(
                "wt",
                Class::Name(MVRegister::Write("WindTurbine".to_string())),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "shutdown".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            // EnergyGenerator class
            UWGraph::UpdateVertex(
                "eg",
                Class::Name(MVRegister::Write("EnergyGenerator".to_string())),
            ),
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "getEnergyOutput".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Class("wt".to_string()))),
                )),
            ),
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "getEnergyOutput".to_string(),
                    Operation::IsAbstract(EWFlag::Enable),
                )),
            ),
            UWGraph::UpdateVertex("eg", Class::IsAbstract(EWFlag::Enable)),
            UWGraph::UpdateArc(
                "wt",
                "eg",
                "ext",
                Relation::Typ(TORegister::Write(RelationType::Extends)),
            ),
            // Rotor class
            UWGraph::UpdateVertex("rotor", Class::Name(MVRegister::Write("Rotor".to_string()))),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "diameter".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Private)),
                )),
            ),
            // Blade class
            UWGraph::UpdateVertex("blade", Class::Name(MVRegister::Write("Blade".to_string()))),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Typ(TORegister::Write(RelationType::Composes)),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::Exactly(3)))),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Label(MVRegister::Write("comprises".to_string())),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "wt",
                "hasRotor",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "wt",
                "hasRotor",
                Relation::Label(MVRegister::Write("hasRotor".to_string())),
            ),
            // Tower class
            UWGraph::UpdateVertex("tower", Class::Name(MVRegister::Write("Tower".to_string()))),
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "heightM".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "material".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateArc(
                "tower",
                "wt",
                "mountedOn",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "tower",
                "wt",
                "mountedOn",
                Relation::Label(MVRegister::Write("mountedOn".to_string())),
            ),
            // Nacelle class
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Name(MVRegister::Write("Nacelle".to_string())),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "weightTons".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "internalTempC".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "internalTempC".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Private)),
                )),
            ),
            UWGraph::UpdateArc(
                "nacelle",
                "wt",
                "hasNacelle",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "nacelle",
                "wt",
                "hasNacelle",
                Relation::Label(MVRegister::Write("hasNacelle".to_string())),
            ),
            // EnergyGrid class
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Name(MVRegister::Write("EnergyGrid".to_string())),
            ),
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "gridName".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "capacityMW".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Label(MVRegister::Write("feedsInto".to_string())),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "energy_grid",
                "energy_grid",
                "connectedTo",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "energy_grid",
                "energy_grid",
                "connectedTo",
                Relation::Label(MVRegister::Write("connectedTo".to_string())),
            ),
            // Manufacturer class
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Name(MVRegister::Write("Manufacturer".to_string())),
            ),
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Features(UWMap::Update(
                    "name".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Label(MVRegister::Write("owns".to_string())),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Label(MVRegister::Write("repairs".to_string())),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
            ),
            // Remove ops
            UWGraph::RemoveVertex("wt"),
            UWGraph::RemoveVertex("eg"),
            UWGraph::RemoveVertex("rotor"),
            UWGraph::RemoveVertex("blade"),
            UWGraph::RemoveVertex("tower"),
            UWGraph::RemoveVertex("nacelle"),
            UWGraph::RemoveVertex("energy_grid"),
            UWGraph::RemoveVertex("manufacturer"),
            UWGraph::RemoveArc("wt", "eg", "ext"),
            UWGraph::RemoveArc("eg", "energy_grid", "feedsInto"),
            UWGraph::RemoveArc("energy_grid", "energy_grid", "connectedTo"),
            UWGraph::RemoveArc("rotor", "wt", "hasRotor"),
            UWGraph::RemoveArc("nacelle", "wt", "hasNacelle"),
            UWGraph::RemoveArc("blade", "rotor", "comprises"),
            UWGraph::RemoveArc("tower", "wt", "mountedOn"),
            UWGraph::RemoveArc("manufacturer", "wt", "owns"),
            UWGraph::RemoveArc("manufacturer", "wt", "repairs"),
            // --- DOUBLED OPERATIONS BELOW ---
            // Add more operations, same as above, but with slight variations for diversity

            // WindTurbine - add new operation and feature
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "restart".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Features(UWMap::Update(
                    "serialNumber".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // EnergyGenerator - add new operation
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "reset".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            // Rotor - add new feature
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "material".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Blade - add new feature
            UWGraph::UpdateVertex(
                "blade",
                Class::Features(UWMap::Update(
                    "length".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            // Tower - add new feature
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "foundationDepth".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            // Nacelle - add new feature
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "manufacturer".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // EnergyGrid - add new feature
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "region".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Manufacturer - add new feature
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Features(UWMap::Update(
                    "country".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Add new arcs for more relations
            UWGraph::UpdateArc(
                "wt",
                "energy_grid",
                "supplies",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "wt",
                "energy_grid",
                "supplies",
                Relation::Label(MVRegister::Write("supplies".to_string())),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "blade",
                "contains",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "blade",
                "contains",
                Relation::Label(MVRegister::Write("contains".to_string())),
            ),
            UWGraph::UpdateArc(
                "tower",
                "manufacturer",
                "builtBy",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "tower",
                "manufacturer",
                "builtBy",
                Relation::Label(MVRegister::Write("builtBy".to_string())),
            ),
            // Remove the new arcs
            UWGraph::RemoveArc("wt", "energy_grid", "supplies"),
            UWGraph::RemoveArc("rotor", "blade", "contains"),
            UWGraph::RemoveArc("tower", "manufacturer", "builtBy"),
            // Remove the new features/vertices (simulate deletions)
            UWGraph::RemoveVertex("energy_grid"),
            UWGraph::RemoveVertex("manufacturer"),
            UWGraph::RemoveVertex("nacelle"),
            UWGraph::RemoveVertex("tower"),
            UWGraph::RemoveVertex("blade"),
            UWGraph::RemoveVertex("rotor"),
            UWGraph::RemoveVertex("eg"),
            UWGraph::RemoveVertex("wt"),
        ];

        let config = EventGraphConfig {
            name: "wind_turbine_class_diagram",
            num_replicas: 16,
            num_operations: 100_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.4,
            reachability: None,
            compare: |a: &ClassDiagram, b: &ClassDiagram| petgraph::algo::is_isomorphic(a, b),
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<ClassDiagramCrdt>(config);
    }

    #[test_log::test]
    fn generate_graph_convergence() {
        let alphabet = ['a', 'b', 'c', 'd', 'e', 'f'];
        let mut names = Vec::new();

        // Generate combinations like "aa", "ab", ..., "ff" (36 total), then "aaa", ...
        for &c1 in &alphabet {
            for &c2 in &alphabet {
                names.push(format!("{}{}", c1, c2));
            }
        }
        for &c1 in &alphabet {
            for &c2 in &alphabet {
                for &c3 in &alphabet {
                    names.push(format!("{}{}{}", c1, c2, c3));
                }
            }
        }

        let mut ops: Vec<Graph<String, usize>> = Vec::new();
        let mut index = 0;

        // AddVertex and RemoveVertex: 15,000 of each
        while ops.len() < 15000 {
            let name = &names[index % names.len()];
            ops.push(Graph::AddVertex(name.clone()));
            ops.push(Graph::RemoveVertex(name.clone()));
            index += 1;
        }

        // AddArc and RemoveArc: 7,500 of each
        index = 0;
        while ops.len() < 30000 {
            let from = &names[index % names.len()];
            let to = &names[(index + 1) % names.len()];
            let weight1 = (index % 10) + 1;
            let weight2 = ((index + 5) % 10) + 1;

            ops.push(Graph::AddArc(from.clone(), to.clone(), weight1));
            ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight1));
            ops.push(Graph::AddArc(from.clone(), to.clone(), weight2));
            ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight2));

            index += 1;
        }

        let config = EventGraphConfig {
            name: "graph",
            num_replicas: 16,
            num_operations: 100_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.8,
            reachability: None,
            compare: |a: &DiGraph<String, usize>, b: &DiGraph<String, usize>| {
                vf2::isomorphisms(a, b).first().is_some()
            },
            record_results: false,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<Graph<String, usize>>>(config);
    }

    // #[test_log::test]
    // fn generate_uw_multigraph_convergence() {
    //     let ops = vec![
    //         UWGraph::UpdateVertex("a", LWWRegister::Write("vertex_a")),
    //         UWGraph::UpdateVertex("b", LWWRegister::Write("vertex_b")),
    //         UWGraph::UpdateVertex("c", LWWRegister::Write("vertex_c")),
    //         UWGraph::UpdateVertex("d", LWWRegister::Write("vertex_d")),
    //         UWGraph::UpdateVertex("e", LWWRegister::Write("vertex_e")),
    //         UWGraph::RemoveVertex("a"),
    //         UWGraph::RemoveVertex("b"),
    //         UWGraph::RemoveVertex("c"),
    //         UWGraph::RemoveVertex("d"),
    //         UWGraph::RemoveVertex("e"),
    //         UWGraph::UpdateArc("a", "b", 1, Counter::Inc(1)),
    //         UWGraph::UpdateArc("a", "a", 1, Counter::Inc(13)),
    //         UWGraph::UpdateArc("a", "a", 1, Counter::Dec(3)),
    //         UWGraph::UpdateArc("a", "b", 2, Counter::Dec(2)),
    //         UWGraph::UpdateArc("a", "b", 2, Counter::Inc(7)),
    //         UWGraph::UpdateArc("b", "c", 1, Counter::Dec(5)),
    //         UWGraph::UpdateArc("c", "d", 1, Counter::Inc(3)),
    //         UWGraph::UpdateArc("d", "e", 1, Counter::Dec(2)),
    //         UWGraph::UpdateArc("e", "a", 1, Counter::Inc(4)),
    //         UWGraph::RemoveArc("a", "b", 1),
    //         UWGraph::RemoveArc("a", "b", 2),
    //         UWGraph::RemoveArc("b", "c", 1),
    //         UWGraph::RemoveArc("c", "d", 1),
    //         UWGraph::RemoveArc("d", "e", 1),
    //         UWGraph::RemoveArc("e", "a", 1),
    //     ];

    //     let config = EventGraphConfig {
    //         n_replicas: 8,
    //         total_operations: 100,
    //         ops: &ops,
    //         final_sync: true,
    //         churn_rate: 0.2,
    //         ..Default::default()
    //     };

    //     let tcsbs = op_weaver::<
    //         UWGraphLog<&str, u8, EventGraph<LWWRegister<&str>>, EventGraph<Counter<i32>>>,
    //     >(config);

    //     // All replicas' eval() should match
    //     let mut reference_val: DiGraph<Content<&str, &str>, Content<(&str, &str, u8), i32>> =
    //         DiGraph::new();
    //     let mut event_sum = 0;
    //     for (i, tcsb) in tcsbs.iter().enumerate() {
    //         if i == 0 {
    //             reference_val = tcsb.eval();
    //             event_sum = tcsb.my_clock().sum();
    //         }
    //         let new_eval = tcsb.eval();
    //         assert_eq!(tcsb.my_clock().sum(), event_sum);
    //         assert!(
    //             vf2::isomorphisms(&new_eval, &reference_val)
    //                 .first()
    //                 .is_some(),
    //             "Replica {} did not converge with the reference. Reference: {}, Replica: {}",
    //             tcsb.id,
    //             petgraph::dot::Dot::with_config(&new_eval, &[]),
    //             petgraph::dot::Dot::with_config(&reference_val, &[]),
    //         );
    //         println!(
    //             "Replica {}: {}",
    //             tcsb.id,
    //             petgraph::dot::Dot::with_config(&new_eval, &[])
    //         );
    //     }
    // }

    // #[test_log::test]
    // fn generate_deeply_nested_uw_map_convergence() {
    //     let ops = vec![
    //         UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Inc(2))),
    //         UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Dec(3))),
    //         UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Reset)),
    //         UWMap::Update("a".to_string(), UWMap::Remove(1)),
    //         UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Inc(5))),
    //         UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Dec(1))),
    //         UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Reset)),
    //         UWMap::Update("b".to_string(), UWMap::Remove(2)),
    //         UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Inc(10))),
    //         UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Dec(2))),
    //         UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Reset)),
    //         UWMap::Update("c".to_string(), UWMap::Remove(3)),
    //         UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Inc(7))),
    //         UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Dec(4))),
    //         UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Reset)),
    //         UWMap::Update("d".to_string(), UWMap::Remove(4)),
    //         UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Inc(3))),
    //         UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Dec(1))),
    //         UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Reset)),
    //         UWMap::Update("e".to_string(), UWMap::Remove(5)),
    //         UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Inc(2))),
    //         UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Dec(2))),
    //         UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Reset)),
    //         UWMap::Update("a".to_string(), UWMap::Remove(6)),
    //     ];

    //     let config = EventGraphConfig {
    //         n_replicas: 5,
    //         total_operations: 40,
    //         ops: &ops,
    //         final_sync: true,
    //         churn_rate: 0.3,
    //         ..Default::default()
    //     };

    //     let tcsbs = op_weaver::<UWMapLog<String, UWMapLog<i32, EventGraph<Counter<i32>>>>>(
    //         config,
    //     );

    //     // All replicas' eval() should match
    //     let mut reference_val: HashMap<String, HashMap<i32, i32>> = HashMap::new();
    //     let mut event_sum = 0;
    //     for (i, tcsb) in tcsbs.iter().enumerate() {
    //         if i == 0 {
    //             reference_val = tcsb.eval();
    //             event_sum = tcsb.my_clock().sum();
    //         }
    //         assert_eq!(tcsb.my_clock().sum(), event_sum);
    //         assert_eq!(
    //             tcsb.eval(),
    //             reference_val,
    //             "Replica {} did not converge with the reference.",
    //             i,
    //         );
    //     }
    // }

    // #[test_log::test]
    // fn generate_lww_register_convergence() {
    //     let ops = vec![
    //         LWWRegister::Write("w".to_string()),
    //         LWWRegister::Write("x".to_string()),
    //         LWWRegister::Write("y".to_string()),
    //         LWWRegister::Write("z".to_string()),
    //         LWWRegister::Write("u".to_string()),
    //         LWWRegister::Write("v".to_string()),
    //     ];

    //     let config = EventGraphConfig {
    //         n_replicas: 5,
    //         total_operations: 50,
    //         ops: &ops,
    //         final_sync: true,
    //         churn_rate: 0.2,
    //         ..Default::default()
    //     };

    //     let tcsbs = op_weaver::<EventGraph<LWWRegister<String>>>(config);

    //     // All replicas' eval() should match
    //     let mut reference_val: String = String::new();
    //     let mut event_sum = 0;
    //     for (i, tcsb) in tcsbs.iter().enumerate() {
    //         if i == 0 {
    //             reference_val = tcsb.eval();
    //             event_sum = tcsb.my_clock().sum();
    //         }
    //         assert_eq!(tcsb.my_clock().sum(), event_sum);
    //         assert_ne!(tcsb.eval(), String::default());
    //         assert_eq!(
    //             tcsb.eval(),
    //             reference_val,
    //             "Replica '{}' did not converge with the reference '{}'. Ref val: '{}'",
    //             tcsb.id,
    //             tcsbs[0].id,
    //             reference_val
    //         );
    //     }
    // }
}
