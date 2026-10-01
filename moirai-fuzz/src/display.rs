use std::{cmp::Reverse, time::Duration};

use comfy_table::{
    Attribute, Cell, CellAlignment, Color, ContentArrangement, Table, presets::UTF8_FULL,
};
use readable::byte::Byte;

use crate::{
    FootprintNode,
    config::{CallSampling, MetricsSampling, ReplicaSampling, RunConfig},
    fuzzer::{ExecutionReport, ExecutionSummary, RunReport},
    utils::format::{format_string_ellipsis, seed_to_hex},
};

/// Presentation is separate from execution; callers can run with `without_display()`.
pub fn display_execution_report(report: &ExecutionReport) {
    for run in &report.runs {
        println!(
            "{}",
            display_config_table(&run.results.config, report.metadata.final_merge)
        );
        for table in display_run_results(run.run_number, &run.results) {
            println!("{table}");
        }
    }
    if let Some(summary) = &report.summary {
        println!("{}", display_summary(summary));
    }
}

pub fn display_config_table(run_config: &RunConfig, final_merge: bool) -> Table {
    let mut config_table = Table::new();
    config_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Fuzzer Configuration")
                .add_attribute(Attribute::Bold)
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Center),
        ]);

    config_table.add_row(vec![
        Cell::new("Parameter").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    config_table.add_row(vec!["Replicas", &run_config.num_replicas.to_string()]);

    config_table.add_row(vec![
        "Operations",
        &group_decimal_digits(&run_config.num_operations.to_string()),
    ]);

    config_table.add_row(vec![
        "P(Offline | Online)",
        &format!("{:.2}%", run_config.churn.p_disconnect.val() * 100.0),
    ]);

    config_table.add_row(vec![
        "P(Online | Offline)",
        &format!("{:.2}%", run_config.churn.p_reconnect.val() * 100.0),
    ]);

    config_table.add_row(vec!["Final merge", if final_merge { "Yes" } else { "No" }]);

    if let Some(seed) = run_config.seed {
        config_table.add_row(vec!["Seed", &seed_to_hex(&seed)]);
    } else {
        config_table.add_row(vec!["Seed", "Random"]);
    }

    for (metric, sampling, basis) in [
        (
            "Response time sampling",
            &run_config.metrics.response_time,
            "communication calls",
        ),
        (
            "Effect time sampling",
            &run_config.metrics.effect_time,
            "effects",
        ),
        (
            "Query time sampling",
            &run_config.metrics.query_time,
            "queries",
        ),
        ("Log size sampling", &run_config.metrics.log_size, "effects"),
        (
            "Replica size sampling",
            &run_config.metrics.replica_size,
            "communication calls",
        ),
    ] {
        config_table.add_row(vec![
            metric.to_owned(),
            format_metrics_sampling(sampling.as_ref(), basis),
        ]);
    }
    config_table.add_row(vec![
        "Stability progress sampling".to_owned(),
        run_config.metrics.track_stability.as_ref().map_or_else(
            || "Disabled".to_owned(),
            |replicas| {
                format!(
                    "Every frontier advance; {}",
                    format_replica_sampling(replicas)
                )
            },
        ),
    ]);

    config_table
}

fn format_metrics_sampling(sampling: Option<&MetricsSampling>, basis: &str) -> String {
    let Some(sampling) = sampling else {
        return "Disabled".to_owned();
    };
    let frequency = match &sampling.calls {
        CallSampling::All => format!("All {basis}"),
        CallSampling::Every { interval } if interval.get() == 1 => format!("All {basis}"),
        CallSampling::Every { interval } => {
            format!("Every {} {basis}", format_count(interval.get()))
        }
    };
    format!(
        "{frequency}; {}",
        format_replica_sampling(&sampling.replicas)
    )
}

fn format_replica_sampling(sampling: &ReplicaSampling) -> String {
    match sampling {
        ReplicaSampling::All => "all replicas".to_owned(),
        ReplicaSampling::Count(count) => format!(
            "{} randomly selected replica{}",
            format_count(count.get()),
            if count.get() == 1 { "" } else { "s" },
        ),
        ReplicaSampling::Indices(indices) => format!(
            "replicas [{}]",
            indices
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

/// Display summary statistics across all runs
pub fn display_summary(_execution_summary: &ExecutionSummary) -> Table {
    let mut summary_table = Table::new();
    summary_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Execution Summary")
                .add_attribute(Attribute::Bold)
                .fg(Color::Magenta)
                .set_alignment(CellAlignment::Center),
        ]);

    summary_table.add_row(vec![
        "Runs",
        &group_decimal_digits(&_execution_summary.total_runs.to_string()),
    ]);

    summary_table
}

pub fn display_run_results(run_number: usize, results: &RunReport) -> Vec<Table> {
    let mut tables = Vec::with_capacity(results.measurements.len() + 1);
    tables.push(run_results_table(run_number, results));
    tables.extend(
        results
            .measurements
            .iter()
            .enumerate()
            .filter(|(_, stats)| has_displayable_metrics(stats))
            .map(|(replica_idx, stats)| replica_metrics_table(replica_idx, stats)),
    );
    tables
}

fn has_displayable_metrics(stats: &crate::fuzzer::ReplicaStats) -> bool {
    stats.stability_progress_count.is_some()
        || stats.avg_events_between_stability.is_some()
        || stats.avg_effect_time.is_some()
        || stats.p99_effect_time.is_some()
        || stats.total_effect_time.is_some()
        || stats.avg_response_time.is_some()
        || stats.p99_response_time.is_some()
        || stats.total_response_time.is_some()
        || stats.throughput_calls_per_sec.is_some()
        || stats.response_sample_count.is_some()
        || stats.effect_sample_count.is_some()
        || stats.query_sample_count.is_some()
        || stats.replica_size_sample_count.is_some()
        || stats.log_size_sample_count.is_some()
        || stats.avg_query_time.is_some()
        || stats.p99_query_time.is_some()
        || stats.sampled_peak_replica_size.is_some()
        || stats.final_replica_size.is_some()
        || stats.sampled_peak_log_size.is_some()
        || stats.final_log_size.is_some()
}

fn run_results_table(run_number: usize, results: &RunReport) -> Table {
    let mut results_table = Table::new();
    results_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new(format!(
                "Run {} Results",
                group_decimal_digits(&run_number.to_string())
            ))
            .add_attribute(Attribute::Bold)
            .fg(Color::Green)
            .set_alignment(CellAlignment::Center),
            Cell::new("Value").add_attribute(Attribute::Bold),
        ]);

    if let Some(val) = &results.final_value {
        results_table.add_row(vec!["Final value", &format_string_ellipsis(val, Some(120))]);
    }

    results_table.add_row(vec![
        "Seed",
        &results
            .config
            .seed
            .as_ref()
            .map_or_else(not_available, seed_to_hex),
    ]);
    if !results.run_stats.is_redundant() {
        results_table.add_row(vec![
            "Average response time (per sample)",
            &format_optional_duration(results.run_stats.avg_response_time),
        ]);
        results_table.add_row(vec![
            "Response samples",
            &format_count(results.run_stats.response_sample_count),
        ]);
        results_table.add_row(vec![
            "Average effect time (per sample)",
            &format_optional_duration(results.run_stats.avg_effect_time),
        ]);
        results_table.add_row(vec![
            "Effect samples",
            &format_count(results.run_stats.effect_sample_count),
        ]);
        results_table.add_row(vec![
            "Average response time (per replica)",
            &format_optional_duration(results.run_stats.avg_replica_response_time),
        ]);
        results_table.add_row(vec![
            "Replicas with response samples",
            &format_count(results.run_stats.response_replica_count),
        ]);
        results_table.add_row(vec![
            "Average effect time (per replica)",
            &format_optional_duration(results.run_stats.avg_replica_effect_time),
        ]);
        results_table.add_row(vec![
            "Replicas with effect samples",
            &format_count(results.run_stats.effect_replica_count),
        ]);
    }

    if let Some(ratio) = results.inter_replica_concurrency_ratio {
        results_table.add_row(vec![
            "Inter-replica concurrency ratio",
            &format!("{ratio:.4}"),
        ]);
    }
    if let Some(average) = results.avg_concurrent_updates {
        results_table.add_row(vec![
            "Average concurrency per update",
            &format_optional_decimal(Some(average)),
        ]);
    }

    results_table
}

fn replica_metrics_table(replica_idx: usize, stats: &crate::fuzzer::ReplicaStats) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new(format!("Replica {replica_idx}"))
                .add_attribute(Attribute::Bold)
                .fg(Color::Cyan),
            Cell::new("Metric").add_attribute(Attribute::Bold),
            Cell::new("Value")
                .add_attribute(Attribute::Bold)
                .set_alignment(CellAlignment::Right),
        ]);

    add_metric_row(
        &mut table,
        "Stability",
        "Progress count",
        format_optional_count(stats.stability_progress_count),
    );
    add_metric_row(
        &mut table,
        "Stability",
        "Average events between progress",
        format_optional_decimal(stats.avg_events_between_stability),
    );

    for (category, count) in [
        ("Response", stats.response_sample_count),
        ("Effect", stats.effect_sample_count),
        ("Query", stats.query_sample_count),
        ("Replica size", stats.replica_size_sample_count),
        ("Log size", stats.log_size_sample_count),
    ] {
        if let Some(count) = count {
            add_metric_row(&mut table, category, "Samples", format_count(count));
        }
    }

    add_metric_row(
        &mut table,
        "Effect",
        "Average time",
        format_optional_duration(stats.avg_effect_time),
    );
    add_metric_row(
        &mut table,
        "Effect",
        "P99 time",
        format_optional_duration(stats.p99_effect_time),
    );
    add_metric_row(
        &mut table,
        "Effect",
        "Total time",
        format_optional_duration(stats.total_effect_time),
    );

    add_metric_row(
        &mut table,
        "Response",
        "Average time",
        format_optional_duration(stats.avg_response_time),
    );
    add_metric_row(
        &mut table,
        "Response",
        "P99 time",
        format_optional_duration(stats.p99_response_time),
    );
    add_metric_row(
        &mut table,
        "Response",
        "Total time",
        format_optional_duration(stats.total_response_time),
    );
    add_metric_row(
        &mut table,
        "Response",
        "Throughput",
        stats
            .throughput_calls_per_sec
            .map_or_else(not_available, |throughput| {
                format!(
                    "{} calls/s",
                    group_decimal_digits(&format!("{throughput:.2}"))
                )
            }),
    );

    add_metric_row(
        &mut table,
        "Query",
        "Average time",
        format_optional_duration(stats.avg_query_time),
    );
    add_metric_row(
        &mut table,
        "Query",
        "P99 time",
        format_optional_duration(stats.p99_query_time),
    );

    add_metric_row(
        &mut table,
        "Replica size",
        "Sampled peak",
        format_optional_bytes(stats.sampled_peak_replica_size),
    );
    add_metric_row(
        &mut table,
        "Replica size",
        "Final",
        format_optional_bytes(stats.final_replica_size),
    );
    add_tree_metric_row(
        &mut table,
        "Log size",
        "Sampled peak tree",
        format_optional_footprint(stats.sampled_peak_log_size.as_ref()),
    );
    add_tree_metric_row(
        &mut table,
        "Log size",
        "Final tree",
        format_optional_footprint(stats.final_log_size.as_ref()),
    );

    table
}

fn add_metric_row(table: &mut Table, category: &str, metric: &str, value: String) {
    table.add_row(vec![
        Cell::new(category),
        Cell::new(metric),
        Cell::new(value).set_alignment(CellAlignment::Right),
    ]);
}

fn add_tree_metric_row(table: &mut Table, category: &str, metric: &str, value: String) {
    table.add_row(vec![
        Cell::new(category),
        Cell::new(metric),
        Cell::new(value).set_alignment(CellAlignment::Left),
    ]);
}

fn format_optional_duration(duration: Option<Duration>) -> String {
    duration.map_or_else(not_available, format_duration)
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds >= 1.0 {
        format!("{seconds:.3} s")
    } else if seconds >= 1e-3 {
        format!("{:.3} ms", seconds * 1e3)
    } else if seconds >= 1e-6 {
        format!("{:.3} µs", seconds * 1e6)
    } else {
        format!("{} ns", duration.as_nanos())
    }
}

fn format_optional_count(value: Option<usize>) -> String {
    value.map_or_else(not_available, format_count)
}

fn format_count(value: usize) -> String {
    group_decimal_digits(&value.to_string())
}

fn format_optional_decimal(value: Option<f64>) -> String {
    value.map_or_else(not_available, |value| format!("{value:.2}"))
}

fn format_optional_bytes(bytes: Option<usize>) -> String {
    bytes.map_or_else(not_available, |bytes| Byte::from(bytes).to_string())
}

fn format_optional_footprint(footprint: Option<&FootprintNode>) -> String {
    footprint.map_or_else(not_available, format_footprint)
}

fn format_footprint(root: &FootprintNode) -> String {
    let mut formatter = FootprintFormatter {
        lines: vec![format_footprint_label(root)],
    };
    formatter.children(&root.children, "", 1, 0);
    formatter.lines.join("\n")
}

const FOOTPRINT_MAX_CHILDREN: usize = 5;
const FOOTPRINT_MAX_LINES: usize = 40;
const FOOTPRINT_MAX_DEPTH: usize = 8;

/// A presentation-only view: show each node's largest children and summarize
/// omitted siblings without traversing their descendants or rewriting the tree.
struct FootprintFormatter {
    lines: Vec<String>,
}

impl FootprintFormatter {
    fn children(
        &mut self,
        children: &[FootprintNode],
        prefix: &str,
        depth: usize,
        reserved_lines: usize,
    ) {
        let limit = if depth > FOOTPRINT_MAX_DEPTH {
            0
        } else {
            children.len().min(FOOTPRINT_MAX_CHILDREN)
        };
        let mut ranked = children.iter().collect::<Vec<_>>();
        if limit > 0 {
            // Stable ties retain tree order. Sort references, not the captured tree.
            ranked.sort_by_key(|child| Reverse(child.inclusive_bytes));
        }
        let mut shown = 0;
        for (index, child) in ranked.iter().take(limit).enumerate() {
            let is_last = index + 1 == children.len();
            // Reserve a summary for remaining siblings and for each ancestor
            // that still has siblings. A non-leaf also needs at least one line
            // for its children, even if they must all be summarized.
            let reserved = reserved_lines + usize::from(!is_last);
            let minimum_lines = 1 + usize::from(!child.children.is_empty());
            if self.lines.len() + reserved + minimum_lines > FOOTPRINT_MAX_LINES {
                break;
            }

            let connector = if is_last { "└── " } else { "├── " };
            self.lines.push(format!(
                "{prefix}{connector}{}",
                format_footprint_label(child)
            ));
            let child_prefix = format!("{prefix}{}", if is_last { "    " } else { "│   " });
            self.children(&child.children, &child_prefix, depth + 1, reserved);
            shown += 1;
        }

        if shown < children.len() {
            let omitted = &ranked[shown..];
            // These are inclusive footprints: adding descendants again would
            // double-count them. Use u128 so the display sum cannot overflow usize.
            let bytes = omitted
                .iter()
                .map(|child| child.inclusive_bytes as u128)
                .sum();
            let reason = if depth > FOOTPRINT_MAX_DEPTH {
                " (depth limit)"
            } else if shown < limit {
                " (line limit)"
            } else {
                ""
            };
            debug_assert!(self.lines.len() + reserved_lines < FOOTPRINT_MAX_LINES);
            self.lines.push(format!(
                "{prefix}└── … {} more {} — Σ {}{reason}",
                format_count(omitted.len()),
                if omitted.len() == 1 {
                    "child"
                } else {
                    "children"
                },
                format_footprint_bytes(bytes),
            ));
        }
    }
}

fn format_footprint_bytes(bytes: u128) -> String {
    match u64::try_from(bytes) {
        Ok(bytes) => Byte::from(bytes).to_string(),
        Err(_) => format!("{} B", group_decimal_digits(&bytes.to_string())),
    }
}

fn format_footprint_label(node: &FootprintNode) -> String {
    format!(
        "{} — {}",
        concise_type_name(&node.name),
        Byte::from(node.inclusive_bytes)
    )
}

pub(crate) fn concise_type_name(type_name: &str) -> String {
    let mut result = String::with_capacity(type_name.len());
    let mut token = String::new();

    let flush_token = |result: &mut String, token: &mut String| {
        if token.is_empty() {
            return;
        }
        result.push_str(token.rsplit("::").next().unwrap_or(token));
        token.clear();
    };

    for character in type_name.chars() {
        if character.is_alphanumeric() || matches!(character, '_' | ':') {
            token.push(character);
        } else {
            flush_token(&mut result, &mut token);
            result.push(character);
        }
    }
    flush_token(&mut result, &mut token);

    result
}

fn not_available() -> String {
    "—".to_string()
}

fn group_decimal_digits(value: &str) -> String {
    let (integer, fractional) = value.split_once('.').unwrap_or((value, ""));
    if !integer.bytes().all(|digit| digit.is_ascii_digit()) {
        return value.to_string();
    }

    let mut result = String::with_capacity(value.len() + integer.len() / 3);
    for (index, digit) in integer.chars().enumerate() {
        if index > 0 && (integer.len() - index).is_multiple_of(3) {
            result.push(' ');
        }
        result.push(digit);
    }
    if !fractional.is_empty() {
        result.push('.');
        result.push_str(fractional);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroUsize};

    use super::*;
    use crate::{
        config::ChurnConfig,
        fuzzer::{ReplicaStats, RunStats},
    };

    fn report(response_replicas: usize, effect_replicas: usize) -> RunReport {
        let mut replica = ReplicaStats::new();
        replica.avg_response_time = Some(Duration::from_nanos(100));
        RunReport {
            config: RunConfig::new(ChurnConfig::new(0.4, 0.6), 4, 10, None, Some([0; 32])),
            final_value: None,
            run_stats: RunStats {
                response_replica_count: response_replicas,
                effect_replica_count: effect_replicas,
                avg_response_time: Some(Duration::from_nanos(100)),
                ..RunStats::default()
            },
            measurements: vec![replica],
            recorders: None,
            execution_graph_dot: None,
            inter_replica_concurrency_ratio: Some(0.5),
            avg_concurrent_updates: Some(3.5),
        }
    }

    fn rows(table: &Table) -> BTreeMap<String, String> {
        table
            .row_iter()
            .filter_map(|row| {
                let mut cells = row.cell_iter();
                Some((cells.next()?.content(), cells.next()?.content()))
            })
            .collect()
    }

    fn footprint(name: &str, bytes: usize, children: Vec<FootprintNode>) -> FootprintNode {
        FootprintNode {
            name: name.to_owned(),
            inclusive_bytes: bytes,
            children,
        }
    }

    #[test]
    fn footprint_tree_sorts_small_trees_and_preserves_connectors() {
        let root = footprint(
            "Root",
            300,
            vec![
                footprint("A", 100, vec![footprint("A1", 50, vec![])]),
                footprint("B", 200, vec![]),
            ],
        );
        assert_eq!(
            format_footprint(&root),
            [
                format_footprint_label(&root),
                format!("├── {}", format_footprint_label(&root.children[1])),
                format!("└── {}", format_footprint_label(&root.children[0])),
                format!(
                    "    └── {}",
                    format_footprint_label(&root.children[0].children[0])
                ),
            ]
            .join("\n")
        );
        assert_eq!(
            format_footprint(&footprint("Leaf", 80, vec![])),
            "Leaf — 80 B"
        );
    }

    #[test]
    fn footprint_tree_displays_largest_children_at_each_parent_and_summarizes_the_rest() {
        let mut children = (0..7)
            .map(|index| footprint(&format!("Branch{index}"), 1_000 + index, vec![]))
            .collect::<Vec<_>>();
        children[6].children = (0..7)
            .map(|index| footprint(&format!("Leaf{index}"), (index + 1) * 10, vec![]))
            .collect();
        // Descendant bytes must not be added to the omitted parent's inclusive size.
        children[0].children = vec![footprint("HiddenDescendant", 900, vec![])];
        let root = footprint("Root", 9_000, children);
        let original = serde_json::to_value(&root).unwrap();
        let rendered = format_footprint(&root);
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("Branch"))
                .count(),
            5
        );
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("Leaf"))
                .count(),
            5
        );
        assert!(rendered.contains(&format!(
            "│   └── … 2 more children — Σ {}",
            format_footprint_bytes(30)
        )));
        assert!(rendered.ends_with(&format!(
            "└── … 2 more children — Σ {}",
            format_footprint_bytes(2_001)
        )));
        let expected_branches = (2..7)
            .rev()
            .map(|index| format!("├── {}", format_footprint_label(&root.children[index])))
            .collect::<Vec<_>>();
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("Branch"))
                .collect::<Vec<_>>(),
            expected_branches
        );
        let expected_leaves = (2..7)
            .rev()
            .map(|index| {
                format!(
                    "│   ├── {}",
                    format_footprint_label(&root.children[6].children[index])
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("Leaf"))
                .collect::<Vec<_>>(),
            expected_leaves
        );
        assert!(!rendered.contains("Branch0"));
        assert!(!rendered.contains("Branch1"));
        assert!(!rendered.contains("Leaf0"));
        assert!(!rendered.contains("Leaf1"));
        assert!(!rendered.contains("HiddenDescendant"));
        assert_eq!(serde_json::to_value(&root).unwrap(), original);
    }

    #[test]
    fn footprint_tree_keeps_original_order_for_equal_sizes() {
        let root = footprint(
            "Root",
            400,
            vec![
                footprint("First", 50, vec![]),
                footprint("Second", 50, vec![]),
                footprint("Largest", 70, vec![]),
                footprint("Third", 50, vec![]),
                footprint("Fourth", 50, vec![]),
                footprint("Fifth", 50, vec![]),
                footprint("Smallest", 10, vec![]),
            ],
        );
        let rendered = format_footprint(&root);
        let expected = [2, 0, 1, 3, 4]
            .map(|index| format!("├── {}", format_footprint_label(&root.children[index])));
        assert_eq!(
            rendered.lines().skip(1).take(5).collect::<Vec<_>>(),
            expected
        );
        assert!(rendered.ends_with("└── … 2 more children — Σ 60 B"));
    }

    #[test]
    fn footprint_tree_displays_repeated_types_individually_and_handles_one_omission() {
        let root = footprint(
            "Container",
            100,
            (0..6).map(|_| footprint("Child", 10, vec![])).collect(),
        );
        let rendered = format_footprint(&root);
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("Child —"))
                .count(),
            5
        );
        assert!(rendered.ends_with("└── … 1 more child — Σ 10 B"));

        let exact = footprint("Container", 100, root.children[..5].to_vec());
        let rendered = format_footprint(&exact);
        assert_eq!(rendered.lines().count(), 6);
        assert!(!rendered.contains('…'));
        assert!(rendered.ends_with("└── Child — 10 B"));
    }

    #[test]
    fn footprint_tree_depth_limit_keeps_an_omission_count_and_size() {
        let mut root = footprint("Bottom", 10, vec![]);
        for depth in (0..10).rev() {
            root = footprint(&format!("Level{depth}"), 100 + depth, vec![root]);
        }
        let rendered = format_footprint(&root);
        assert!(rendered.contains("Level8 —"));
        assert!(!rendered.contains("Level9 —"));
        assert!(rendered.ends_with(&format!(
            "… 1 more child — Σ {} (depth limit)",
            format_footprint_bytes(109)
        )));
    }

    #[test]
    fn footprint_tree_line_limit_reserves_summaries_for_omitted_siblings() {
        fn wide_tree(depth: usize) -> FootprintNode {
            let children = if depth == 0 {
                vec![]
            } else {
                (0..5).map(|_| wide_tree(depth - 1)).collect()
            };
            let bytes = 1 + children
                .iter()
                .map(|child| child.inclusive_bytes)
                .sum::<usize>();
            footprint("Node", bytes, children)
        }
        let root = wide_tree(4);
        let rendered = format_footprint(&root);
        assert!(rendered.lines().count() <= FOOTPRINT_MAX_LINES);
        assert!(rendered.contains("(line limit)"));
        assert!(rendered.lines().last().unwrap().starts_with("└── … "));
        assert!(
            rendered
                .lines()
                .last()
                .unwrap()
                .contains("more children — Σ")
        );
    }

    #[test]
    fn footprint_tree_summary_handles_sizes_larger_than_usize() {
        let root = footprint(
            "Root",
            usize::MAX,
            (0..7)
                .map(|_| footprint("Child", usize::MAX, vec![]))
                .collect(),
        );
        assert!(format_footprint(&root).ends_with(&format!(
            "└── … 2 more children — Σ {}",
            format_footprint_bytes(2 * usize::MAX as u128)
        )));
    }

    #[test]
    fn configuration_table_displays_metric_frequencies_and_replica_selection() {
        let config = RunConfig::new(ChurnConfig::new(0.4, 0.6), 4, 10, None, None)
            .with_response_time(MetricsSampling::all())
            .with_effect_time(MetricsSampling::new(
                CallSampling::Every {
                    interval: NonZeroUsize::new(5).unwrap(),
                },
                ReplicaSampling::Indices(vec![0, 3]),
            ))
            .with_query_time(MetricsSampling::new(
                CallSampling::Every {
                    interval: NonZeroUsize::new(1).unwrap(),
                },
                ReplicaSampling::Count(NonZeroUsize::new(1).unwrap()),
            ))
            .with_replica_size(MetricsSampling::every_replica(
                NonZeroUsize::new(2).unwrap(),
            ))
            .with_track_stability(ReplicaSampling::Indices(vec![0]));
        let mut table = display_config_table(&config, true);
        let rows = rows(&table);
        assert_eq!(
            rows["Response time sampling"],
            "All communication calls; all replicas"
        );
        assert_eq!(
            rows["Effect time sampling"],
            "Every 5 effects; replicas [0, 3]"
        );
        assert_eq!(
            rows["Query time sampling"],
            "All queries; 1 randomly selected replica"
        );
        assert_eq!(rows["Log size sampling"], "Disabled");
        assert_eq!(
            rows["Replica size sampling"],
            "All communication calls; 2 randomly selected replicas"
        );
        assert_eq!(
            rows["Stability progress sampling"],
            "Every frontier advance; replicas [0]"
        );
        table.set_width(140);
        let rendered = table.to_string();
        for (metric, value) in rows
            .into_iter()
            .filter(|(metric, _)| metric.ends_with("sampling"))
        {
            assert!(rendered.contains(&metric));
            assert!(rendered.contains(&value));
        }
    }

    #[test]
    fn configuration_table_displays_disabled_metrics_and_log_sampling_axis() {
        let config = RunConfig::new(ChurnConfig::new(0.4, 0.6), 4, 10, None, None);
        let config_rows = rows(&display_config_table(&config, true));
        let metrics: Vec<_> = config_rows
            .iter()
            .filter(|(metric, _)| metric.ends_with("sampling"))
            .collect();
        assert_eq!(metrics.len(), 6);
        assert!(metrics.iter().all(|(_, value)| *value == "Disabled"));

        let config = config
            .with_log_size(MetricsSampling::every_call(NonZeroUsize::new(3).unwrap()))
            .with_track_stability(ReplicaSampling::All);
        let config_rows = rows(&display_config_table(&config, true));
        assert_eq!(
            config_rows["Log size sampling"],
            "Every 3 effects; all replicas"
        );
        assert_eq!(
            config_rows["Stability progress sampling"],
            "Every frontier advance; all replicas"
        );
    }

    #[test]
    fn final_value_is_plain_text_and_only_truncated_for_display() {
        for value in [
            r#"{"hzog": {89}, "crx": {0}, "xhzg": {83}}"#.to_owned(),
            "é🙂".repeat(100),
        ] {
            let mut report = report(0, 0);
            report.final_value = Some(value.clone());
            let displayed = rows(&run_results_table(1, &report));
            assert_eq!(
                displayed["Final value"],
                format_string_ellipsis(&value, Some(120))
            );
            assert_eq!(report.final_value, Some(value));
        }
    }

    #[test]
    fn single_replica_sampling_hides_run_stats_but_keeps_replica_and_concurrency_metrics() {
        for replica_count in [0, 1] {
            let tables = display_run_results(1, &report(replica_count, replica_count));
            let run = tables[0].to_string();
            assert!(!run.contains("Average response time (per sample)"));
            assert!(!run.contains("Average effect time (per sample)"));
            assert!(!run.contains("Replicas with response samples"));
            assert!(run.contains("Average concurrent updates per update"));
            assert!(run.contains("3.50"));
            assert!(run.contains("Inter-replica concurrency ratio"));
            assert_eq!(tables.len(), 2);
            assert!(tables[1].to_string().contains("Replica 0"));
        }
    }

    #[test]
    fn multi_replica_sampling_displays_run_stats() {
        for (responses, effects) in [(2, 0), (0, 2), (1, 2)] {
            let table = run_results_table(1, &report(responses, effects)).to_string();
            assert!(table.contains("Average response time (per sample)"));
            assert!(table.contains("Average effect time (per sample)"));
            assert!(table.contains("Replicas with response samples"));
        }
    }
}
