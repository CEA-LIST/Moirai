use comfy_table::{
    presets::UTF8_FULL, Attribute, Cell, CellAlignment, Color, ContentArrangement, Table,
};
use readable::num::Int;

use crate::fuzz::{
    config::RunConfig,
    fuzzer::{ExecutionSummary, RunResults},
};

pub fn display_config_table(run_config: &RunConfig, final_merge: bool) -> Table {
    let mut config_table = Table::new();
    config_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Fuzzer Configuration")
            .add_attribute(Attribute::Bold)
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Center)]);

    config_table.add_row(vec![
        Cell::new("Parameter").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    config_table.add_row(vec![
        "Replicas",
        &format!("{}", Int::from(run_config.num_replicas)),
    ]);

    config_table.add_row(vec![
        "Operations",
        &format!("{}", Int::from(run_config.num_operations as i32)),
    ]);

    config_table.add_row(vec![
        "Churn rate",
        &format!("{:.1}%", run_config.churn_rate * 100.0),
    ]);

    config_table.add_row(vec!["Final merge", if final_merge { "Yes" } else { "No" }]);

    if let Some(seed) = run_config.seed {
        config_table.add_row(vec![
            "Seed",
            &format!(
                "0x{:016X}",
                u64::from_le_bytes(seed[0..8].try_into().unwrap())
            ),
        ]);
    } else {
        config_table.add_row(vec!["Seed", "Random"]);
    }

    config_table
}

/// Display summary statistics across all runs
pub fn display_summary(_execution_summary: &ExecutionSummary) -> Table {
    let mut summary_table = Table::new();
    summary_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Execution Summary")
            .add_attribute(Attribute::Bold)
            .fg(Color::Magenta)
            .set_alignment(CellAlignment::Center)]);

    // summary_table.add_row(vec![]);

    summary_table
}

pub fn display_run_results(run_number: usize, results: &RunResults) -> Table {
    let mut results_table = Table::new();
    results_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new(format!("Run {run_number} Results"))
            .add_attribute(Attribute::Bold)
            .fg(Color::Green)
            .set_alignment(CellAlignment::Center)]);

    results_table.add_row(vec![
        Cell::new("Metric").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    results_table.add_row(vec!["Final state", &results.final_state]);

    results_table.add_row(vec![
        "Avg time per op (ms)",
        &format!("{:.3}", results.avg_time_per_op_ms),
    ]);

    results_table.add_row(vec![
        "Avg throughput (ops/sec)",
        &format!(
            "{}",
            Int::from(results.avg_throughput_ops_per_sec.floor() as i64)
        ),
    ]);

    results_table.add_row(vec![
        "Total deliver time per replica (ms)",
        &format!(
            "[{}]",
            results
                .total_deliver_ms_per_replica
                .iter()
                .map(|t| format!("{}", t))
                .collect::<Vec<String>>()
                .join(", ")
        ),
    ]);

    results_table.add_row(vec![
        "Total effect time per replica (ms)",
        &format!(
            "[{}]",
            results
                .total_effect_ms_per_replica
                .iter()
                .map(|t| format!("{}", t))
                .collect::<Vec<String>>()
                .join(", ")
        ),
    ]);

    results_table.add_row(vec![
        "Avg effect time (ms)",
        &format!("{}", results.avg_effect_ms),
    ]);

    results_table
}
