use super::tracer::Tracer;
use anyhow::Result;
use camino::Utf8Path;
use graphviz_rust::cmd::Format;
use graphviz_rust::exec;
use graphviz_rust::parse;
use graphviz_rust::printer::PrinterContext;
use std::cmp::Ordering;
use std::fs::write;

pub fn tracer_to_graphviz(tracer: &Tracer) -> String {
    let mut graphviz_str = String::new();
    graphviz_str.push_str("strict digraph G {splines=polyline;");
    graphviz_str.push_str("node [shape=box];");
    let mut graph: Vec<(usize, usize)> = vec![];
    for (i, event) in tracer.trace.iter().enumerate() {
        graphviz_str.push_str(&format!(
            "  {} [label=<<FONT FACE=\"monospace\" POINT-SIZE=\"8\">({})</FONT> <B>{}</B>: {}<BR /><I><FONT FACE=\"monospace\" POINT-SIZE=\"10\">{}</FONT></I>>{}];",
            i,
            i + 1,
            event.metadata.origin,
            event.metadata.clock,
            event.op.replace("\"", ""),
            if event.metadata.origin == tracer.origin {
                "style=filled, fillcolor=lightblue"
            } else {
                "style=filled, fillcolor=lightgray"
            },
        ));
        for (j, previous_event) in tracer.trace.iter().enumerate() {
            if j < i {
                match previous_event
                    .metadata
                    .clock
                    .partial_cmp(&event.metadata.clock)
                {
                    Some(Ordering::Less) => {
                        graph.push((j, i));
                    }
                    Some(Ordering::Greater) => {
                        graph.push((i, j));
                    }
                    _ => {}
                }
            }
        }
    }
    transitive_reduction(&mut graph);
    for (i, j) in graph {
        graphviz_str.push_str(&format!("{} -> {};", i, j));
    }
    graphviz_str.push('}');
    graphviz_str
}

pub fn graphviz_str_to_svg(graphviz_str: &str, path: &Utf8Path) -> Result<()> {
    let g = parse(graphviz_str).map_err(|e| anyhow::anyhow!(e))?;
    let graph_svg = exec(g, &mut PrinterContext::default(), vec![Format::Svg.into()])?;
    write(path, graph_svg)?;
    Ok(())
}

fn transitive_reduction(graph: &mut Vec<(usize, usize)>) {
    let mut matrix = vec![vec![false; graph.len()]; graph.len()];
    for (i, j) in graph.iter() {
        matrix[*i][*j] = true;
    }
    for k in 0..graph.len() {
        for i in 0..graph.len() {
            for j in 0..graph.len() {
                if matrix[i][k] && matrix[k][j] {
                    matrix[i][j] = false;
                }
            }
        }
    }
    graph.clear();
    for i in 0..matrix.len() {
        for j in 0..matrix.len() {
            if matrix[i][j] {
                graph.push((i, j));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf;

    use super::*;

    #[test_log::test]
    fn evict_multiple_msg_trace() {
        let tracer = Tracer::deserialize_from_file(&Utf8PathBuf::from(
            "traces/membership_evict_multiple_msg_b_trace.json",
        ))
        .unwrap();
        let graphviz_str = tracer_to_graphviz(&tracer);
        let res = graphviz_str_to_svg(
            &graphviz_str,
            &Utf8PathBuf::from("traces/membership_evict_multiple_msg_b_trace.svg"),
        );
        assert!(res.is_ok());
    }

    #[test_log::test]
    fn evict_full_scenario() {
        let tracer = Tracer::deserialize_from_file(&Utf8PathBuf::from(
            "traces/membership_evict_full_scenario.json",
        ))
        .unwrap();
        let graphviz_str = tracer_to_graphviz(&tracer);
        let res = graphviz_str_to_svg(
            &graphviz_str,
            &Utf8PathBuf::from("traces/membership_evict_full_scenario.svg"),
        );
        assert!(res.is_ok());
    }
}
