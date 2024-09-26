use super::tracer::Tracer;
use anyhow::Result;
use graphviz_rust::cmd::Format;
use graphviz_rust::exec;
use graphviz_rust::parse;
use graphviz_rust::printer::PrinterContext;
use std::cmp::Ordering;
use std::fs::write;
use std::path::Path;

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
            event.metadata.vc,
            event.op.replace("\"", ""),
            if event.metadata.origin == tracer.origin {
                "style=filled, fillcolor=lightblue"
            } else {
                "style=filled, fillcolor=lightgray"
            },
        ));
        for (j, previous_event) in tracer.trace.iter().enumerate() {
            if j < i {
                match previous_event.metadata.vc.partial_cmp(&event.metadata.vc) {
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

pub fn graphviz_str_to_svg(graphviz_str: &str, path: &Path) -> Result<()> {
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
    use std::path::PathBuf;

    use super::*;

    #[test_log::test]
    fn membership_evict_trace() {
        let tracer =
            Tracer::deserialize_from_file(&PathBuf::from("membership_evict_a_trace.json")).unwrap();
        let graphviz_str = tracer_to_graphviz(&tracer);
        let res = graphviz_str_to_svg(
            &graphviz_str,
            &PathBuf::from("membership_evict_a_trace.svg"),
        );
        assert!(res.is_ok());
    }

    #[test_log::test]
    fn concurrent_aw_set_trace() {
        let tracer =
            Tracer::deserialize_from_file(&PathBuf::from("concurrent_aw_set_a_trace.json"))
                .unwrap();
        let graphviz_str = tracer_to_graphviz(&tracer);
        let res = graphviz_str_to_svg(
            &graphviz_str,
            &PathBuf::from("concurrent_aw_set_a_trace.svg"),
        );
        assert!(res.is_ok());
    }

    #[test_log::test]
    fn evict_multiple_msg_trace() {
        let tracer = Tracer::deserialize_from_file(&PathBuf::from(
            "membership_evict_multiple_msg_b_trace.json",
        ))
        .unwrap();
        let graphviz_str = tracer_to_graphviz(&tracer);
        let res = graphviz_str_to_svg(
            &graphviz_str,
            &PathBuf::from("membership_evict_multiple_msg_b_trace.svg"),
        );
        assert!(res.is_ok());
    }
}
