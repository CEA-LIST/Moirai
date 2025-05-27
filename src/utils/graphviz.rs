use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::File,
    io::Write,
    process::{Command, Stdio},
};

use anyhow::Result;

use super::tracer::Tracer;
use crate::clocks::clock::Clock;

pub fn tracer_to_graphviz(tracer: &Tracer, name: &str) -> String {
    let mut graphviz_str = String::new();
    graphviz_str.push_str("strict digraph G {\n");
    graphviz_str.push_str(&format!("label=\"{}\"\n", name));
    graphviz_str.push_str("splines=polyline;\n");
    graphviz_str.push_str("node [shape=box];\n");
    graphviz_str.push_str("newrank=true;\n");
    graphviz_str.push_str("fontname=\"Helvetica\"\n");
    let mut graph: Vec<(usize, usize)> = vec![];
    let mut process = HashMap::<String, Vec<usize>>::new();
    for (i, event) in tracer.trace.iter().enumerate() {
        graphviz_str.push_str(&format!(
            "{} [label=<<FONT FACE=\"monospace\" POINT-SIZE=\"8\">({})</FONT>   {}<BR /><I><FONT FACE=\"monospace\" POINT-SIZE=\"10\">{}</FONT></I>>style=filled, fillcolor=\"lightblue\"];\n",
            i,
            i + 1,
            event.metadata,
            event.op.replace("\"", ""),
        ));
        if process.contains_key(event.metadata.origin()) {
            let list = process.get_mut(event.metadata.origin()).unwrap();
            list.push(i);
        } else {
            process.insert(event.metadata.origin().to_string(), vec![i]);
        }
        for (j, previous_event) in tracer.trace.iter().enumerate() {
            if j < i {
                match previous_event
                    .metadata
                    .view_id()
                    .cmp(&event.metadata.view_id())
                {
                    Ordering::Less => {
                        graph.push((j, i));
                    }
                    Ordering::Greater => {
                        graph.push((i, j));
                    }
                    // TODO: partial_cmp is not safe
                    Ordering::Equal => match previous_event.metadata.partial_cmp(&event.metadata) {
                        Some(Ordering::Less) => {
                            graph.push((j, i));
                        }
                        Some(Ordering::Greater) => {
                            graph.push((i, j));
                        }
                        _ => {}
                    },
                }
            }
        }
    }
    for (id, list) in &process {
        graphviz_str.push_str(&format!("subgraph cluster_{} {{\n", id));
        graphviz_str.push_str("style=filled;\n");
        graphviz_str.push_str("color=lightgrey;\n");
        graphviz_str.push_str("node [style=filled,color=white];\n");
        graphviz_str.push_str(&format!("start_{}", id));
        for (i, e) in list.iter().enumerate() {
            if i == list.len() - 1 {
                if i == 0 && list.len() == 1 {
                    graphviz_str.push_str(&format!("-> {} -> end_{}[style=\"dashed\"];\n", e, id));
                } else {
                    graphviz_str.push_str(&format!("{} -> end_{}[style=\"dashed\"];\n", e, id));
                }
            } else if i == 0 {
                graphviz_str.push_str(&format!(" -> {} -> ", e));
            } else {
                graphviz_str.push_str(&format!("{} -> ", e));
            }
        }
        graphviz_str.push_str(&format!(
            "label = <<FONT FACE=\"monospace\"><B>{}</B></FONT>>;\n",
            id
        ));
        graphviz_str.push_str("}\n");
    }
    transitive_reduction(&mut graph);
    for (i, j) in graph {
        graphviz_str.push_str(&format!("{} -> {};\n", i, j));
    }
    graphviz_str.push_str("\n{rank=same;");
    for proc in process.keys() {
        graphviz_str.push_str(&format!(
            "start_{}[shape=point;style=filled;color=black];",
            proc
        ));
    }
    graphviz_str.push_str("}\n");
    graphviz_str.push_str("\n{rank=same;");
    for proc in process.keys() {
        graphviz_str.push_str(&format!(
            "end_{}[shape=point;style=filled;color=black];",
            proc
        ));
    }
    graphviz_str.push_str("}\n");
    graphviz_str.push_str("}\n");
    graphviz_str
}

pub fn generate_svg(dot_source: &str, output_path: &str) -> Result<()> {
    // Spawn the dot process
    let mut process = Command::new("dot")
        .arg("-Tsvg") // Output SVG format
        .stdin(Stdio::piped()) // Provide input via stdin
        .stdout(Stdio::piped()) // Capture output via stdout
        .spawn()?;

    // Write the DOT input to the stdin of the process
    if let Some(mut stdin) = process.stdin.take() {
        stdin.write_all(dot_source.as_bytes())?;
    }

    // Capture the output
    let output = process.wait_with_output()?;

    // Save the output to the specified file
    let mut file = File::create(output_path)?;
    file.write_all(&output.stdout)?;

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
#[cfg(feature = "serde")]
mod tests {
    use std::path::Path;

    use super::*;

    fn trace_to_file(name: &str) -> Result<()> {
        let tracer = Tracer::deserialize_from_file(Path::new(&format!("traces/{}.json", name)));
        match tracer {
            Ok(tracer) => {
                let graphviz_str = tracer_to_graphviz(&tracer, name);
                generate_svg(&graphviz_str, &format!("traces/{}.svg", name))
            }
            Err(_) => Ok(()),
        }
    }

    #[test_log::test]
    fn aw_set_a() {
        let name = "aw_set_a";
        let res = trace_to_file(name);
        assert!(res.is_ok(), "{:?}", res);
    }

    #[test_log::test]
    fn membership() {
        let name = "membership";
        let res = trace_to_file(name);
        assert!(res.is_ok(), "{:?}", res);
    }

    #[test_log::test]
    fn convergence() {
        let name = "convergence";
        let res = trace_to_file(name);
        assert!(res.is_ok(), "{:?}", res);
    }

    #[test_log::test]
    fn random_event_graph() {
        let name = "random_event_graph";
        let res = trace_to_file(name);
        assert!(res.is_ok(), "{:?}", res);
    }
}
