// fn alphabet_string_combination(alphabet: &str, length: usize) -> String {
//     let mut result = String::new();
//     for i in 0..length {
//         result.push(alphabet.chars().nth(i % alphabet.len()).unwrap());
//     }
//     result
// }

fn main() {
    // let mut graph = petgraph::graph::Graph::<String, ()>::new();
    // let mut node_indexes = Vec::new();
    // let alphabet = "abcdefghijklmnopqrstuvwxyz";

    // let max = 1_000_000;

    // for i in 0..max {
    //     node_indexes
    //         .push(graph.add_node(alphabet_string_combination(alphabet, 3 + (i % 26) as usize)));
    // }

    // for i in 0..max {
    //     let from = node_indexes[i % max];
    //     let to = node_indexes[(i + 1) % max];
    //     graph.add_edge(from, to, ());
    // }

    // for i in 0..max {
    //     let idx = node_indexes[i % max];
    //     graph.remove_node(idx);
    //     node_indexes.remove(i);
    // }

    // graph.shrink_to_fit();

    // println!(
    //     "Graph has {} nodes and {} edges",
    //     graph.node_count(),
    //     graph.edge_count()
    // );
    // graph capacity
    // println!("Graph has capacity of {:?} nodes", graph.capacity());
    // node indexes size
    // println!("Node indexes size: {}", node_indexes.len());

    // get edges of graph into an interator
    // recreate the graph to shrink it
    // let mut new_graph = petgraph::stable_graph::StableDiGraph::<String, ()>::new();

    // for i in 0..max {
    //     node_indexes
    //         .push(new_graph.add_node(alphabet_string_combination(alphabet, 3 + (i % 26) as usize)));
    // }

    // drop(graph);
    // drop(node_indexes);

    // fill the graph with

    // println!(
    //     "New graph has {} nodes and {} edges",
    //     new_graph.node_count(),
    //     new_graph.edge_count()
    // );
    // println!("New graph has capacity of {:?} nodes", new_graph.capacity());
}
