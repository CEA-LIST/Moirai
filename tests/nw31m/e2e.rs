//! NW.31m end to end (vault spec/items/NW.31m.md): the generated path. The
//! built moirai-netstack-router on 127.0.0.1 port 0 and three nodes of the
//! crate arachne-cli generates from examples/json.ecore (NW.31j's node,
//! tests/nw31j/node.rs, built by NW.31j's gate at NW.31ka's 693dcb2) in Zenoh
//! peer mode, told only the router. NW.31j's driver is reused whole: the gate
//! copies tests/nw31j/e2e.rs without its inner doc lines into
//! src/nw31j_e2e.rs, and module `j` includes it, so its helpers and phase
//! prefixes ("NW31J <phase>:") are this driver's too.
//!
//! Ground truth: the states written below by hand from the ops posted (a JSON
//! object of counters), read back over GET /api/state. Phases: "mesh",
//! "first" (each node writes one key, all converge), "one-sided" (b pauses a
//! over HTTP and a does not pause b; a, b and c each write; c serves every
//! write), "resume" (b resumes a over HTTP; every node serves every write
//! within 30 s, then a writes once more and all converge). "set-up:" is a
//! failure of the instrument, never a pass. Last line on success: NW31M E2E OK.
//!
//! Environment: NW31M_ROUTER_BIN, NW31M_NODE_BIN, NW31M_DIR (a scratch folder).

#[allow(dead_code)]
mod j {
    include!("nw31j_e2e.rs");

    pub fn one_sided() {
        let (router_bin, node_bin, dir) = (
            var("NW31M_ROUTER_BIN"),
            var("NW31M_NODE_BIN"),
            var("NW31M_DIR"),
        );
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("set-up: {}: {e}", dir.display()));
        let mut router = spawn(&router_bin, &dir, "router", &[]);
        let (router_port, _) = router.report();
        if router_port == 0 {
            panic!("set-up: the router reported port 0");
        }
        let endpoint = format!("tcp/127.0.0.1:{router_port}");
        println!("NW31M router pid {} port {router_port}", router.child.id());
        let ids = ["a", "b", "c"];
        let mut procs: Vec<Proc> = ids
            .iter()
            .map(|id| {
                let env = [
                    ("REPLICA_ID", id.to_string()),
                    ("HTTP_PORT", "0".to_string()),
                    ("HTTP_BIND_ADDR", "127.0.0.1".to_string()),
                    ("LOG_ID", LOG_ID.to_string()),
                    ("MOIRAI_TRANSPORT", "zenoh".to_string()),
                    ("ZENOH_ROUTER", endpoint.clone()),
                ];
                spawn(&node_bin, &dir, id, &env)
            })
            .collect();
        let http_ports: Vec<u16> = procs.iter_mut().map(|p| p.report().1).collect();
        let nodes: Vec<(&str, u16)> = ids
            .iter()
            .copied()
            .zip(http_ports.iter().copied())
            .collect();
        let [a, b, c] = [nodes[0].1, nodes[1].1, nodes[2].1];

        peers_are("mesh", &nodes, &ids, "Connected");
        post_op("first", a, "a", 1.0);
        post_op("first", b, "b", 2.0);
        post_op("first", c, "c", 3.0);
        converge(
            "first",
            &nodes,
            &object(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]),
        );

        control("one-sided", b, "/api/pause/a", "Paused peer 'a'");
        post_op("one-sided", a, "a", 10.0);
        post_op("one-sided", b, "b", 20.0);
        post_op("one-sided", c, "c", 30.0);
        let second = object(&[("a", 11.0), ("b", 22.0), ("c", 33.0)]);
        converge("one-sided", &nodes[2..], &second);

        let message = control("resume", b, "/api/resume/a", "Resumed peer 'a', delivered ");
        println!("NW31M resume a on b: {message}");
        converge("resume", &nodes, &second);
        post_op("resume", a, "shared", 1.0);
        let third = object(&[("a", 11.0), ("b", 22.0), ("c", 33.0), ("shared", 1.0)]);
        converge("resume", &nodes, &third);

        for p in procs.iter_mut() {
            p.alive("end");
        }
        router.alive("end");
        println!("NW31M E2E OK");
    }
}

fn main() {
    j::one_sided();
}
