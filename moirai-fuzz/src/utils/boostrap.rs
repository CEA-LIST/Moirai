use std::fmt::Debug;

use moirai_protocol::{
    broadcast::tcsb::IsTcsb,
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::log::IsLog,
};

pub fn bootstrap_n<L, T>(n: u8) -> Vec<Replica<L, T>>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    // One id for the whole cohort: replicas that each minted their own would
    // host n different logs and refuse everything the fuzzer exchanges.
    let log_id = LogId::generate();
    let mut replicas = Vec::new();
    for i in 0..n {
        let id = i.to_string();
        let replica = Replica::<L, T>::bootstrap_with_log_id(
            id,
            &(0..n)
                .map(|j| j.to_string())
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            log_id.clone(),
        );
        replicas.push(replica);
    }
    replicas
}
