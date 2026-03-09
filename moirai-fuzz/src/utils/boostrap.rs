use std::fmt::Debug;

use moirai_protocol::{
    broadcast::tcsb::IsTcsb,
    replica::{IsReplica, Replica},
    state::log::IsLog,
};

pub fn bootstrap_n<L, T>(n: u8) -> Vec<Replica<L, T>>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    let mut replicas = Vec::new();
    for i in 0..n {
        let id = i.to_string();
        let replica = Replica::<L, T>::bootstrap(
            id,
            &(0..n)
                .map(|j| j.to_string())
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        replicas.push(replica);
    }
    replicas
}
