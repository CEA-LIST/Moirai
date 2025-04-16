use crate::{
    clocks::clock::Clock,
    crdt::test_util::n_members,
    protocol::{event::Event, log::Log},
};
use std::fmt::Debug;

fn factorial(number: usize) -> usize {
    let mut factorial: usize = 1;
    for i in 1..(number + 1) {
        factorial *= i;
    }
    factorial
}

fn generate_permutations(n: usize) -> Vec<Vec<usize>> {
    let indices: Vec<usize> = (0..n).collect();
    permute(&indices)
}

fn permute(indices: &[usize]) -> Vec<Vec<usize>> {
    if indices.len() == 1 {
        return vec![indices.to_vec()];
    }

    let mut result = Vec::new();

    for i in 0..indices.len() {
        let mut rest = indices.to_vec();
        let current = rest.remove(i);
        for mut p in permute(&rest) {
            let mut permutation = vec![current];
            permutation.append(&mut p);
            result.push(permutation);
        }
    }
    result
}

pub fn convergence_checker<L: Log>(ops: &[L::Op], value: L::Value)
where
    L::Value: PartialEq + Debug,
{
    assert!(
        ops.len() <= 15,
        "The number of operations must be less than or equal to 15 to avoid overflow"
    );
    assert!(
        !ops.is_empty(),
        "The number of operations must be greater than 0"
    );
    let fac = factorial(ops.len());
    let mut tcsbs = n_members::<L>(fac);
    let permutations = generate_permutations(ops.len());
    assert!(permutations.len() == fac);

    let mut to_deliver: Vec<Event<L::Op>> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let event = tcsbs[i].tc_bcast(op.clone());
        to_deliver.push(event);
    }

    for (i, perm) in permutations.iter().enumerate() {
        for seq in perm {
            if i != *seq {
                let event = to_deliver[*seq].clone();
                tcsbs[i].try_deliver(event.clone());
            }
        }
    }

    for i in 0..tcsbs.len() {
        if i == tcsbs.len() - 1 {
            break;
        }
        if value != tcsbs[i].eval() {
            #[cfg(feature = "utils")]
            tcsbs[i]
                .tracer
                .serialize_to_file(std::path::Path::new("traces/convergence.json"))
                .unwrap();
            assert_eq!(tcsbs[i].eval(), value);
        }
        assert_eq!(tcsbs[i].my_clock().sum(), ops.len());
        assert_eq!(tcsbs[i].my_clock().sum(), tcsbs[i + 1].my_clock().sum());
        assert_eq!(tcsbs[i].eval(), tcsbs[i + 1].eval());
    }
}
