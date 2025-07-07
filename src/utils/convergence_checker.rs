use crate::{
    crdt::test_util::n_members,
    protocol::{event::Event, log::Log},
};

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

/// Generate all sequential permutations of a given set of operations and check if they converge to the expected value.
/// The number of operations must be less than or equal to 15 to avoid overflow.
/// This function is useful for testing convergence properties of CRDTs but is factorial in complexity,
pub fn convergence_checker<L: Log>(
    ops: &[L::Op],
    value: L::Value,
    cmp: fn(&L::Value, &L::Value) -> bool,
) {
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
    // Each replica make one operation
    for (i, op) in ops.iter().enumerate() {
        let event = tcsbs[i].tc_bcast(op.clone());
        to_deliver.push(event);
    }

    // Each replica delivers the events from all other replicas
    // in all possible orders
    for (i, perm) in permutations.iter().enumerate() {
        for seq in perm {
            if i != *seq {
                let event = to_deliver[*seq].clone();
                tcsbs[i].try_deliver(event.clone());
            }
        }
        assert_eq!(tcsbs[i].my_clock().sum(), ops.len());
        assert!(
            cmp(&tcsbs[i].eval(), &value),
            "Convergence check failed for sequence {:?}",
            permutations[i]
                .iter()
                .map(|x| ops[*x].clone())
                .collect::<Vec<_>>()
        );
    }
}
