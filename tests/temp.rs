// use moirai::{
//     crdt::set::{aw_set::AWSet, ewflag_set::EWFlagSet},
//     fuzz::{
//         config::{FuzzerConfig, RunConfig},
//         fuzzer::fuzzer,
//     },
//     protocol::state::po_log::VecLog,
// };

// #[test]
// fn fuzz_ewflag_set() {
//     // let run_1 = RunConfig::new(0.7, 16, 1_000, None, None, false, true);
//     // let run_2 = RunConfig::new(0.7, 16, 3_000, None, None, false, true);
//     let run_3 = RunConfig::new(0.7, 16, 10_000, None, None, false, true);
//     // let run_4 = RunConfig::new(0.7, 16, 30_000, None, None, false, true);
//     // let run_5 = RunConfig::new(0.7, 16, 100_000, None, None, false, true);
//     // let run_6 = RunConfig::new(0.7, 16, 300_000, None, None, false, true);
//     // let run_7 = RunConfig::new(0.7, 16, 1_000_000, None, None, false, true);
//     // let run_8 = RunConfig::new(0.7, 16, 3_000_000, None, None, false, true);
//     // let runs = vec![run_1, run_2, run_3, run_4, run_5, run_6, run_7, run_8];
//     let runs = vec![run_3];

//     let config =
//         FuzzerConfig::<EWFlagSet<usize>>::new("ew_flag_set", runs, true, |a, b| a == b, true);

//     fuzzer::<EWFlagSet<usize>>(config);
// }

// #[test]
// fn fuzz_aw_set() {
//     // let run_1 = RunConfig::new(0.7, 16, 1_000, None, None, false, true);
//     // let run_2 = RunConfig::new(0.7, 16, 3_000, None, None, false, true);
//     let run_3 = RunConfig::new(0.7, 16, 10_000, None, None, false, true);
//     // let run_4 = RunConfig::new(0.7, 16, 30_000, None, None, false, true);
//     // let run_5 = RunConfig::new(0.7, 16, 100_000, None, None, false, true);
//     // let run_6 = RunConfig::new(0.7, 16, 300_000, None, None, false, true);
//     // let run_7 = RunConfig::new(0.7, 16, 1_000_000, None, None, false, true);
//     // let run_8 = RunConfig::new(0.7, 16, 3_000_000, None, None, false, true);
//     // let runs = vec![run_1, run_2, run_3, run_4, run_5, run_6, run_7, run_8];
//     let runs = vec![run_3];

//     let config =
//         FuzzerConfig::<VecLog<AWSet<usize>>>::new("aw_set", runs, true, |a, b| a == b, true);

//     fuzzer::<VecLog<AWSet<usize>>>(config);
// }
