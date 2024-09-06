// static ALLOC: dhat::Alloc = dhat::Alloc;

// #[test]
// fn test() {
//     let _profiler = dhat::Profiler::builder().testing().build();

//     let _v1 = vec![1, 2, 3, 4];
//     let v2 = vec![5, 6, 7, 8];
//     drop(v2);
//     let v3 = vec![9, 10, 11, 12];
//     drop(v3);

//     let stats = dhat::HeapStats::get();

//     // Three allocations were done in total.
//     dhat::assert_eq!(stats.total_blocks, 3);
//     dhat::assert_eq!(stats.total_bytes, 48);

//     // At the point of peak heap size, two allocations totalling 32 bytes existed.
//     dhat::assert_eq!(stats.max_blocks, 2);
//     dhat::assert_eq!(stats.max_bytes, 32);

//     // Now a single allocation remains alive.
//     dhat::assert_eq!(stats.curr_blocks, 1);
//     dhat::assert_eq!(stats.curr_bytes, 16);
// }
