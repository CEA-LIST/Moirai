pub mod clocks;
#[cfg(feature = "crdt")]
pub mod crdt;
pub mod macros;
pub mod protocol;
#[cfg(feature = "utils")]
pub mod utils;

// mod patate {
//     use crate::{
//         crdt::lww_register::LWWRegister, object, patate, protocol::event_graph::EventGraph,
//     };

//     pub type Name = EventGraph<LWWRegister<String>>;
//     pub type Age = EventGraph<LWWRegister<u32>>;

//     fn test() {
//         object!(Patate {
//             name: EventGraph::<LWWRegister<String>>,
//             age: EventGraph::<LWWRegister<u32>>,
//         });

//         let test = Patate::Age(LWWRegister::Write(123));
//     }
// }
