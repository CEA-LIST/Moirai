pub mod clocks;
#[cfg(feature = "crdt")]
pub mod crdt;
// pub mod macros;
pub mod protocol;
#[cfg(feature = "utils")]
pub mod utils;

// crdt_object!({
//     name: crdt::lww_register::LWWRegister<String>,
//     bag_content: crdt::aw_set::AWSet<String>
// });

// make_struct!(foo_bar);
