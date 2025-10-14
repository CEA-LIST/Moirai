// use std::{fmt::Display};

// use crate::{
//     protocol::membership::view::View,
//     utils::mut_owner::{MutOwner, Reader},
// };

// pub mod view;

pub type ReplicaId = str;
// pub type ReplicaIdOwned = tinystr::TinyAsciiStr<8>;
pub type ReplicaIdOwned = String;
pub type ReplicaIdx = usize;

// #[derive(Debug, Clone)]
// pub struct Membership {
//     mapping: HashMap<ReplicaId, MutOwner<View>>,
// }

// impl Membership {
//     pub fn new(id: &ReplicaId) -> Self {
//         let view = View::new(id);
//         let mapping = HashMap::from([(id.clone(), MutOwner::new(view))]);
//         Self { mapping }
//     }

//     pub fn build(mapping: HashMap<ReplicaId, MutOwner<View>>) -> Self {
//         Self { mapping }
//     }

//     pub fn get(&self, id: &ReplicaId) -> Option<&MutOwner<View>> {
//         self.mapping.get(id)
//     }

//     pub fn get_reader(&self, id: &ReplicaId) -> Option<Reader<View>> {
//         self.mapping.get(id).map(|v| v.as_reader())
//     }
// }

// impl Display for Membership {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         for (id, view) in &self.mapping {
//             writeln!(f, "{} => {}", id, view.as_reader())?;
//         }
//         Ok(())
//     }
// }
