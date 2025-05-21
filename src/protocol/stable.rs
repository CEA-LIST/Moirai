use super::pure_crdt::PureCRDT;
use std::fmt::Debug;

pub trait Stable<O>: Default + Clone + Debug {
    fn is_default(&self) -> bool;

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn apply_redundant(&mut self, rdnt: fn(&O, bool, &O) -> bool, op: &O);

    fn apply(&mut self, value: O);
}

impl<O: PureCRDT> Stable<O> for Vec<O> {
    fn is_default(&self) -> bool {
        self.is_empty()
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn apply(&mut self, value: O) {
        self.push(value);
    }

    fn apply_redundant(&mut self, rdnt: fn(&O, bool, &O) -> bool, op: &O) {
        self.retain(|o| !(rdnt(o, false, op)));
    }
}

// macro_rules! impl_stable_for_nums {
//     ($($t:ty),+) => {
//         $(
//             impl Stable for $t {
//                 fn is_default(&self) -> bool {
//                     *self == Self::default()
//                 }

//                 fn clear(&mut self) {
//                     *self = Self::default();
//                 }
//             }
//         )+
//     };
// }

// impl_stable_for_nums!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

// impl<V> Stable for HashSet<V>
// where
//     V: PartialEq + Eq + Hash + Clone,
// {
//     fn is_default(&self) -> bool {
//         self.is_empty()
//     }

//     fn clear(&mut self) {
//         self.clear();
//     }

//     fn apply_to(&self, value: &mut Self) {
//         self.insert(value)
//     }
// }
