use std::hash::Hash;
use std::ops::Add;
use std::ops::AddAssign;

pub trait Incrementable<C> = Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default;
pub trait Keyable = Ord + PartialOrd + Hash + Eq + Default;
