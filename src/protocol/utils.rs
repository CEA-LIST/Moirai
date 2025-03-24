use std::{
    fmt::Display,
    hash::Hash,
    ops::{Add, AddAssign},
};

pub trait Incrementable<C> = Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Display;
pub trait Keyable = Ord + PartialOrd + Hash + Eq + Default + Display;
