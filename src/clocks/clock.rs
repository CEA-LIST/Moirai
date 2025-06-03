use core::panic;
#[cfg(feature = "utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::{min, Ordering},
    collections::{HashMap, HashSet},
    fmt::{Display, Error, Formatter},
    rc::Rc,
};
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::dot::Dot;
use crate::protocol::membership::ViewData;

// ===== Typestate Pattern ===== //

/// Marker trait for vector clock states
pub trait ClockState {}

/// Full vector clock (supports partial order comparisons)
#[derive(Debug, Clone)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct Full;

/// Partial vector clock (dependency clock, no ordering)
#[derive(Debug, Clone)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct Partial;

impl ClockState for Full {}
impl ClockState for Partial {}

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct Clock<S: ClockState> {
    pub(crate) view: Rc<ViewData>,
    /// The key is the index of the member in the members list
    /// The value is the version of the last event by this member, known by this version vector
    pub(crate) clock: HashMap<usize, usize>,
    /// The index of the origin member in the members list/clock
    pub(crate) origin: Option<usize>,
    _state: std::marker::PhantomData<S>,
}

impl Clock<Full> {
    pub fn new(view: &Rc<ViewData>, origin: Option<&str>) -> Self {
        let origin = origin.map(|o| {
            view.members
                .iter()
                .position(|m| m == o)
                .expect("Member not found")
        });
        Self {
            origin,
            view: Rc::clone(view),
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, 0))
                .collect(),
            _state: std::marker::PhantomData,
        }
    }
}

impl Clock<Partial> {
    pub fn new(view: &Rc<ViewData>, origin: &str) -> Self {
        assert!(view.members.contains(&origin.to_string()));
        let mut clock = HashMap::new();
        let origin_idx = view
            .members
            .iter()
            .position(|m| m == origin)
            .expect("Member not found");
        clock.insert(origin_idx, 0);
        Self {
            origin: Some(
                view.members
                    .iter()
                    .position(|m| m == origin)
                    .expect("Member not found"),
            ),
            view: Rc::clone(view),
            clock,
            _state: std::marker::PhantomData,
        }
    }
}

impl PartialEq for Clock<Full> {
    fn eq(&self, other: &Self) -> bool {
        self.clock == other.clock
    }
}

impl Eq for Clock<Full> {}
impl Eq for Clock<Partial> {}

impl PartialEq for Clock<Partial> {
    fn eq(&self, other: &Self) -> bool {
        self.clock == other.clock
    }
}

impl<S: ClockState> Clock<S> {
    pub fn view_id(&self) -> usize {
        self.view.id
    }

    pub fn iter(&self) -> impl Iterator<Item = (&usize, &usize)> {
        self.clock.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&usize, &mut usize)> {
        self.clock.iter_mut()
    }

    pub fn build(view: &Rc<ViewData>, origin: Option<&str>, clock: &[usize]) -> Clock<S> {
        assert_eq!(view.members.len(), clock.len());
        assert_eq!(clock.len(), view.members.len());
        let origin = origin.map(|o| view.member_pos(o).expect("Origin member not found"));
        let clock = view
            .members
            .iter()
            .enumerate()
            .map(|(i, _)| (i, clock[i]))
            .collect();
        Clock {
            view: Rc::clone(view),
            clock,
            origin,
            _state: std::marker::PhantomData,
        }
    }

    pub fn dot(&self) -> usize {
        self.get(self.origin()).unwrap()
    }

    pub fn merge<T: ClockState>(&mut self, other: &Clock<T>) {
        assert!(self.view.id == other.view.id);
        for (idx, m) in self.view.members.iter().enumerate() {
            if self.get(m) < other.get(m) {
                self.clock.insert(idx, other.get(m).unwrap());
            }
        }
    }

    pub fn increment(&mut self) -> usize {
        let idx = self.get(self.origin()).unwrap();
        self.clock
            .insert(self.origin.expect("Origin not set"), idx + 1);
        idx + 1
    }

    pub fn min(&self, other: &Self) -> Self {
        assert!(self.view.id == other.view.id);
        let mut new_clock = HashMap::new();
        for (idx, m) in self.view.members.iter().enumerate() {
            let self_val = self.get(m).unwrap_or(0);
            let other_val = other.get(m).unwrap_or(0);
            new_clock.insert(idx, min(self_val, other_val));
        }
        Self {
            view: self.view.clone(),
            clock: new_clock,
            origin: self.origin,
            _state: std::marker::PhantomData,
        }
    }

    pub fn remove(&mut self, member: &str) {
        let idx = self
            .view
            .members
            .iter()
            .position(|m| m == member)
            .expect("Member not found");
        self.clock.remove(&(idx));
    }

    pub fn dim(&self) -> usize {
        self.clock.len()
    }

    /// Returns the value of the clock for the given member or None if the member is not in the clock or the value is not set
    pub fn get(&self, member: &str) -> Option<usize> {
        let idx = self.view.members.iter().position(|m| m == member);
        if let Some(idx) = idx {
            if let Some(val) = self.clock.get(&idx) {
                return Some(*val);
            }
        }
        None
    }

    pub fn set(&mut self, member: &str, value: usize) {
        let idx = self
            .view
            .members
            .iter()
            .position(|m| m == member)
            .unwrap_or_else(|| panic!("Member {} not found", member));
        self.clock.insert(idx, value);
    }

    /// Can panic if the origin is not set
    pub fn origin(&self) -> &str {
        &self.view.members[self.origin.expect("Origin not set")]
    }

    pub fn sum(&self) -> usize {
        self.clock.values().sum()
    }
}

impl<S: ClockState> From<&Clock<S>> for Dot {
    fn from(clock: &Clock<S>) -> Dot {
        Dot::new(
            clock.origin.expect("Origin not set"),
            clock.get(clock.origin()).unwrap(),
            &Rc::clone(&clock.view),
        )
    }
}

impl<S: ClockState> From<Clock<S>> for HashMap<String, usize> {
    fn from(val: Clock<S>) -> Self {
        let mut id_counter = HashMap::new();
        for (idx, m) in val.view.members.iter().enumerate() {
            id_counter.insert(m.clone(), *val.clock.get(&(idx)).unwrap_or(&0));
        }
        id_counter
    }
}

impl<S: ClockState> Display for Clock<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{{ ")?;
        let mut first = true;
        for (idx, m) in self.view.members.iter().enumerate() {
            if let Some(val) = self.clock.get(&idx) {
                if first {
                    write!(f, "{}: {}", m, val)?;
                    first = false;
                } else {
                    write!(f, ", {}: {}", m, val)?;
                }
            }
        }
        write!(f, " }}")?;
        if let Some(origin) = self.origin {
            write!(f, "@{}", self.view.members[origin])?;
        }
        Ok(())
    }
}

impl PartialOrd for Clock<Full> {
    /// Will PANIC if the clocks are not vector clocks
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.view.id.cmp(&other.view.id) {
            Ordering::Less => return Some(Ordering::Less),
            Ordering::Greater => return Some(Ordering::Greater),
            _ => {}
        };

        if self.clock.keys().collect::<HashSet<_>>() != other.clock.keys().collect::<HashSet<_>>() {
            panic!("Clocks must behave like vector clocks to be comparable.");
        }

        if self.get(self.origin()) == other.get(other.origin()) && self.origin == other.origin {
            return Some(Ordering::Equal);
        }

        if self.get(self.origin()) <= other.get(self.origin()) {
            return Some(Ordering::Less);
        }

        if other.get(other.origin()) <= self.get(other.origin()) {
            return Some(Ordering::Greater);
        }

        let mut less = false;
        let mut greater = false;

        for m in self.view.members.iter() {
            let self_val = self.get(m).unwrap();
            let other_val = other.get(m).unwrap();

            match self_val.cmp(&other_val) {
                Ordering::Less => less = true,
                Ordering::Greater => greater = true,
                _ => (),
            }

            // If both less and greater are true, the clocks are concurrent
            if less && greater {
                return None;
            }
        }

        if less {
            Some(Ordering::Less)
        } else if greater {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Equal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::membership::{View, ViewStatus};

    #[test_log::test]
    fn concurrent_clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = Clock::<Full>::new(&rc, Some("a"));
        let mut v2 = Clock::<Full>::new(&rc, Some("b"));
        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), None);
    }

    #[test_log::test]
    fn shortcut_clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = Clock::<Full>::new(&rc, Some("a"));
        let mut v2 = Clock::<Full>::new(&rc, Some("b"));
        v1.increment();
        v1.increment();
        v2.merge(&v1);

        assert_eq!(v1.clock, v2.clock);

        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
    }

    #[test_log::test]
    fn same_clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = Clock::<Full>::new(&rc, Some("a"));
        let mut v2 = Clock::<Full>::new(&rc, Some("a"));
        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Equal));
    }

    #[test_log::test]
    fn same_origin_clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = Clock::<Full>::new(&rc, Some("a"));
        let mut v2 = Clock::<Full>::new(&rc, Some("a"));
        v1.increment();
        v2.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
    }

    #[test_log::test]
    fn clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = Clock::<Full>::new(&rc, Some("a"));
        let mut v2 = Clock::<Full>::new(&rc, Some("b"));

        v1.increment();
        v1.increment();
        v2.increment();
        v2.increment();
        v2.increment();

        assert_eq!(v1.get("a").unwrap(), 2);
        assert_eq!(v1.get("b").unwrap(), 0);
        assert_eq!(v1.get("c").unwrap(), 0);

        assert_eq!(v2.get("a").unwrap(), 0);
        assert_eq!(v2.get("b").unwrap(), 3);
        assert_eq!(v2.get("c").unwrap(), 0);

        v1.merge(&v2);
        assert_eq!(v1.get("a").unwrap(), 2);
        assert_eq!(v1.get("b").unwrap(), 3);
        assert_eq!(v1.get("c").unwrap(), 0);

        v2.merge(&v1);
        assert_eq!(v2.get("a").unwrap(), 2);
        assert_eq!(v2.get("b").unwrap(), 3);
        assert_eq!(v2.get("c").unwrap(), 0);
    }

    #[test_log::test]
    fn display() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let v1 = Clock::<Full>::new(&rc, Some("a"));
        assert_eq!(format!("{}", v1), "{ a: 0, b: 0, c: 0 }@a");
    }
}
