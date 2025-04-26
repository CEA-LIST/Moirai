use std::{
    cmp::{min, Ordering},
    collections::HashMap,
    fmt::{Display, Error, Formatter},
    rc::Rc,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tsify::Tsify;

use super::{clock::Clock, dot::Dot};
use crate::protocol::membership::ViewData;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]

pub struct DependencyClock {
    pub(crate) view: Rc<ViewData>,
    /// The key is the index of the member in the members list
    /// The value is the version of the last event by this member, known by this version vector
    pub(crate) clock: HashMap<usize, usize>,
    /// The index of the origin member in the members list/clock
    pub(crate) origin: Option<usize>,
}

impl DependencyClock {
    pub fn view_id(&self) -> usize {
        self.view.id
    }

    pub fn new_originless(view: &Rc<ViewData>) -> Self {
        Self {
            origin: None,
            view: Rc::clone(view),
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, 0))
                .collect(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&usize, &usize)> {
        self.clock.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&usize, &mut usize)> {
        self.clock.iter_mut()
    }

    pub fn build(view: &Rc<ViewData>, origin: Option<&str>, clock: &[usize]) -> DependencyClock {
        assert_eq!(view.members.len(), clock.len());
        let origin = origin.map(|o| {
            view.members
                .iter()
                .position(|m| m == o)
                .expect("Member not found")
        });
        let clock = view
            .members
            .iter()
            .enumerate()
            .map(|(i, _)| (i, clock[i]))
            .collect();
        DependencyClock {
            view: Rc::clone(view),
            clock,
            origin,
        }
    }
}

impl From<&DependencyClock> for Dot {
    fn from(clock: &DependencyClock) -> Dot {
        Dot::new(
            clock.origin.expect("Origin not set"),
            clock.get(clock.origin()).unwrap(),
            &Rc::clone(&clock.view),
        )
    }
}

impl Clock for DependencyClock {
    fn new(view: &Rc<ViewData>, origin: &str) -> Self {
        assert!(view.members.contains(&origin.to_string()));
        Self {
            origin: Some(
                view.members
                    .iter()
                    .position(|m| m == origin)
                    .expect("Member not found"),
            ),
            view: Rc::clone(view),
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, 0))
                .collect(),
        }
    }

    fn dot(&self) -> usize {
        self.get(self.origin()).unwrap()
    }

    fn merge(&mut self, other: &Self) {
        assert!(self.view.id == other.view.id);
        for (idx, m) in self.view.members.iter().enumerate() {
            if self.get(m) < other.get(m) {
                self.clock.insert(idx, other.get(m).unwrap());
            }
        }
    }

    fn increment(&mut self) {
        let idx = self.get(self.origin()).unwrap();
        self.clock
            .insert(self.origin.expect("Origin not set"), idx + 1);
    }

    fn min(&self, other: &Self) -> Self {
        assert!(self.view.id == other.view.id);
        let mut new_clock = HashMap::new();
        for (idx, m) in self.view.members.iter().enumerate() {
            new_clock.insert(idx, min(self.get(m).unwrap(), other.get(m).unwrap()));
        }
        Self {
            view: self.view.clone(),
            clock: new_clock,
            origin: self.origin,
        }
    }

    fn remove(&mut self, member: &str) {
        let idx = self
            .view
            .members
            .iter()
            .position(|m| m == member)
            .expect("Member not found");
        self.clock.remove(&(idx));
    }

    fn dim(&self) -> usize {
        self.clock.len()
    }

    /// Returns the value of the clock for the given member OR 0 if the member is not in the clock
    fn get(&self, member: &str) -> Option<usize> {
        let idx = self.view.members.iter().position(|m| m == member);
        if let Some(idx) = idx {
            if let Some(val) = self.clock.get(&idx) {
                return Some(*val);
            }
        }
        None
    }

    fn set(&mut self, member: &str, value: usize) {
        let idx = self
            .view
            .members
            .iter()
            .position(|m| m == member)
            .unwrap_or_else(|| panic!("Member {} not found", member));
        self.clock.insert(idx, value);
    }

    fn origin(&self) -> &str {
        &self.view.members[self.origin.expect("Origin not set")]
    }

    fn sum(&self) -> usize {
        self.clock.values().sum()
    }
}

impl From<DependencyClock> for HashMap<String, usize> {
    fn from(val: DependencyClock) -> Self {
        let mut id_counter = HashMap::new();
        for (idx, m) in val.view.members.iter().enumerate() {
            id_counter.insert(m.clone(), *val.clock.get(&(idx)).unwrap_or(&0));
        }
        id_counter
    }
}

impl Display for DependencyClock {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{{ ")?;
        for (idx, m) in self.view.members.iter().enumerate() {
            write!(f, "{}: {}", m, self.get(m).unwrap())?;
            if idx < self.view.members.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, " }}")?;
        if let Some(origin) = self.origin {
            write!(f, "@{}", self.view.members[origin])?;
        }
        Ok(())
    }
}

impl PartialOrd for DependencyClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.view.id.cmp(&other.view.id) {
            Ordering::Less => return Some(Ordering::Less),
            Ordering::Greater => return Some(Ordering::Greater),
            _ => {}
        };

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
            let self_val = self.get(m).unwrap_or(0);
            let other_val = other.get(m).unwrap_or(0);

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
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "b");
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
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "b");
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
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "a");
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
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "a");
        v1.increment();
        v2.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
    }

    #[test_log::test]
    fn test_clock() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::clone(&view.data);
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "b");

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
        let v1 = DependencyClock::new(&rc, "a");
        assert_eq!(format!("{}", v1), "{ a: 0, b: 0, c: 0 }@a");
    }
}
