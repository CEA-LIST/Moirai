use std::{
    cmp::{min, Ordering},
    collections::HashMap,
    fmt::{Display, Error, Formatter},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::protocol::membership::View;

use super::clock::Clock;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DependencyClock {
    pub(crate) view: Rc<View>,
    /// The key is the index of the member in the members list
    /// The value is the version of the last event by this member, known by this version vector
    pub(crate) clock: HashMap<usize, usize>,
    /// The index of the origin member in the members list/clock
    pub(crate) origin: usize,
}

impl Clock for DependencyClock {
    fn new(view: &Rc<View>, origin: &str) -> Self {
        Self {
            origin: view.members.iter().position(|m| m == origin).unwrap(),
            view: Rc::clone(view),
            clock: HashMap::new(),
        }
    }

    fn merge(&mut self, other: &Self) {
        assert!(self.view.id == other.view.id);
        for (idx, m) in self.view.members.iter().enumerate() {
            if self.get(m) < other.get(m) {
                self.clock.insert(idx, other.get(m));
            }
        }
    }

    fn increment(&mut self) {
        let idx = self.get(&self.origin());
        self.clock.insert(self.origin, idx + 1);
    }

    fn min(&self, other: &Self) -> Self {
        assert!(self.view.id == other.view.id);
        let mut new_clock = HashMap::new();
        for (idx, m) in self.view.members.iter().enumerate() {
            new_clock.insert(idx, min(self.get(m), other.get(m)));
        }
        Self {
            view: self.view.clone(),
            clock: new_clock,
            origin: self.origin,
        }
    }

    fn remove(&mut self, member: &str) {
        let idx = self.view.members.iter().position(|m| m == member).unwrap();
        self.clock.remove(&(idx));
    }

    fn dim(&self) -> usize {
        self.clock.len()
    }

    fn get(&self, member: &str) -> usize {
        let idx = self.view.members.iter().position(|m| m == member).unwrap();
        *self.clock.get(&(idx)).unwrap_or(&0)
    }

    fn bot(members: &Rc<View>) -> Self {
        Self {
            view: Rc::clone(members),
            clock: HashMap::new(),
            origin: 0,
        }
    }

    fn origin(&self) -> &str {
        &self.view.members[self.origin as usize]
    }
}

impl Into<HashMap<String, usize>> for DependencyClock {
    fn into(self) -> HashMap<String, usize> {
        let mut id_counter = HashMap::new();
        for (idx, m) in self.view.members.iter().enumerate() {
            id_counter.insert(m.clone(), *self.clock.get(&(idx)).unwrap_or(&0));
        }
        id_counter
    }
}

impl Display for DependencyClock {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:?}", self.clock)
    }
}

impl PartialOrd for DependencyClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.view.id.cmp(&other.view.id) {
            Ordering::Less => return Some(Ordering::Less),
            Ordering::Greater => return Some(Ordering::Greater),
            _ => {}
        };

        if self.get(&self.origin()) == other.get(&other.origin()) && self.origin == other.origin {
            return Some(Ordering::Equal);
        }

        match self.get(&self.origin()).cmp(&other.get(&self.origin())) {
            Ordering::Less => return Some(Ordering::Less),
            _ => {}
        }

        match &other.get(&self.origin()).cmp(&self.get(&self.origin())) {
            Ordering::Less => return Some(Ordering::Greater),
            _ => {}
        }

        let mut less = false;
        let mut greater = false;

        for m in self.view.members.iter() {
            let self_val = self.get(m);
            let other_val = other.get(m);

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

impl Ord for DependencyClock {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(&other) {
            Some(ord) => ord,
            None => self.origin.cmp(&other.origin),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::membership::ViewStatus;

    use super::*;

    #[test_log::test]
    fn test_version_vector() {
        let view = View::new(
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ViewStatus::Installed,
        );
        let rc = Rc::new(view);
        let mut v1 = DependencyClock::new(&rc, "a");
        let mut v2 = DependencyClock::new(&rc, "b");

        v1.increment();
        v1.increment();
        v2.increment();
        v2.increment();
        v2.increment();

        assert_eq!(v1.get("a"), 2);
        assert_eq!(v1.get("b"), 0);
        assert_eq!(v1.get("c"), 0);

        assert_eq!(v2.get("a"), 0);
        assert_eq!(v2.get("b"), 3);
        assert_eq!(v2.get("c"), 0);

        v1.merge(&v2);
        assert_eq!(v1.get("a"), 2);
        assert_eq!(v1.get("b"), 3);
        assert_eq!(v1.get("c"), 0);

        v2.merge(&v1);
        assert_eq!(v2.get("a"), 2);
        assert_eq!(v2.get("b"), 3);
        assert_eq!(v2.get("c"), 0);
    }
}
