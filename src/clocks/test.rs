use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

// ===== Typestate Pattern ===== //

/// Marker trait for vector clock states
pub trait ClockState {}

/// Full vector clock (supports partial order comparisons)
pub struct Full;

/// Partial vector clock (dependency clock, no ordering)
pub struct Partial;

impl ClockState for Full {}
impl ClockState for Partial {}

// ===== VectorClock Definition ===== //

/// Dummy view data placeholder
#[derive(Debug)]
pub struct ViewData {
    // Fill with your membership info, etc.
}

/// A vector clock with typestate S
pub struct VectorClock<S: ClockState> {
    pub(crate) view: Rc<ViewData>,
    pub(crate) clock: HashMap<usize, usize>,
    pub(crate) origin: Option<usize>,
    _state: std::marker::PhantomData<S>,
}

/// Type alias for full vector clock
pub type FullClock = VectorClock<Full>;

/// Type alias for dependency (partial) vector clock
pub type DependencyClock = VectorClock<Partial>;

// ===== Constructors ===== //

impl<S: ClockState> VectorClock<S> {
    pub fn new(view: Rc<ViewData>, origin: Option<usize>) -> Self {
        Self {
            view,
            clock: HashMap::new(),
            origin,
            _state: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, member: usize, counter: usize) {
        self.clock.insert(member, counter);
    }
}

// ===== PartialEq and PartialOrd for FullClock ===== //

impl PartialEq for VectorClock<Full> {
    fn eq(&self, other: &Self) -> bool {
        self.clock == other.clock
    }
}

impl PartialOrd for VectorClock<Full> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut less = false;
        let mut greater = false;

        for (&member, &version) in &self.clock {
            let other_version = other.clock.get(&member).copied().unwrap_or(0);
            if version < other_version {
                less = true;
            } else if version > other_version {
                greater = true;
            }
        }

        for (&member, &other_version) in &other.clock {
            if !self.clock.contains_key(&member) {
                if other_version > 0 {
                    less = true;
                }
            }
        }

        match (less, greater) {
            (true, true) => None,
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => Some(Ordering::Equal),
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn tessst() {
        let view = Rc::new(ViewData {});

        let mut a = FullClock::new(view.clone(), Some(0));
        let mut b = FullClock::new(view.clone(), Some(1));

        a.insert(0, 1);
        a.insert(1, 0);

        b.insert(0, 1);
        b.insert(1, 2);

        if a < b {
            println!("a happened before b");
        }

        let mut dep = DependencyClock::new(view.clone(), None);
        let _ = dep < dep; // ❌ Compile error: no implementation of PartialOrd for VectorClock<Partial>
    }
}
