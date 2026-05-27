use std::{
    cell::OnceCell,
    fmt,
    fmt::{Debug, Result},
};

#[derive(Default)]
pub struct CacheCell<V> {
    value: OnceCell<V>,
}

impl<V> CacheCell<V> {
    pub fn new() -> Self {
        Self {
            value: OnceCell::new(),
        }
    }

    pub fn get(&self) -> Option<&V> {
        self.value.get()
    }

    pub fn get_mut(&mut self) -> Option<&mut V> {
        self.value.get_mut()
    }

    pub fn get_or_compute(&self, f: impl FnOnce() -> V) -> &V {
        self.value.get_or_init(f)
    }

    pub fn invalidate(&mut self) {
        self.value.take();
    }

    pub fn replace(&mut self, value: V) {
        if let Some(slot) = self.value.get_mut() {
            *slot = value;
        } else {
            let _ = self.value.set(value);
        }
    }
}

impl<V> Debug for CacheCell<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result {
        f.debug_struct("CacheCell").finish_non_exhaustive()
    }
}

impl<V> Clone for CacheCell<V> {
    fn clone(&self) -> Self {
        // Cache contents are derived from the log and can be recomputed. Cloning a log starts
        // with an empty cache to avoid adding a `V: Clone` bound to log clones.
        Self::new()
    }
}
