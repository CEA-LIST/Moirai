use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

/// Read-only shared handle
#[derive(Debug, PartialEq, Eq)]
pub struct Reader<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> Clone for Reader<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> Reader<T> {
    pub fn borrow(&self) -> Ref<'_, T> {
        self.inner.borrow()
    }
}

/// Unique owner with mutable access
#[derive(Debug, Clone)]
pub struct MutOwner<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> MutOwner<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: Rc::new(RefCell::new(value)),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<'_, T> {
        self.inner.borrow_mut()
    }

    /// Downgrade into a read-only handle
    pub fn as_reader(&self) -> Reader<T> {
        Reader {
            inner: self.inner.clone(),
        }
    }
}
