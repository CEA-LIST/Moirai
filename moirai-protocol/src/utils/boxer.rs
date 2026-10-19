pub trait Boxer<Boxed> {
    fn boxer(self) -> Boxed;

    fn with_boxed<R>(&self, f: impl FnOnce(&Boxed) -> R) -> R
    where
        Self: Clone,
    {
        f(&self.clone().boxer())
    }
}

impl<T> Boxer<T> for T {
    fn boxer(self) -> T {
        self
    }

    fn with_boxed<R>(&self, f: impl FnOnce(&T) -> R) -> R
    where
        Self: Clone,
    {
        f(self)
    }
}

impl<T> Boxer<T> for Box<T> {
    fn boxer(self) -> T {
        *self
    }

    fn with_boxed<R>(&self, f: impl FnOnce(&T) -> R) -> R
    where
        Self: Clone,
    {
        f(self)
    }
}

impl<T> Boxer<Box<T>> for T {
    fn boxer(self) -> Box<T> {
        Box::new(self)
    }
}
