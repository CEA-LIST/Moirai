pub trait Boxer<Boxed> {
    fn boxer(self) -> Boxed;
}

impl<T> Boxer<T> for T {
    fn boxer(self) -> T {
        self
    }
}
