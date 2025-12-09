pub trait Unboxer<Unboxed> {
    fn unbox(self) -> Unboxed;
}

impl<T> Unboxer<T> for T {
    fn unbox(self) -> T {
        self
    }
}
