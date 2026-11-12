use std::convert::Infallible;

use crate::data_type::sequential::SequentialDataType;

/// SequentialDataType storage for CRDTs that retain their stable operations.
impl<O> SequentialDataType for Vec<O>
where
    O: Clone,
{
    type Update = O;
    type Rejection = Infallible;

    fn apply(&mut self, update: &O) {
        self.push(update.clone());
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn is_default(&self) -> bool {
        self.is_empty()
    }
}
