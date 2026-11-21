use std::fmt::Debug;

use crate::crdt::replicated_data_type::Sequential;

/// Sequential storage for CRDTs that retain their stable operations.
impl<O: Clone + Debug> Sequential for Vec<O> {
    type Update = O;

    fn apply(&mut self, update: &O) {
        self.push(update.clone());
    }

    fn is_default(&self) -> bool {
        self.is_empty()
    }
}
