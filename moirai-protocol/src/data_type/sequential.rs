use crate::data_type::query::QueryOperation;

/// SequentialDataType behavior implemented directly by the stored state.
pub trait SequentialDataType: Default {
    type Update: Clone;
    type Rejection;

    fn apply(&mut self, update: &Self::Update);

    fn is_enabled(&self, update: &Self::Update) -> Result<(), Self::Rejection>;

    /// Whether the state contains no retained contribution.
    fn is_default(&self) -> bool;
}

pub trait SequentialEval<Q>
where
    Self: SequentialDataType,
    Q: QueryOperation,
{
    fn eval(state: &Self, query: &Q) -> Q::Response;
}
